"""``MarketStability`` cache adaptors used by the Market_Regime_Engine.

The Risk_Engine consumes the ``MarketStability`` factor through the
WarmCache last-known-value path (R5.13, design § Latency Budget
Allocation, design § Components § Risk_Engine). The WarmCache itself is
a Rust crate that lands as task 44.x; until that ships, the
Market_Regime_Engine writes the factor to a Redis cache key inside the
existing :mod:`hedge_memory_rag.redis_cache` namespace using
:class:`~hedge_memory_rag.redis_cache.RedisHotCache`.

This module is the seam between the engine and whichever backend is
live:

* :class:`MarketStabilityCache` — the protocol every backend implements.
* :class:`RedisMarketStabilityCache` — wraps the existing
  :class:`RedisHotCache` and writes through its
  ``set_market_stability`` / ``set_regime`` accessors. Heavy imports
  (:mod:`hedge_memory_rag`, :mod:`redis.asyncio`) are deferred to
  construction so the regime subpackage can be imported in environments
  that have not installed the RAG package yet.
* :class:`InMemoryMarketStabilityCache` — captures writes in memory for
  assertion in tests. Mirrors the
  :class:`hedge_warm_ai.regime.publisher.InMemoryRegimePublisher`
  pattern.

When the Rust WarmCache (task 44.x) lands, a thin
``WarmCacheMarketStability`` wrapper will sit alongside the Redis
adaptor and the engine will be re-pointed at it without any change to
the public API surface — both adaptors implement the same
:class:`MarketStabilityCache` protocol.

The ``MarketStability`` value object that flows through this module is
:class:`hedge_warm_ai.regime.signals.MarketStabilityFactor`. Backends
serialise it via :class:`pydantic.BaseModel`-compatible JSON so the
on-disk shape is stable across the Rust/Python boundary.
"""

from __future__ import annotations

import time
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, Optional, Protocol

import structlog

from ..schemas.ai_regime_changed import Regime
from .errors import MarketStabilityCacheError
from .signals import MarketStabilityFactor

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_memory_rag.redis_cache import RedisHotCache

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class MarketStabilityCache(Protocol):
    """Sink for the ``MarketStability`` factor.

    Implementations MUST:

    * Be async-safe — multiple coroutines may race writes.
    * Persist a *cache-invalidation-on-write* semantic: a subsequent
      ``get`` returns the most recent ``set`` (within the staleness
      window the backend chooses).
    * Translate wire-level failures to
      :class:`MarketStabilityCacheError` so the engine can surface the
      degraded state to the supervisor.
    """

    async def set_factor(self, factor: MarketStabilityFactor) -> None: ...
    async def get_factor(self) -> Optional[MarketStabilityFactor]: ...
    async def set_current_regime(self, regime: Regime, *, ts_ns: int) -> None: ...


# ---------------------------------------------------------------------------
# In-memory adaptor ---------------------------------------------------------
# ---------------------------------------------------------------------------


class InMemoryMarketStabilityCache:
    """In-memory cache for tests.

    Captures every ``set_factor`` and ``set_current_regime`` call so
    assertions can confirm the engine writes the right values at the
    right edge.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._factor: Optional[MarketStabilityFactor] = None
        self._regime_writes: list[tuple[Regime, int]] = []
        self._factor_writes: list[MarketStabilityFactor] = []

    async def set_factor(self, factor: MarketStabilityFactor) -> None:
        with self._lock:
            self._factor = factor
            self._factor_writes.append(factor)

    async def get_factor(self) -> Optional[MarketStabilityFactor]:
        with self._lock:
            return self._factor

    async def set_current_regime(self, regime: Regime, *, ts_ns: int) -> None:
        with self._lock:
            self._regime_writes.append((regime, ts_ns))

    @property
    def factor_writes(self) -> list[MarketStabilityFactor]:
        with self._lock:
            return list(self._factor_writes)

    @property
    def regime_writes(self) -> list[tuple[Regime, int]]:
        with self._lock:
            return list(self._regime_writes)

    def reset(self) -> None:
        with self._lock:
            self._factor = None
            self._regime_writes.clear()
            self._factor_writes.clear()


# ---------------------------------------------------------------------------
# Redis adaptor (interim until the WarmCache crate / task 44.x lands) ------
# ---------------------------------------------------------------------------


class RedisMarketStabilityCache:
    """``MarketStability`` cache backed by :class:`RedisHotCache`.

    Stores two keys under the existing ``hedge:rag:cache`` namespace
    (defined in :mod:`hedge_memory_rag.redis_cache.config`):

    * ``regime:current``                  ← current :class:`Regime` label
      (written through :meth:`RedisHotCache.set_regime`).
    * ``regime:market_stability:current`` ← :class:`MarketStabilityFactor`
      payload (written through :meth:`RedisHotCache.set_market_stability`).

    Both keys carry the TTLs configured on
    :class:`hedge_memory_rag.redis_cache.RedisCacheConfig` so a stalled
    Market_Regime_Engine cannot leak stale values into the Risk_Engine
    forever.

    Wire-level Redis failures are captured by :class:`RedisHotCache` as
    :class:`hedge_memory_rag.redis_cache.RedisCacheError` subclasses;
    this adaptor re-raises them as
    :class:`MarketStabilityCacheError` to keep the engine's failure
    surface narrow.

    Note (task 44.x replacement plan):
        When the Rust ``hedge-warmcache`` crate lands, the engine will
        construct a new ``WarmCacheMarketStability`` adaptor (writing
        directly to the WarmCache atomic slot) and pass it where
        :class:`RedisMarketStabilityCache` is passed today. Both
        adaptors implement :class:`MarketStabilityCache`, so no engine
        code changes — only the constructor call.
    """

    def __init__(self, hot_cache: "RedisHotCache") -> None:
        self._hot = hot_cache

    async def set_factor(self, factor: MarketStabilityFactor) -> None:
        # Encode as a compact JSON-friendly mapping so a Rust consumer
        # (and replay rigs) can decode without a Python runtime.
        payload: dict[str, Any] = {
            "value": float(factor.value),
            "derived_from": factor.derived_from,
            "ts_ns": int(factor.ts_ns),
        }
        try:
            await self._hot.set_market_stability(payload)
        except Exception as exc:
            _LOG.warning(
                "market_stability_cache_set_failed",
                value=factor.value,
                derived_from=factor.derived_from,
                error=str(exc),
            )
            raise MarketStabilityCacheError(
                f"failed to write MarketStability factor to RedisHotCache: {exc}"
            ) from exc

    async def get_factor(self) -> Optional[MarketStabilityFactor]:
        try:
            raw = await self._hot.get_market_stability()
        except Exception as exc:
            raise MarketStabilityCacheError(
                f"failed to read MarketStability factor from RedisHotCache: {exc}"
            ) from exc
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise MarketStabilityCacheError(
                "MarketStability cache entry is not a JSON object: "
                f"got {type(raw).__name__}"
            )
        try:
            return MarketStabilityFactor(
                value=float(raw["value"]),
                derived_from=raw["derived_from"],
                ts_ns=int(raw["ts_ns"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MarketStabilityCacheError(
                f"MarketStability cache entry malformed: {exc}; raw={raw!r}"
            ) from exc

    async def set_current_regime(self, regime: Regime, *, ts_ns: int) -> None:
        # We persist a small mapping rather than the bare label so the
        # cache entry carries a wall-clock timestamp consumers can use
        # for staleness checks.
        payload = {"regime": regime, "ts_ns": int(ts_ns)}
        try:
            await self._hot.set_regime(payload)
        except Exception as exc:
            _LOG.warning(
                "regime_current_cache_set_failed",
                regime=regime,
                error=str(exc),
            )
            raise MarketStabilityCacheError(
                f"failed to write current regime to RedisHotCache: {exc}"
            ) from exc


# ---------------------------------------------------------------------------
# Convenience: derive the stability factor for a regime --------------------
# ---------------------------------------------------------------------------


def derive_stability_factor(
    regime: Regime,
    *,
    stability_factor_value: float,
    ts_ns: Optional[int] = None,
) -> MarketStabilityFactor:
    """Build a :class:`MarketStabilityFactor` from a regime + scalar.

    Convenience helper used by the engine and exported here so callers
    that bypass the engine (replay rigs, integration tests) can stay
    in lockstep with the canonical clamp + timestamp behaviour.
    """
    if ts_ns is None:
        ts_ns = time.time_ns()
    # The dataclass re-validates value bounds; we let any
    # :class:`RegimeClassificationError` propagate.
    return MarketStabilityFactor(
        value=float(stability_factor_value),
        derived_from=regime,
        ts_ns=int(ts_ns),
    )


__all__ = [
    "InMemoryMarketStabilityCache",
    "MarketStabilityCache",
    "RedisMarketStabilityCache",
    "derive_stability_factor",
]
