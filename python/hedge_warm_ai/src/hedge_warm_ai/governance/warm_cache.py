"""Interim WarmCache adaptors for governance weight + shadow flag.

The AI_Governance_Engine writes two pieces of state into the
Hot_Path-readable WarmCache surface (R24.2, R24.3):

* The per-component **governance weight** at
  ``hedge.warm.governance.<component>``. The Risk_Engine and the
  AI_Trade_Ranking_Engine read this multiplier through the WarmCache
  last-known-value path; the dedicated ``hedge-warmcache`` crate
  (task 44.x) will adopt the namespace verbatim.
* The per-component **shadow flag** at
  ``hedge.warm.shadow.<component>``. The AI_Shadow_Mode service
  (task 29.1) consumes this flag to decide whether the component's
  outputs may influence the displayed ranking.

This module is the seam between the engine and whichever backend is
live:

* :class:`GovernanceWarmCache` — the protocol every backend
  implements.
* :class:`InMemoryGovernanceWarmCache` — captures writes in memory
  for assertion in tests. Mirrors the
  :class:`hedge_warm_ai.regime.warm_cache.InMemoryMarketStabilityCache`
  pattern.
* :class:`RedisGovernanceWarmCache` — wraps an
  ``async`` Redis client and writes through the canonical key
  scheme.

The Redis client is intentionally a parameter rather than fetched
from a global singleton: tests pass an in-memory fake; the
``hedge-governance`` service-layer entry point passes the same
``aioredis.Redis`` instance the rest of the Warm_AI_Pipeline uses
(constructed via :func:`hedge_memory_rag.redis_cache.config.load_redis_cache_config`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, Optional, Protocol

import structlog

from .errors import GovernanceCacheError
from .ladder import GovernanceLevel
from .state import GovernedComponent
from .subjects import (
    DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE,
    DEFAULT_SHADOW_FLAG_NAMESPACE,
    governance_weight_key,
    shadow_flag_key,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from redis import asyncio as aioredis

_LOG: Final = structlog.get_logger(__name__)

#: Default TTL (seconds) for governance-weight entries. Generous
#: enough to cover a stalled engine's restart; bounded so a
#: persistent failure does not leak a stale weight forever.
DEFAULT_WEIGHT_TTL_S: Final[int] = 600

#: Default TTL (seconds) for shadow-flag entries. Same rationale.
DEFAULT_SHADOW_TTL_S: Final[int] = 600


# ---------------------------------------------------------------------------
# Wire payload ------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GovernanceWeightPayload:
    """Wire payload written to ``hedge.warm.governance.<component>``.

    The Risk_Engine and AI_Trade_Ranking_Engine read this through the
    WarmCache adaptor and apply ``weight`` as a multiplier on the
    component's contribution to the formulas. ``level`` is carried
    so a consumer can branch on the discrete level (e.g. emit a
    "shadowed" UI badge) without re-deriving it from the multiplier.
    """

    component: GovernedComponent
    weight: float
    level: GovernanceLevel
    ts_ns: int


# ---------------------------------------------------------------------------
# Protocol -----------------------------------------------------------------
# ---------------------------------------------------------------------------


class GovernanceWarmCache(Protocol):
    """Sink for governance weight + shadow flag (R24.2, R24.3).

    Implementations MUST:

    * Be async-safe — multiple coroutines may race writes.
    * Persist a *cache-invalidation-on-write* semantic: a subsequent
      read returns the most recent write.
    * Translate wire-level failures to :class:`GovernanceCacheError`
      so the engine can surface the degraded state to the supervisor.
    """

    async def set_weight(self, payload: GovernanceWeightPayload) -> None: ...
    async def get_weight(
        self, component: GovernedComponent
    ) -> Optional[GovernanceWeightPayload]: ...

    async def set_shadow(self, component: GovernedComponent, *, ts_ns: int) -> None: ...
    async def clear_shadow(self, component: GovernedComponent) -> None: ...
    async def is_shadowed(self, component: GovernedComponent) -> bool: ...


# ---------------------------------------------------------------------------
# In-memory adaptor ---------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class InMemoryGovernanceWarmCache:
    """In-memory cache for tests.

    Captures every write so assertions can confirm the engine writes
    the right values at the right edge.
    """

    _lock: RLock = field(default_factory=RLock, init=False)
    _weights: dict[GovernedComponent, GovernanceWeightPayload] = field(
        default_factory=dict, init=False
    )
    _shadow: dict[GovernedComponent, int] = field(default_factory=dict, init=False)
    _weight_writes: list[GovernanceWeightPayload] = field(
        default_factory=list, init=False
    )
    _shadow_writes: list[tuple[GovernedComponent, str, int]] = field(
        default_factory=list, init=False
    )

    async def set_weight(self, payload: GovernanceWeightPayload) -> None:
        with self._lock:
            self._weights[payload.component] = payload
            self._weight_writes.append(payload)

    async def get_weight(
        self, component: GovernedComponent
    ) -> Optional[GovernanceWeightPayload]:
        with self._lock:
            return self._weights.get(component)

    async def set_shadow(self, component: GovernedComponent, *, ts_ns: int) -> None:
        with self._lock:
            self._shadow[component] = int(ts_ns)
            self._shadow_writes.append((component, "set", int(ts_ns)))

    async def clear_shadow(self, component: GovernedComponent) -> None:
        with self._lock:
            self._shadow.pop(component, None)
            self._shadow_writes.append((component, "clear", 0))

    async def is_shadowed(self, component: GovernedComponent) -> bool:
        with self._lock:
            return component in self._shadow

    @property
    def weight_writes(self) -> list[GovernanceWeightPayload]:
        with self._lock:
            return list(self._weight_writes)

    @property
    def shadow_writes(self) -> list[tuple[GovernedComponent, str, int]]:
        with self._lock:
            return list(self._shadow_writes)

    def reset(self) -> None:
        with self._lock:
            self._weights.clear()
            self._shadow.clear()
            self._weight_writes.clear()
            self._shadow_writes.clear()


# ---------------------------------------------------------------------------
# Redis adaptor (interim until the WarmCache crate / task 44.x lands) ------
# ---------------------------------------------------------------------------


def _encode_weight_payload(payload: GovernanceWeightPayload) -> bytes:
    return json.dumps(
        {
            "component": payload.component.value,
            "weight": float(payload.weight),
            "level": payload.level.value,
            "ts_ns": int(payload.ts_ns),
        },
        separators=(",", ":"),
    ).encode("utf-8")


def _decode_weight_payload(raw: Any) -> Optional[GovernanceWeightPayload]:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        try:
            obj = json.loads(bytes(raw).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
    elif isinstance(raw, str):
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            return None
    elif isinstance(raw, dict):
        obj = raw
    else:
        return None
    try:
        return GovernanceWeightPayload(
            component=GovernedComponent(obj["component"]),
            weight=float(obj["weight"]),
            level=GovernanceLevel(obj["level"]),
            ts_ns=int(obj["ts_ns"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


@dataclass
class RedisGovernanceWarmCache:
    """``GovernanceWarmCache`` backed by ``redis.asyncio``.

    Stores two key families under disjoint namespaces (see
    :data:`DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE` and
    :data:`DEFAULT_SHADOW_FLAG_NAMESPACE`):

    * ``hedge.warm.governance.<component>`` — JSON-encoded
      :class:`GovernanceWeightPayload` (weight + level + ts_ns).
    * ``hedge.warm.shadow.<component>`` — bare ``ts_ns`` integer.
      Presence of the key means the component is shadowed; absence
      means it is influencing.

    Wire-level Redis failures raise :class:`GovernanceCacheError`.
    """

    client: "aioredis.Redis"
    weight_namespace: str = DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE
    shadow_namespace: str = DEFAULT_SHADOW_FLAG_NAMESPACE
    weight_ttl_s: int = DEFAULT_WEIGHT_TTL_S
    shadow_ttl_s: int = DEFAULT_SHADOW_TTL_S

    def __post_init__(self) -> None:
        if self.weight_ttl_s <= 0:
            raise ValueError(
                f"weight_ttl_s must be > 0; got {self.weight_ttl_s!r}"
            )
        if self.shadow_ttl_s <= 0:
            raise ValueError(
                f"shadow_ttl_s must be > 0; got {self.shadow_ttl_s!r}"
            )

    async def set_weight(self, payload: GovernanceWeightPayload) -> None:
        key = governance_weight_key(
            payload.component.value, namespace=self.weight_namespace
        )
        body = _encode_weight_payload(payload)
        try:
            await self.client.set(key, body, ex=self.weight_ttl_s)
        except Exception as exc:
            _LOG.warning(
                "governance_weight_cache_set_failed",
                key=key,
                component=payload.component.value,
                weight=payload.weight,
                level=payload.level.value,
                error=str(exc),
            )
            raise GovernanceCacheError(
                f"failed to write governance weight on {key!r}: {exc}"
            ) from exc

    async def get_weight(
        self, component: GovernedComponent
    ) -> Optional[GovernanceWeightPayload]:
        key = governance_weight_key(
            component.value, namespace=self.weight_namespace
        )
        try:
            raw = await self.client.get(key)
        except Exception as exc:
            raise GovernanceCacheError(
                f"failed to read governance weight on {key!r}: {exc}"
            ) from exc
        return _decode_weight_payload(raw)

    async def set_shadow(self, component: GovernedComponent, *, ts_ns: int) -> None:
        key = shadow_flag_key(component.value, namespace=self.shadow_namespace)
        body = str(int(ts_ns)).encode("ascii")
        try:
            await self.client.set(key, body, ex=self.shadow_ttl_s)
        except Exception as exc:
            _LOG.warning(
                "governance_shadow_cache_set_failed",
                key=key,
                component=component.value,
                ts_ns=ts_ns,
                error=str(exc),
            )
            raise GovernanceCacheError(
                f"failed to write shadow flag on {key!r}: {exc}"
            ) from exc

    async def clear_shadow(self, component: GovernedComponent) -> None:
        key = shadow_flag_key(component.value, namespace=self.shadow_namespace)
        try:
            await self.client.delete(key)
        except Exception as exc:
            _LOG.warning(
                "governance_shadow_cache_clear_failed",
                key=key,
                component=component.value,
                error=str(exc),
            )
            raise GovernanceCacheError(
                f"failed to clear shadow flag on {key!r}: {exc}"
            ) from exc

    async def is_shadowed(self, component: GovernedComponent) -> bool:
        key = shadow_flag_key(component.value, namespace=self.shadow_namespace)
        try:
            raw = await self.client.get(key)
        except Exception as exc:
            raise GovernanceCacheError(
                f"failed to read shadow flag on {key!r}: {exc}"
            ) from exc
        return raw is not None


__all__ = [
    "DEFAULT_SHADOW_TTL_S",
    "DEFAULT_WEIGHT_TTL_S",
    "GovernanceWarmCache",
    "GovernanceWeightPayload",
    "InMemoryGovernanceWarmCache",
    "RedisGovernanceWarmCache",
]
