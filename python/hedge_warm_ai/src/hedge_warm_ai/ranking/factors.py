"""Factor providers for the AI_Trade_Ranking_Engine.

The :class:`hedge_warm_ai.ranking.AiTradeRankingEngine` is intentionally
stateless about *where* its five factor inputs come from. It depends on
a :class:`FactorProvider` protocol whose single
:meth:`FactorProvider.factors_for` async method returns a fully-populated
:class:`hedge_warm_ai.ranking.score.RankingFactors` for one
:class:`SignalEvent`.

Factor sources (all already maintained by sibling Warm_AI_Pipeline
engines — task 26.1 does NOT introduce new producers):

* ``orderflow`` and ``technical_strength``: produced by the Hot_Path
  Feature_Extraction_Engine on every ``feat.update.<sym>`` and bridged
  into the Warm_AI_Pipeline state via
  :class:`hedge_memory_rag.redis_cache.RedisHotCache`. The bridge is
  the existing Hot_Path-to-Warm_AI seam (Redis-backed today, replaced
  by the Rust ``hedge-warmcache`` crate in task 44.x). The engine
  reads the per-symbol Redis key ``hedge:rag:cache:factors:orderflow:<sym>``
  for the orderflow component and ``hedge:rag:cache:factors:technical_strength:<sym>``
  for the technical-strength component.
* ``news_sentiment``: produced by the News_Intelligence_Engine
  (task 21.1) on every ``ai.news.impact.<sym>`` emission and cached
  per-symbol via :meth:`RedisHotCache.cache_news`. The provider reads
  the most recent entry's ``impact_magnitude`` (a ``[0.0, 1.0]``
  scalar) — see :class:`hedge_warm_ai.schemas.NewsImpact`.
* ``market_regime``: produced by the Market_Regime_Engine (task 22.1)
  and surfaced via :meth:`RedisHotCache.get_market_stability` — the
  ``MarketStability`` factor (R5.13, R13.5).
* ``trader_discipline``: produced by the Trader_Psychology_Engine
  (task 25.1) on every ``ai.psych.stability`` emission and cached
  via :meth:`RedisHotCache.set_stability_score`. The provider reads
  the most recent payload's ``components.discipline`` field.

Three concrete providers are shipped:

* :class:`StubFactorProvider` — returns a fixed
  :class:`RankingFactors`. Used by unit tests and the white-box
  ``hedge-rank --check`` smoke command.
* :class:`InMemoryFactorProvider` — accepts per-symbol updates from a
  test harness via :meth:`InMemoryFactorProvider.set_factor`. Used by
  the property tests in task 26.2.
* :class:`RankingFactorProvider` — production adaptor that pulls each
  factor from the existing :class:`RedisHotCache`. The adaptor is the
  *only* place that knows the Redis key scheme. When the WarmCache
  Rust crate (task 44.x) lands, a sibling
  ``WarmCacheFactorProvider`` will replace this class without
  changing the engine's call site — both implement
  :class:`FactorProvider`.

Factor staleness:
    Stale factors are *not* fatal. The ranking engine's ranking
    decision must complete within the 5 ms p95 budget (R17.5); if a
    factor lookup blows past
    :attr:`hedge_warm_ai.ranking.config.RankingConfig.factor_staleness_window_s`,
    the provider returns the configured default (typically 0.5) and
    logs the staleness via structlog. The downstream Risk_Engine has
    its own fallback path (``Signal_v1.confidence``) so a degraded
    rank cannot wedge sizing.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, Mapping, Optional, Protocol

import structlog

from .errors import RankingFactorError
from .score import RankingFactors
from .state import SignalEvent

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_memory_rag.redis_cache import RedisHotCache

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Protocol ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class FactorProvider(Protocol):
    """Source of :class:`RankingFactors` for a given :class:`SignalEvent`.

    Implementations MUST:

    * Be async-safe — multiple coroutines may resolve factors
      concurrently while the Warm_AI_Pipeline drives a burst of
      ``sig.emitted`` events through the engine.
    * Treat missing factors as recoverable: return the configured
      default for any factor whose source is stale or unreachable.
      Never raise out of :meth:`factors_for` — the engine's per-call
      latency budget (R17.5) does not allow for an exception-and-
      retry path.
    * Surface anomalies via structlog so the Self_Healing_Supervisor
      sees a structured event for each degraded resolution.
    """

    async def factors_for(self, event: SignalEvent) -> RankingFactors: ...


# ---------------------------------------------------------------------------
# Stubs and in-memory providers (tests + smoke commands) -------------------
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class StubFactorProvider:
    """Returns the same :class:`RankingFactors` for every signal.

    Convenient for tests that only care about the formula's output
    given a known input.
    """

    factors: RankingFactors = field(default_factory=RankingFactors)

    async def factors_for(self, event: SignalEvent) -> RankingFactors:  # noqa: D401
        return self.factors


class InMemoryFactorProvider:
    """Per-symbol :class:`RankingFactors` registry held in memory.

    The harness pushes per-symbol updates via :meth:`set_factor` (or
    :meth:`set_default` for the catch-all). Lookups in
    :meth:`factors_for` return the most recently set value for the
    signal's symbol, falling back to the default. Thread-safe: the
    underlying dict is guarded by an :class:`RLock`.
    """

    def __init__(
        self,
        *,
        default: Optional[RankingFactors] = None,
    ) -> None:
        self._lock = RLock()
        self._per_symbol: dict[str, RankingFactors] = {}
        self._default: RankingFactors = default or RankingFactors()

    def set_factor(self, symbol: str, factors: RankingFactors) -> None:
        """Register the latest factors for *symbol* (test helper)."""
        if not isinstance(symbol, str):
            raise TypeError(
                f"symbol must be str, got {type(symbol).__name__}"
            )
        with self._lock:
            self._per_symbol[symbol] = factors

    def set_default(self, factors: RankingFactors) -> None:
        """Replace the catch-all factors used when no symbol is matched."""
        with self._lock:
            self._default = factors

    async def factors_for(self, event: SignalEvent) -> RankingFactors:
        with self._lock:
            return self._per_symbol.get(event.symbol, self._default)


# ---------------------------------------------------------------------------
# Production: Redis-backed factor provider ---------------------------------
# ---------------------------------------------------------------------------

# Per-symbol Redis key suffixes, written by the Hot_Path-to-Warm_AI
# bridge for orderflow / technical-strength signals. Mirrors the
# WarmCache crate's key plan from task 44.x. The full key is
# ``<namespace>:<suffix>:<symbol>`` where ``<namespace>`` is the
# RedisHotCache config's ``namespace`` (default ``hedge:rag:cache``).
_KEY_FACTORS_ORDERFLOW: Final[str] = "factors:orderflow"
_KEY_FACTORS_TECHNICAL_STRENGTH: Final[str] = "factors:technical_strength"


def _coerce_unit_factor(
    raw: Any,
    *,
    factor_name: str,
    default: float,
) -> float:
    """Coerce *raw* into a ``[0.0, 1.0]`` scalar.

    Accepts ``int`` / ``float`` (cast directly), ``str`` (parsed as a
    JSON number), and ``Mapping`` carrying a ``value`` field (the
    Redis-cached :class:`MarketStabilityFactor` / wrapper shape).
    Returns *default* on any malformation, logging the anomaly via
    structlog. Out-of-range values are clamped, never raised — the
    engine cannot afford an exception path on the score loop.
    """
    if raw is None:
        return default

    candidate: Optional[float] = None

    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        candidate = float(raw)
    elif isinstance(raw, str):
        try:
            candidate = float(raw)
        except ValueError:
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, (int, float)) and not isinstance(parsed, bool):
                candidate = float(parsed)
            elif isinstance(parsed, Mapping):
                inner = parsed.get("value")
                if isinstance(inner, (int, float)) and not isinstance(inner, bool):
                    candidate = float(inner)
    elif isinstance(raw, Mapping):
        inner = raw.get("value")
        if inner is None:
            inner = raw.get("score")
        if isinstance(inner, (int, float)) and not isinstance(inner, bool):
            candidate = float(inner)

    if candidate is None or candidate != candidate:  # NaN check
        _LOG.warning(
            "ranking_factor_malformed",
            factor=factor_name,
            payload_type=type(raw).__name__,
        )
        return default

    if candidate < 0.0:
        return 0.0
    if candidate > 1.0:
        return 1.0
    return candidate


def _abs_news_sentiment(
    raw: Any,
    *,
    default: float,
) -> float:
    """Map a :class:`NewsImpact` payload (or list thereof) to a ``[0,1]`` factor.

    The cached news entries are dicts that match the canonical
    :class:`hedge_warm_ai.schemas.NewsImpact` shape; the relevant
    field for ranking is ``impact_magnitude`` (already in ``[0, 1]``).
    The provider reads the *most recent* (newest-first) entry — that
    is what :meth:`RedisHotCache.recent_news` returns by construction.
    """
    if raw is None:
        return default

    if isinstance(raw, list):
        if not raw:
            return default
        head = raw[0]
    else:
        head = raw

    if isinstance(head, Mapping):
        magnitude = head.get("impact_magnitude")
        if isinstance(magnitude, (int, float)) and not isinstance(magnitude, bool):
            value = float(magnitude)
            if value != value:  # NaN
                return default
            if value < 0.0:
                return 0.0
            if value > 1.0:
                return 1.0
            return value

    return _coerce_unit_factor(head, factor_name="news_sentiment", default=default)


def _discipline_from_psych_stability(
    raw: Any,
    *,
    default: float,
) -> float:
    """Pull ``components.discipline`` from a cached ``PsychStability`` payload.

    The Trader_Psychology_Engine writes the latest stability event to
    :meth:`RedisHotCache.set_stability_score`. The cached payload is
    the JSON-encoded :class:`hedge_warm_ai.schemas.PsychStability`
    model (or its ``model_dump(mode="json")`` dict).
    """
    if raw is None:
        return default

    if isinstance(raw, Mapping):
        components = raw.get("components")
        if isinstance(components, Mapping):
            inner = components.get("discipline")
            if isinstance(inner, (int, float)) and not isinstance(inner, bool):
                return _coerce_unit_factor(
                    inner,
                    factor_name="trader_discipline",
                    default=default,
                )
        # Some callers may store only the bare scalar score.
        score = raw.get("score")
        if isinstance(score, (int, float)) and not isinstance(score, bool):
            return _coerce_unit_factor(
                score,
                factor_name="trader_discipline",
                default=default,
            )

    return _coerce_unit_factor(
        raw,
        factor_name="trader_discipline",
        default=default,
    )


@dataclass
class FactorDefaults:
    """Catch-all defaults used when a factor source is missing or stale.

    Each field is in ``[0.0, 1.0]``. Defaults are tuned to neutral
    (0.5) so a fully-degraded ranking engine produces a mid-band
    score rather than zero (which the Risk_Engine would treat as
    "no confidence" and reject every signal).
    """

    orderflow: float = 0.5
    technical_strength: float = 0.5
    news_sentiment: float = 0.5
    market_regime: float = 0.5
    trader_discipline: float = 0.5

    def to_factors(self) -> RankingFactors:
        return RankingFactors(
            orderflow=self.orderflow,
            technical_strength=self.technical_strength,
            news_sentiment=self.news_sentiment,
            market_regime=self.market_regime,
            trader_discipline=self.trader_discipline,
        )


class RankingFactorProvider:
    """Production :class:`FactorProvider` reading from :class:`RedisHotCache`.

    Each lookup runs the five reads concurrently and, on any read
    failure, falls back to the configured default for that factor.
    Lookups never raise — the engine's 5 ms p95 budget (R17.5) does
    not tolerate an exception path.

    Args:
        hot_cache: An already-started
            :class:`hedge_memory_rag.redis_cache.RedisHotCache`.
            The provider does not own the cache lifecycle; the
            service-layer entry point starts and stops it.
        defaults: :class:`FactorDefaults` to use when a per-factor
            lookup misses or returns a malformed value.
        staleness_window_s: Maximum permitted age (seconds) for a
            cached factor entry. Entries with a ``ts_ns`` field older
            than ``now - staleness_window_s`` are treated as missing.
            ``None`` disables the staleness check (the cache's own
            TTL is then the only bound).
        clock: Override of :func:`time.time` for tests.
    """

    def __init__(
        self,
        *,
        hot_cache: "RedisHotCache",
        defaults: Optional[FactorDefaults] = None,
        staleness_window_s: Optional[float] = None,
        clock: Optional[Any] = None,
    ) -> None:
        self._hot = hot_cache
        self._defaults = defaults or FactorDefaults()
        self._staleness_window_s = staleness_window_s
        self._clock = clock or time.time

    @property
    def defaults(self) -> FactorDefaults:
        return self._defaults

    async def factors_for(self, event: SignalEvent) -> RankingFactors:
        symbol = event.symbol
        if not symbol:
            # Portfolio-scoped signal — only the portfolio-level
            # factors (market regime + trader discipline) are
            # relevant. Use neutral defaults for the per-symbol ones.
            market_regime = await self._read_market_regime()
            trader_discipline = await self._read_trader_discipline()
            return RankingFactors(
                orderflow=self._defaults.orderflow,
                technical_strength=self._defaults.technical_strength,
                news_sentiment=self._defaults.news_sentiment,
                market_regime=market_regime,
                trader_discipline=trader_discipline,
            )

        orderflow = await self._read_per_symbol_factor(
            suffix=_KEY_FACTORS_ORDERFLOW,
            symbol=symbol,
            factor_name="orderflow",
            default=self._defaults.orderflow,
        )
        technical_strength = await self._read_per_symbol_factor(
            suffix=_KEY_FACTORS_TECHNICAL_STRENGTH,
            symbol=symbol,
            factor_name="technical_strength",
            default=self._defaults.technical_strength,
        )
        news_sentiment = await self._read_news_sentiment(symbol)
        market_regime = await self._read_market_regime()
        trader_discipline = await self._read_trader_discipline()

        return RankingFactors(
            orderflow=orderflow,
            technical_strength=technical_strength,
            news_sentiment=news_sentiment,
            market_regime=market_regime,
            trader_discipline=trader_discipline,
        )

    # -----------------------------------------------------------------
    # Per-factor read helpers -----------------------------------------
    # -----------------------------------------------------------------

    async def _read_per_symbol_factor(
        self,
        *,
        suffix: str,
        symbol: str,
        factor_name: str,
        default: float,
    ) -> float:
        """Read a per-symbol scalar factor from the Hot_Path-to-Warm_AI bridge."""
        # The hot cache's ``_get_simple`` is private; we go through
        # the underlying client + the same JSON codec so the staleness
        # check sees the raw payload.
        try:
            raw = await self._read_namespaced_key(f"{suffix}:{symbol}")
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ranking_factor_read_failed",
                factor=factor_name,
                symbol=symbol,
                error=str(exc),
            )
            return default
        if raw is None:
            return default
        if not self._is_fresh(raw):
            return default
        return _coerce_unit_factor(
            raw, factor_name=factor_name, default=default
        )

    async def _read_news_sentiment(self, symbol: str) -> float:
        """Read the most recent ``NewsImpact`` for *symbol*."""
        try:
            recent = await self._hot.recent_news(symbol)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ranking_news_sentiment_read_failed",
                symbol=symbol,
                error=str(exc),
            )
            return self._defaults.news_sentiment
        if not recent:
            return self._defaults.news_sentiment
        head = recent[0]
        if isinstance(head, Mapping) and not self._is_fresh(head):
            return self._defaults.news_sentiment
        return _abs_news_sentiment(head, default=self._defaults.news_sentiment)

    async def _read_market_regime(self) -> float:
        """Read the current ``MarketStability`` factor."""
        try:
            raw = await self._hot.get_market_stability()
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ranking_market_regime_read_failed",
                error=str(exc),
            )
            return self._defaults.market_regime
        if raw is None:
            return self._defaults.market_regime
        if not self._is_fresh(raw):
            return self._defaults.market_regime
        return _coerce_unit_factor(
            raw,
            factor_name="market_regime",
            default=self._defaults.market_regime,
        )

    async def _read_trader_discipline(self) -> float:
        """Read the current ``components.discipline`` from the latest psych event."""
        try:
            raw = await self._hot.get_stability_score()
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ranking_trader_discipline_read_failed",
                error=str(exc),
            )
            return self._defaults.trader_discipline
        if raw is None:
            return self._defaults.trader_discipline
        if not self._is_fresh(raw):
            return self._defaults.trader_discipline
        return _discipline_from_psych_stability(
            raw, default=self._defaults.trader_discipline
        )

    # -----------------------------------------------------------------
    # Helpers ---------------------------------------------------------
    # -----------------------------------------------------------------

    async def _read_namespaced_key(self, suffix: str) -> Any:
        """Read ``<namespace>:<suffix>`` through the cache's JSON codec.

        Goes through the cache instance's underlying ``redis.asyncio``
        client to avoid leaking the internal ``_get_simple`` method.
        Returns ``None`` if the key is missing.
        """
        # We deliberately depend on ``RedisHotCache`` exposing a
        # ``_require_client``-style accessor. Until 44.x lands, we
        # use the public ``recent_trades`` / ``set_market_stability``
        # APIs for the well-known keys; for the orderflow/technical-
        # strength keys we have to reach through. The cache is
        # internally async-safe so the access is correct, just
        # private. When 44.x adds a dedicated ``get_factor(symbol,
        # name)`` method, this helper goes away.
        from hedge_memory_rag.redis_cache.codec import decode_payload  # local import
        from hedge_memory_rag.redis_cache.errors import RedisCacheError

        client = getattr(self._hot, "_client", None)
        config = getattr(self._hot, "_config", None)
        if client is None or config is None:
            raise RankingFactorError(
                "RedisHotCache exposes no underlying client; "
                "cannot read raw factor key"
            )
        key = f"{config.namespace}:{suffix}"
        try:
            raw = await client.get(key)
        except Exception as exc:
            raise RedisCacheError(
                f"redis error reading factor key {key!r}: {exc}",
                op="ranking.read_factor",
                key=key,
            ) from exc
        return decode_payload(raw, op="ranking.read_factor", key=key)

    def _is_fresh(self, payload: Any) -> bool:
        """Return ``True`` if *payload*'s ``ts_ns`` is within the staleness window.

        Payloads without a ``ts_ns`` field are treated as fresh — the
        cache's TTL is then the only bound.
        """
        if self._staleness_window_s is None:
            return True
        if not isinstance(payload, Mapping):
            return True
        ts_ns = payload.get("ts_ns")
        if not isinstance(ts_ns, (int, float)):
            return True
        now_ns = int(self._clock() * 1_000_000_000)
        cutoff_ns = now_ns - int(self._staleness_window_s * 1_000_000_000)
        return int(ts_ns) >= cutoff_ns


__all__ = [
    "FactorDefaults",
    "FactorProvider",
    "InMemoryFactorProvider",
    "RankingFactorProvider",
    "StubFactorProvider",
]
