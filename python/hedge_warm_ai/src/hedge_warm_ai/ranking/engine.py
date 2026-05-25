"""AI_Trade_Ranking_Engine — task 26.1 (R17.1–R17.5).

The engine ties together the four collaborators introduced in this
subpackage:

* :class:`~.factors.FactorProvider` — resolves the five factor inputs
  for one :class:`SignalEvent` (R17.1 inputs).
* :class:`~.publisher.RankPublisher` — emits
  ``ai.rank.<correlation_id>`` on the canonical NATS subject (R17.3).
* :class:`~.warm_cache.AiRankCache` — writes the latest per-symbol
  :class:`AiRank` to the interim WarmCache adaptor (R17.4 — Risk_Engine
  reads via WarmCache only).
* :class:`~.config.RankingConfig` — supplies cache namespace, factor
  staleness window, and ranking timeout (R17.5).

The engine is **strictly off the Hot_Path** (R17.4, Property 2 —
Authority Hierarchy and Hot_Path Purity). The Hot_Path Risk_Engine
consumes ranking results via the WarmCache last-known-value path only;
it does not subscribe to ``ai.rank.<cid>`` and never awaits this
engine. The publish + cache path is fire-and-forget at the engine
layer; broker-side failures are logged via structlog and surfaced as
typed :class:`hedge_warm_ai.ranking.errors` exceptions to the
Self_Healing_Supervisor.

The engine is async-first. The single public entry point is:

* :meth:`AiTradeRankingEngine.rank` — feed one :class:`SignalEvent`;
  the engine resolves its factor inputs, computes the score, builds
  the canonical :class:`hedge_warm_ai.schemas.RankedSignal` payload,
  publishes ``ai.rank.<correlation_id>``, writes the latest
  :class:`AiRank` to the interim WarmCache, and returns a
  :class:`RankingSample` describing what happened.

Determinism: the score is a pure function of
``compute_trade_confidence_score(factors)`` once the factor provider
and config are fixed. Tests in 26.2 enumerate inputs and assert
formula equivalence (Property 4) and per-call latency (Property 3).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Final

import structlog

from ..schemas import RankedSignal
from .config import RankingConfig
from .errors import (
    RankingCacheError,
    RankingPublishError,
)
from .factors import FactorProvider, StubFactorProvider
from .publisher import (
    NoopRankPublisher,
    RankPublisher,
    ai_rank_subject,
)
from .score import (
    RankingFactors,
    compute_trade_confidence_score,
)
from .state import AiRank, RankingSample, SignalEvent
from .warm_cache import (
    AiRankCache,
    InMemoryAiRankCache,
)

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Engine --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class AiTradeRankingEngine:
    """Orchestrator for the AI_Trade_Ranking_Engine subsystem (R17).

    Lifecycle::

        engine = AiTradeRankingEngine(
            config=RankingConfig.from_hedge_config(hedge_cfg),
            factor_provider=RankingFactorProvider(hot_cache=redis_hot_cache),
            publisher=NatsRankPublisher(async_publish=nc.publish),
            rank_cache=RedisAiRankCache(redis_hot_cache),
        )
        sample = await engine.rank(signal_event)

    Threading: the engine is intended to be driven by an asyncio task
    that pulls ``sig.emitted`` events off NATS and feeds them in.
    Multiple concurrent :meth:`rank` calls are safe (no internal
    mutable state beyond the ``ranked_count`` counter).

    Construction:

    * ``config`` — resolved :class:`RankingConfig`.
    * ``factor_provider`` — concrete :class:`FactorProvider`.
      Defaults to :class:`StubFactorProvider` (returns the all-zeros
      :class:`RankingFactors`) so unit tests that focus on the
      formula or the publisher do not need to wire a real provider.
    * ``publisher`` — concrete :class:`RankPublisher`. Defaults to
      :class:`NoopRankPublisher` so unit tests that focus on the
      score do not need to wire NATS.
    * ``rank_cache`` — concrete :class:`AiRankCache`. Defaults to
      :class:`InMemoryAiRankCache` for the same reason.
    * ``clock_ns`` — wall-clock ns callable. Defaults to
      :func:`time.time_ns`. Override in tests for determinism.
    """

    config: RankingConfig
    factor_provider: FactorProvider = field(default_factory=StubFactorProvider)
    publisher: RankPublisher = field(default_factory=NoopRankPublisher)
    rank_cache: AiRankCache = field(default_factory=InMemoryAiRankCache)
    clock_ns: Callable[[], int] = field(default=time.time_ns)
    ranked_count: int = field(default=0, init=False)

    # ----- public API ------------------------------------------------------

    async def rank(self, event: SignalEvent) -> RankingSample:
        """Score one :class:`SignalEvent` and emit ``ai.rank.<cid>``.

        Steps:

        1. Resolve the five factor inputs via
           :meth:`FactorProvider.factors_for`. The provider is
           guaranteed not to raise (R17.5 latency budget); on a
           degraded source it returns the configured defaults.
        2. Compute the score via
           :func:`compute_trade_confidence_score` — the canonical
           closed-form expression with named module-level weights
           (R17.1, R17.2).
        3. Build the canonical
           :class:`hedge_warm_ai.schemas.RankedSignal` payload
           (the wire shape of ``ai.rank.<cid>``).
        4. Publish ``ai.rank.<correlation_id>`` via the configured
           :class:`RankPublisher` (R17.3).
        5. Write the latest :class:`AiRank` to the interim WarmCache
           via the configured :class:`AiRankCache` so the
           Hot_Path Risk_Engine reads it without subscribing
           (R17.4).

        Args:
            event: The normalised :class:`SignalEvent`.

        Returns:
            A :class:`RankingSample` describing the outcome (the
            :class:`AiRank`, whether the WarmCache write succeeded,
            and the published subject).

        Raises:
            RankingPublishError: NATS publish failed. The engine
                still attempts the WarmCache write so the Risk_Engine
                sees the latest rank even when the bus is degraded.
        """
        factors = await self.factor_provider.factors_for(event)
        score = compute_trade_confidence_score(factors)

        rank = AiRank(
            correlation_id=event.correlation_id,
            signal_id=event.signal_id,
            trade_confidence_score=score,
            factors=factors,
            symbol=event.symbol,
            shadow=event.shadow,
            ts_ns=event.ts_ns if event.ts_ns > 0 else int(self.clock_ns()),
        )

        # Write the latest rank to the WarmCache *before* publishing
        # so a transient NATS failure does not prevent the Risk_Engine
        # (which reads via WarmCache) from seeing the score. The
        # publish failure is surfaced to the supervisor via the
        # typed exception.
        cache_write_succeeded = await self._write_cache(rank)

        wire = self._build_ranked_signal(rank)
        publish_subject = ai_rank_subject(rank.correlation_id.hex())
        try:
            await self.publisher.publish_rank(wire)
        except RankingPublishError:
            self.ranked_count += 1
            raise

        self.ranked_count += 1
        return RankingSample(
            rank=rank,
            cache_write_succeeded=cache_write_succeeded,
            publish_subject=publish_subject,
        )

    # ----- internals -------------------------------------------------------

    def _build_ranked_signal(self, rank: AiRank) -> RankedSignal:
        """Compose the canonical ``ai.rank.<cid>`` wire payload."""
        # The :class:`RankedSignal` Pydantic model re-validates every
        # field on construction; out-of-range scores or factors raise
        # ``ValidationError`` immediately so a malformed payload
        # cannot make it onto the bus.
        return RankedSignal.model_validate(
            {
                "correlation_id": rank.correlation_id.hex(),
                "signal_id": rank.signal_id,
                "trade_confidence_score": float(rank.trade_confidence_score),
                "factors": {
                    "orderflow": float(rank.factors.orderflow),
                    "technical_strength": float(rank.factors.technical_strength),
                    "news_sentiment": float(rank.factors.news_sentiment),
                    "market_regime": float(rank.factors.market_regime),
                    "trader_discipline": float(rank.factors.trader_discipline),
                },
                "shadow": bool(rank.shadow),
                "ts_ns": int(rank.ts_ns),
            }
        )

    async def _write_cache(self, rank: AiRank) -> bool:
        """Write *rank* to the interim WarmCache. Failures are logged + dropped.

        The Risk_Engine has a documented fallback to
        ``Signal_v1.confidence`` when the cache is stale (design §
        Components § AI_Trade_Ranking_Engine), so a single failed
        write is non-fatal at the engine layer. Persistent failure
        surfaces to the Self_Healing_Supervisor through the
        :class:`RankingCacheError` typed exception class — but the
        engine itself does *not* re-raise so the publish path is
        unaffected.
        """
        try:
            await self.rank_cache.set_rank(rank)
        except RankingCacheError as exc:
            _LOG.warning(
                "ai_rank_cache_write_failed",
                signal_id=rank.signal_id,
                symbol=rank.symbol,
                trade_confidence_score=rank.trade_confidence_score,
                error=str(exc),
            )
            return False
        except Exception as exc:  # pragma: no cover - defensive
            _LOG.warning(
                "ai_rank_cache_write_unhandled",
                signal_id=rank.signal_id,
                symbol=rank.symbol,
                error=str(exc),
            )
            return False
        return True


__all__ = [
    "AiTradeRankingEngine",
    "RankingFactors",
    "RankingSample",
]
