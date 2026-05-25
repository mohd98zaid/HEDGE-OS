"""AI_Trade_Ranking_Engine — task 26.1 of the PROJECT HEDGE spec.

This sub-package implements the design's *AI_Trade_Ranking_Engine*
(design § Components — AI_Trade_Ranking_Engine) and the requirements
17.1–17.5 from ``requirements.md``.

It does five things:

1. **Score computation.** :func:`compute_trade_confidence_score`
   implements the exact closed-form formula
   ``clamp(0.30×O + 0.25×T + 0.20×N + 0.15×M + 0.10×D, 0.0, 1.0)``
   (R17.1, R17.2, Property 4 — Score and Formula Equivalence). The
   five weights are exposed as named module constants so the
   formula's audit-trail matches the design verbatim.
2. **Asynchronous ingestion.** The engine subscribes to
   ``sig.emitted`` (R17.3) via the existing NATS subscriber wrappers
   shipping in task 4.2. The subscriber + decoder wiring lives in
   the service-layer (``hedge-rank`` console-script entry point);
   the engine itself accepts a normalised :class:`SignalEvent` so
   unit tests do not need FlatBuffers.
3. **Per-correlation-id emission.** :class:`RankPublisher` emits
   ``ai.rank.<correlation_id>`` (R17.3) carrying the original
   ``signal_id``, the per-factor :class:`RankingFactors`, the
   clamped score, and the originating ``ts_ns``.
4. **WarmCache write (R17.4).** The latest per-symbol :class:`AiRank`
   is written to the interim Redis WarmCache namespace
   (``hedge.warm.rank.<symbol>``) so the Hot_Path Risk_Engine reads
   it via the WarmCache last-known-value path **without subscribing**
   to the ``ai.rank.*`` fan-out. The future Rust ``hedge-warmcache``
   crate (task 44.x) will replace this path without changing the
   engine's public API surface.
5. **Off-Hot_Path execution (R17.4, Property 2).** The engine runs in
   the Warm_AI_Pipeline event loop and never blocks the Hot_Path.
   The Hot_Path Risk_Engine reads the WarmCache slot synchronously
   in <50 µs and falls back to ``Signal_v1.confidence`` when the slot
   is stale (design § Components § AI_Trade_Ranking_Engine).

Module layout:

* :mod:`.score`       — :func:`compute_trade_confidence_score` +
                        named weight constants + :class:`RankingFactors`.
* :mod:`.state`       — :class:`SignalEvent`, :class:`AiRank`,
                        :class:`RankingSample` value types.
* :mod:`.factors`     — :class:`FactorProvider` protocol +
                        :class:`StubFactorProvider`,
                        :class:`InMemoryFactorProvider`,
                        :class:`RankingFactorProvider`
                        (Redis-backed production adaptor).
* :mod:`.publisher`   — :class:`RankPublisher` protocol +
                        ``Noop``, ``InMemory``, ``Nats``
                        implementations mirroring
                        :mod:`hedge_warm_ai.psychology.publisher`.
* :mod:`.warm_cache`  — :class:`AiRankCache` protocol +
                        :class:`RedisAiRankCache` (interim, until
                        the Rust WarmCache crate / task 44.x lands).
* :mod:`.config`      — :class:`RankingConfig` (cache namespace,
                        factor staleness window, ranking timeout).
* :mod:`.engine`      — :class:`AiTradeRankingEngine` orchestrator.
* :mod:`.errors`      — typed exception hierarchy.
* :mod:`.service`     — ``hedge-rank`` console-script entry point.

Heavy dependencies (:mod:`hedge_memory_rag`, :mod:`redis.asyncio`)
are imported lazily inside the adaptor modules so importing this
package does not pay the cost of the RAG layer in environments that
only need the formula or the value types.
"""

from __future__ import annotations

from .config import (
    DEFAULT_FACTOR_STALENESS_WINDOW_S,
    DEFAULT_RANK_CACHE_NAMESPACE,
    DEFAULT_RANK_CACHE_TTL_S,
    DEFAULT_RANKING_TIMEOUT_MS,
    RankingConfig,
)
from .engine import AiTradeRankingEngine
from .errors import (
    RankingCacheError,
    RankingConfigError,
    RankingEngineError,
    RankingFactorError,
    RankingPublishError,
)
from .factors import (
    FactorDefaults,
    FactorProvider,
    InMemoryFactorProvider,
    RankingFactorProvider,
    StubFactorProvider,
)
from .publisher import (
    AI_RANK_PREFIX,
    InMemoryRankPublisher,
    NatsRankPublisher,
    NoopRankPublisher,
    RankPublisher,
    ai_rank_subject,
)
from .score import (
    MARKET_REGIME_WEIGHT,
    NEWS_SENTIMENT_WEIGHT,
    ORDERFLOW_WEIGHT,
    RankingFactors,
    TECHNICAL_STRENGTH_WEIGHT,
    TRADER_DISCIPLINE_WEIGHT,
    compute_trade_confidence_score,
)
from .state import AiRank, RankingSample, Side, SignalEvent
from .warm_cache import (
    AiRankCache,
    InMemoryAiRankCache,
    RedisAiRankCache,
)

__all__ = [
    # config
    "DEFAULT_FACTOR_STALENESS_WINDOW_S",
    "DEFAULT_RANK_CACHE_NAMESPACE",
    "DEFAULT_RANK_CACHE_TTL_S",
    "DEFAULT_RANKING_TIMEOUT_MS",
    "RankingConfig",
    # engine
    "AiTradeRankingEngine",
    # errors
    "RankingCacheError",
    "RankingConfigError",
    "RankingEngineError",
    "RankingFactorError",
    "RankingPublishError",
    # factors
    "FactorDefaults",
    "FactorProvider",
    "InMemoryFactorProvider",
    "RankingFactorProvider",
    "StubFactorProvider",
    # publisher
    "AI_RANK_PREFIX",
    "InMemoryRankPublisher",
    "NatsRankPublisher",
    "NoopRankPublisher",
    "RankPublisher",
    "ai_rank_subject",
    # score
    "MARKET_REGIME_WEIGHT",
    "NEWS_SENTIMENT_WEIGHT",
    "ORDERFLOW_WEIGHT",
    "RankingFactors",
    "TECHNICAL_STRENGTH_WEIGHT",
    "TRADER_DISCIPLINE_WEIGHT",
    "compute_trade_confidence_score",
    # state
    "AiRank",
    "RankingSample",
    "Side",
    "SignalEvent",
    # warm cache
    "AiRankCache",
    "InMemoryAiRankCache",
    "RedisAiRankCache",
]
