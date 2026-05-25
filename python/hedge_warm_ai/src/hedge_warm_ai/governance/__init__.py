"""AI_Governance_Engine — task 28.1 of the PROJECT HEDGE spec.

This sub-package implements the design's *AI_Governance_Engine*
(design § Components — AI_Governance_Engine) and the requirements
23.3, 24.1, 24.2, 24.3, 24.4 from ``requirements.md``.

It does five things:

1. **Per-component metric tracking** (R24.1). For each governed
   Warm_AI_Pipeline component (``news``, ``regime``, ``priority``,
   ``prev_day``, ``psychology``, ``ranking``, ``journal``), the
   engine maintains four rolling-window metric estimators —
   :class:`MetricKind.DRIFT`,
   :class:`MetricKind.CONFIDENCE_STABILITY`,
   :class:`MetricKind.HALLUCINATION_INDICATORS`,
   :class:`MetricKind.PREDICTION_QUALITY`.
2. **Influence reduction on degradation** (R24.2). When a metric
   crosses ``degradation_threshold``, the engine writes a
   per-component :class:`GovernanceWeightPayload` to the interim
   WarmCache key ``hedge.warm.governance.<component>``. The Risk_Engine
   and the AI_Trade_Ranking_Engine read this multiplier through the
   WarmCache last-known-value path and apply it on every
   ``Trade_Confidence_Score`` and ``Adaptive_Risk`` recomputation.
3. **Shadow mode on critical** (R24.3). When a metric crosses
   ``critical_threshold``, the engine writes a flag to the interim
   ``hedge.warm.shadow.<component>`` key. The AI_Shadow_Mode service
   (task 29.1) consumes this flag.
4. **Edge-triggered ``ai.gov.action`` emission** (R24.4, Property 8).
   One publication per per-component level transition; the
   :class:`hedge_warm_ai.schemas.AiGovAction` payload carries
   ``component``, ``action``, ``metric``, ``value``, ``threshold``,
   ``ts_ns``.
5. **Prediction-quality scoring against realised outcomes** (R23.3).
   The engine subscribes to ``exec.trade.closed`` and
   ``pos.update.<sym>``, correlates each outcome against the
   originating component output by ``correlation_id``, appends a
   match / mis-match indicator to the per-component
   :class:`MetricKind.PREDICTION_QUALITY` window, and persists the
   result to the ``governance_metrics`` Timescale hypertable.

Module layout
-------------

* :mod:`.state`       — :class:`GovernedComponent`, :class:`MetricKind`,
                        :class:`ComponentOutput`, :class:`TradeOutcome`,
                        :class:`RollingWindow`, :func:`wire_metric_for`.
* :mod:`.metrics`     — pure functions :func:`compute_drift`,
                        :func:`compute_confidence_stability`,
                        :func:`compute_hallucination_rate`,
                        :func:`compute_prediction_inaccuracy`.
* :mod:`.ladder`      — :class:`GovernanceLevel`,
                        :class:`GovernanceLadder`,
                        :class:`LadderTransition`,
                        :data:`DEFAULT_WEIGHT_BY_LEVEL`,
                        :func:`action_for`.
* :mod:`.config`      — :class:`GovernanceMetricThresholds`,
                        :class:`GovernanceConfig`.
* :mod:`.publisher`   — :class:`AiGovActionPublisher` protocol +
                        ``Noop``, ``InMemory``, ``Nats`` implementations.
* :mod:`.warm_cache`  — :class:`GovernanceWarmCache` protocol +
                        :class:`InMemoryGovernanceWarmCache`,
                        :class:`RedisGovernanceWarmCache`.
* :mod:`.persistence` — :class:`GovernanceMetricSink` protocol +
                        :class:`NoopGovernanceMetricSink`,
                        :class:`TimescaleGovernanceMetricSink`.
* :mod:`.subjects`    — canonical NATS subjects + Redis namespaces.
* :mod:`.engine`      — :class:`AiGovernanceEngine` orchestrator +
                        :class:`GovernanceSample`.
* :mod:`.errors`      — typed exception hierarchy.
* :mod:`.service`     — ``hedge-governance`` console-script entry point.

Heavy dependencies (:mod:`hedge_memory_rag`, :mod:`redis.asyncio`)
are imported lazily inside the adaptor modules so importing this
package does not pay the cost of the RAG layer in environments that
only need the metric estimators or the value types.
"""

from __future__ import annotations

from .config import (
    DEFAULT_CRITICAL_THRESHOLD,
    DEFAULT_DEGRADATION_THRESHOLD,
    DEFAULT_DRIFT_REFERENCE_WINDOW,
    DEFAULT_DRIFT_WINDOW,
    DEFAULT_HALLUCINATION_WINDOW,
    DEFAULT_PREDICTION_WINDOW,
    DEFAULT_STABILITY_WINDOW,
    GovernanceConfig,
    GovernanceMetricThresholds,
)
from .engine import AiGovernanceEngine, GovernanceSample
from .errors import (
    GovernanceCacheError,
    GovernanceConfigError,
    GovernanceEngineError,
    GovernancePersistenceError,
    GovernancePublishError,
)
from .ladder import (
    DEFAULT_WEIGHT_BY_LEVEL,
    GovernanceLadder,
    GovernanceLevel,
    LadderTransition,
    action_for,
)
from .metrics import (
    compute_confidence_stability,
    compute_drift,
    compute_hallucination_rate,
    compute_prediction_inaccuracy,
)
from .persistence import (
    GovernanceMetricSink,
    NoopGovernanceMetricSink,
    TimescaleGovernanceMetricSink,
)
from .publisher import (
    AiGovActionPublisher,
    InMemoryAiGovActionPublisher,
    NatsAiGovActionPublisher,
    NoopAiGovActionPublisher,
)
from .state import (
    ComponentOutput,
    DEFAULT_COMPONENTS,
    GovernedComponent,
    MetricKind,
    RollingWindow,
    TradeOutcome,
    wire_metric_for,
)
from .subjects import (
    DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE,
    DEFAULT_SHADOW_FLAG_NAMESPACE,
    SUBJECT_AI_GOV_ACTION,
    SUBJECT_EXEC_TRADE_CLOSED,
    SUBJECT_POS_UPDATE_PREFIX,
    governance_weight_key,
    pos_update_subject_pattern,
    shadow_flag_key,
)
from .warm_cache import (
    DEFAULT_SHADOW_TTL_S,
    DEFAULT_WEIGHT_TTL_S,
    GovernanceWarmCache,
    GovernanceWeightPayload,
    InMemoryGovernanceWarmCache,
    RedisGovernanceWarmCache,
)

__all__ = [
    # config
    "DEFAULT_CRITICAL_THRESHOLD",
    "DEFAULT_DEGRADATION_THRESHOLD",
    "DEFAULT_DRIFT_REFERENCE_WINDOW",
    "DEFAULT_DRIFT_WINDOW",
    "DEFAULT_HALLUCINATION_WINDOW",
    "DEFAULT_PREDICTION_WINDOW",
    "DEFAULT_STABILITY_WINDOW",
    "GovernanceConfig",
    "GovernanceMetricThresholds",
    # engine
    "AiGovernanceEngine",
    "GovernanceSample",
    # errors
    "GovernanceCacheError",
    "GovernanceConfigError",
    "GovernanceEngineError",
    "GovernancePersistenceError",
    "GovernancePublishError",
    # ladder
    "DEFAULT_WEIGHT_BY_LEVEL",
    "GovernanceLadder",
    "GovernanceLevel",
    "LadderTransition",
    "action_for",
    # metrics
    "compute_confidence_stability",
    "compute_drift",
    "compute_hallucination_rate",
    "compute_prediction_inaccuracy",
    # persistence
    "GovernanceMetricSink",
    "NoopGovernanceMetricSink",
    "TimescaleGovernanceMetricSink",
    # publisher
    "AiGovActionPublisher",
    "InMemoryAiGovActionPublisher",
    "NatsAiGovActionPublisher",
    "NoopAiGovActionPublisher",
    # state
    "ComponentOutput",
    "DEFAULT_COMPONENTS",
    "GovernedComponent",
    "MetricKind",
    "RollingWindow",
    "TradeOutcome",
    "wire_metric_for",
    # subjects
    "DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE",
    "DEFAULT_SHADOW_FLAG_NAMESPACE",
    "SUBJECT_AI_GOV_ACTION",
    "SUBJECT_EXEC_TRADE_CLOSED",
    "SUBJECT_POS_UPDATE_PREFIX",
    "governance_weight_key",
    "pos_update_subject_pattern",
    "shadow_flag_key",
    # warm cache
    "DEFAULT_SHADOW_TTL_S",
    "DEFAULT_WEIGHT_TTL_S",
    "GovernanceWarmCache",
    "GovernanceWeightPayload",
    "InMemoryGovernanceWarmCache",
    "RedisGovernanceWarmCache",
]
