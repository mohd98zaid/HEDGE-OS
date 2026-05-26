"""AI_Shadow_Mode service — task 29.1 (R23.1, R23.2, R23.3).

This sub-package implements the design's *AI_Shadow_Mode* service
(design § Operating Modes § AI_Shadow_Mode) and the requirements
23.1, 23.2, 23.3 from ``requirements.md``:

* When a component is shadowed, its outputs are produced and
  persisted with timestamps but tagged ``shadow=True`` (R23.1).
* The UI gateway filters ``shadow=True`` outputs out of the
  ranked-signal display surfaced to the trader (R23.2).
* The AI_Governance_Engine still consumes shadowed outputs to
  compute accuracy metrics (R23.3).

Public surface
--------------

* :class:`ShadowModeService`        — orchestrator (poll loop +
  ``is_shadowed`` query + ``persist_output`` API).
* :class:`ShadowModeConfig` /
  :func:`load_shadow_mode_config`    — config resolved from
  :class:`hedge_warm_ai.config.HedgeConfig` (poll interval, buffer,
  seed components, flag namespace).
* :class:`ShadowedOutput` /
  :class:`ShadowSnapshot` /
  :class:`ShadowKind`                 — value types passed through
  the service.
* :class:`ShadowFilter`              — UI gateway filter callable
  (drops payloads with ``shadow=True``).
* :class:`ShadowFlagSource` +
  :class:`InMemoryShadowFlagSource` /
  :class:`RedisShadowFlagSource`     — shadow-flag source adaptors.
* :class:`ShadowedOutputSink` +
  :class:`InMemoryShadowedOutputSink` /
  :class:`NoopShadowedOutputSink` /
  :class:`TimescaleShadowedOutputSink` — persistence sinks.
* :class:`GovernanceObserver` +
  :class:`InMemoryGovernanceObserver` /
  :class:`NoopGovernanceObserver` /
  :class:`AiGovernanceEngineObserver` — adapters that forward
  shadowed outputs to the AI_Governance_Engine for R23.3 accuracy
  scoring.
* canonical NATS subjects + Redis namespaces in
  :mod:`hedge_warm_ai.shadow.subjects`.
* typed exception hierarchy in :mod:`hedge_warm_ai.shadow.errors`.

Heavy dependencies (:mod:`hedge_memory_rag`, :mod:`redis.asyncio`,
:mod:`hedge_warm_ai.governance`) are imported lazily inside the
adaptor modules so importing this package does not pay the cost of
the RAG layer or the governance subsystem in environments that only
need the value types or the UI filter.

References
----------
- Requirements §23 — R23.1, R23.2, R23.3.
- Design § Operating Modes § AI_Shadow_Mode.
- Design § Components § AI_Governance_Engine (R24.3 — flag write).
- Design § Correctness Properties § Property 5 — Persistence
  Round-Trip; Property 10 — Subscriber Delivery.
"""

from __future__ import annotations

from .config import (
    DEFAULT_SHADOW_FLAG_NAMESPACE,
    DEFAULT_SHADOW_PERSISTENCE_BUFFER,
    DEFAULT_SHADOW_POLL_INTERVAL_S,
    ShadowModeConfig,
)
from .engine import ShadowModeService, chain_filters
from .errors import (
    ShadowConfigError,
    ShadowEngineError,
    ShadowFlagSourceError,
    ShadowPersistenceError,
)
from .filter import ShadowFilter, is_payload_shadowed, passes_ui_filter
from .flag_source import (
    InMemoryShadowFlagSource,
    RedisShadowFlagSource,
    ShadowFlagSource,
)
from .governance_observer import (
    AiGovernanceEngineObserver,
    GovernanceObserver,
    InMemoryGovernanceObserver,
    NoopGovernanceObserver,
)
from .persistence import (
    InMemoryShadowedOutputSink,
    NoopShadowedOutputSink,
    ShadowedOutputSink,
    TimescaleShadowedOutputSink,
)
from .state import (
    EMPTY_SHADOW_SNAPSHOT,
    ShadowKind,
    ShadowSnapshot,
    ShadowedOutput,
)
from .subjects import (
    SHADOW_FLAG_NAMESPACE,
    SUBJECT_AI_JOURNAL_ENTRY,
    SUBJECT_AI_NEWS_IMPACT_PREFIX,
    SUBJECT_AI_PRIORITY_CHANGED_PREFIX,
    SUBJECT_AI_PSYCH_STABILITY,
    SUBJECT_AI_RANK_PREFIX,
    SUBJECT_AI_REGIME_CHANGED,
    SUBJECT_MEM_PREV_DAY_PREFIX,
    shadow_flag_key,
)

__all__ = [
    # config
    "DEFAULT_SHADOW_FLAG_NAMESPACE",
    "DEFAULT_SHADOW_PERSISTENCE_BUFFER",
    "DEFAULT_SHADOW_POLL_INTERVAL_S",
    "ShadowModeConfig",
    # engine
    "ShadowModeService",
    "chain_filters",
    # errors
    "ShadowConfigError",
    "ShadowEngineError",
    "ShadowFlagSourceError",
    "ShadowPersistenceError",
    # filter
    "ShadowFilter",
    "is_payload_shadowed",
    "passes_ui_filter",
    # flag_source
    "InMemoryShadowFlagSource",
    "RedisShadowFlagSource",
    "ShadowFlagSource",
    # governance_observer
    "AiGovernanceEngineObserver",
    "GovernanceObserver",
    "InMemoryGovernanceObserver",
    "NoopGovernanceObserver",
    # persistence
    "InMemoryShadowedOutputSink",
    "NoopShadowedOutputSink",
    "ShadowedOutputSink",
    "TimescaleShadowedOutputSink",
    # state
    "EMPTY_SHADOW_SNAPSHOT",
    "ShadowKind",
    "ShadowSnapshot",
    "ShadowedOutput",
    # subjects
    "SHADOW_FLAG_NAMESPACE",
    "SUBJECT_AI_JOURNAL_ENTRY",
    "SUBJECT_AI_NEWS_IMPACT_PREFIX",
    "SUBJECT_AI_PRIORITY_CHANGED_PREFIX",
    "SUBJECT_AI_PSYCH_STABILITY",
    "SUBJECT_AI_RANK_PREFIX",
    "SUBJECT_AI_REGIME_CHANGED",
    "SUBJECT_MEM_PREV_DAY_PREFIX",
    "shadow_flag_key",
]
