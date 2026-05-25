"""Exception hierarchy for :mod:`hedge_warm_ai.governance` (R23, R24).

A dedicated hierarchy lets the Self_Healing_Supervisor (task 41.1) and
operators distinguish *configuration* errors (fail-closed at startup)
from *publication* errors (NATS degraded), *cache* errors (interim
WarmCache surface degraded), and *persistence* errors (TimescaleDB
degraded).

Class layout::

    GovernanceEngineError                <- base, never raised directly
    ├── GovernanceConfigError            <- thresholds invariant violated
    ├── GovernancePublishError           <- NATS publish failed
    ├── GovernanceCacheError             <- interim WarmCache failed
    └── GovernancePersistenceError       <- Timescale write failed
"""

from __future__ import annotations


class GovernanceEngineError(Exception):
    """Base class for every error raised by :mod:`hedge_warm_ai.governance`."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class GovernanceConfigError(GovernanceEngineError):
    """Raised when the resolved governance configuration is inconsistent.

    The cross-field invariants (``degradation < critical`` for
    every metric, non-empty rolling-window sizes, sane component
    set) raise this on construction so the engine fails closed at
    startup rather than silently misclassifying inputs at runtime.
    """


class GovernancePublishError(GovernanceEngineError):
    """Raised when the publisher fails to emit ``ai.gov.action``.

    Surfaced to the Self_Healing_Supervisor so it can detect a
    degraded NATS connection and trigger a reconnect.
    """


class GovernanceCacheError(GovernanceEngineError):
    """Raised when the interim WarmCache (Redis) write or read fails.

    The Risk_Engine and AI_Trade_Ranking_Engine consume the
    per-component ``governance_weight`` multiplier through the
    WarmCache last-known-value path (R24.2). Persistent failure
    means downstream consumers fall back to the design's documented
    defaults until the cache recovers.
    """


class GovernancePersistenceError(GovernanceEngineError):
    """Raised when the ``governance_metrics`` Timescale write fails.

    The engine treats persistence as best-effort — a single failed
    write does not abort the publish or cache path — but the typed
    error surfaces to the Self_Healing_Supervisor for follow-up.
    """


__all__ = [
    "GovernanceCacheError",
    "GovernanceConfigError",
    "GovernanceEngineError",
    "GovernancePersistenceError",
    "GovernancePublishError",
]
