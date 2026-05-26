"""Exception hierarchy for :mod:`hedge_warm_ai.shadow` (R23.1, R23.2, R23.3).

A dedicated hierarchy lets the Self_Healing_Supervisor (task 41.1) and
operators distinguish *configuration* errors (fail-closed at startup)
from *cache* errors (interim WarmCache shadow-flag surface degraded)
and *persistence* errors (TimescaleDB degraded). The shape mirrors
:mod:`hedge_warm_ai.governance.errors` so consumers can pattern-match
on either subsystem with one ``except`` clause per category.

Class layout::

    ShadowEngineError                      <- base, never raised directly
    ├── ShadowConfigError                  <- thresholds / interval invariant violated
    ├── ShadowFlagSourceError              <- interim WarmCache (Redis) failed
    └── ShadowPersistenceError             <- Timescale write failed
"""

from __future__ import annotations


class ShadowEngineError(Exception):
    """Base class for every error raised by :mod:`hedge_warm_ai.shadow`."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ShadowConfigError(ShadowEngineError):
    """Raised when the resolved :class:`ShadowModeConfig` is inconsistent.

    Cross-field invariants (positive poll interval, positive
    persistence buffer, non-empty component set) raise this on
    construction so the service fails closed at startup rather than
    silently misclassifying inputs at runtime.
    """


class ShadowFlagSourceError(ShadowEngineError):
    """Raised when the interim WarmCache shadow-flag surface fails.

    The AI_Governance_Engine writes flags to
    ``hedge.warm.shadow.<component>`` (task 28.1). The shadow service
    consumes them; persistent failure means the upstream
    Warm_AI_Pipeline engines fall back to "not shadowed" until the
    cache recovers — i.e. the design's documented fail-open behaviour
    on a degraded shadow lane. The typed exception surfaces the
    failure to the supervisor for follow-up.
    """


class ShadowPersistenceError(ShadowEngineError):
    """Raised when the shadowed-output Timescale write fails.

    The service treats persistence as best-effort — a single failed
    write does not abort the publish or governance-forwarding path —
    but the typed error surfaces to the Self_Healing_Supervisor so
    operators can detect a degraded ``ai_scores`` /
    ``regime_history`` / sibling hypertable.
    """


__all__ = [
    "ShadowConfigError",
    "ShadowEngineError",
    "ShadowFlagSourceError",
    "ShadowPersistenceError",
]
