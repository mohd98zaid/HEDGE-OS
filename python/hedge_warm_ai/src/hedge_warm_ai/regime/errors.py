"""Exception hierarchy for :mod:`hedge_warm_ai.regime`.

A dedicated hierarchy lets the Self_Healing_Supervisor (task 41.1) and
operators distinguish *classification* failures (recoverable —
classifier-internal) from *publication* failures (not recoverable
locally — NATS or WarmCache is degraded). The Risk_Engine reads the
`MarketStability` factor through the WarmCache last-known-value path,
so a persistent publication failure must surface a typed error rather
than be silently swallowed.

Class layout::

    RegimeEngineError                    <- base, never raised directly
    ├── RegimeClassificationError        <- classifier rejected its input
    ├── RegimeConfigError                <- thresholds invalid / cyclic
    ├── RegimePublishError               <- NATS publish failed
    └── MarketStabilityCacheError        <- WarmCache write/read failed
"""

from __future__ import annotations


class RegimeEngineError(Exception):
    """Base class for every error raised by :mod:`hedge_warm_ai.regime`."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RegimeClassificationError(RegimeEngineError):
    """Raised when the classifier rejects an input observation.

    Distinct from :class:`RegimeConfigError` (config invalid) because
    it identifies a *runtime* anomaly — typically a NaN or an out-of-
    range observation — rather than a permanent configuration bug.
    """


class RegimeConfigError(RegimeEngineError):
    """Raised when :class:`RegimeConfig` thresholds are inconsistent.

    Cross-field invariants (e.g. ``panic_drawdown < 0`` or
    ``trending_trend_strength > 1.0``) raise this on
    :class:`RegimeConfig` construction so the engine fails closed at
    startup rather than silently misclassifying.
    """


class RegimePublishError(RegimeEngineError):
    """Raised when the regime publisher fails to emit ``ai.regime.changed``.

    Surfaced to the Self_Healing_Supervisor so it can detect a degraded
    NATS connection and trigger a reconnect.
    """


class MarketStabilityCacheError(RegimeEngineError):
    """Raised when the ``MarketStability`` factor cannot be written or read.

    The Risk_Engine consumes this factor via the WarmCache; persistent
    failure means the Risk_Engine falls back to its last-known-value
    until the cache recovers (R5.13).
    """


__all__ = [
    "MarketStabilityCacheError",
    "RegimeClassificationError",
    "RegimeConfigError",
    "RegimeEngineError",
    "RegimePublishError",
]
