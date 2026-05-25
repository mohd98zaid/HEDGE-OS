"""Exception hierarchy for :mod:`hedge_warm_ai.ranking` (R17).

A dedicated hierarchy lets the Self_Healing_Supervisor (task 41.1) and
operators distinguish *factor-resolution* failures (recoverable —
WarmCache or sibling-engine output is stale or missing) from
*publication* failures (NATS degraded) and *cache* failures (interim
Redis WarmCache surface degraded). The Risk_Engine consumes
``ai.rank.<correlation_id>`` via the WarmCache last-known-value path
(R5.13, R17.4), so a persistent cache-write failure must surface a
typed error rather than be silently swallowed.

Class layout::

    RankingEngineError                 <- base, never raised directly
    ├── RankingConfigError             <- config invalid / staleness window <= 0
    ├── RankingFactorError             <- factor lookup malformed / out of range
    ├── RankingPublishError            <- NATS publish failed
    └── RankingCacheError              <- WarmCache interim Redis path failed
"""

from __future__ import annotations


class RankingEngineError(Exception):
    """Base class for every error raised by :mod:`hedge_warm_ai.ranking`."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class RankingConfigError(RankingEngineError):
    """Raised when :class:`RankingConfig` values are inconsistent.

    The cross-field invariants (positive TTL, non-empty cache
    namespace, sane staleness window) raise this on construction so
    the engine fails closed at startup rather than silently
    misclassifying inputs at runtime.
    """


class RankingFactorError(RankingEngineError):
    """Raised when the :class:`FactorProvider` returns a malformed value.

    A ``RankingFactorError`` indicates a runtime anomaly (e.g. a
    cache entry shaped wrong, a sibling-engine emission whose value
    sits outside ``[0.0, 1.0]``). The engine treats it as recoverable
    by falling back to the configured default factor value for that
    factor and continuing — but it surfaces the anomaly to the
    supervisor via structlog so the upstream producer can be fixed.
    """


class RankingPublishError(RankingEngineError):
    """Raised when the publisher fails to emit ``ai.rank.<correlation_id>``.

    Surfaced to the Self_Healing_Supervisor so it can detect a
    degraded NATS connection and trigger a reconnect.
    """


class RankingCacheError(RankingEngineError):
    """Raised when the interim WarmCache (Redis) write or read fails.

    The Risk_Engine consumes the latest rank via the WarmCache
    last-known-value path (R17.4); persistent failure means the
    Risk_Engine falls back to ``Signal_v1.confidence`` until the
    cache recovers (design § Components § AI_Trade_Ranking_Engine,
    "the original Signal_Engine `confidence` is used as fallback if
    the cache entry is stale").
    """


__all__ = [
    "RankingCacheError",
    "RankingConfigError",
    "RankingEngineError",
    "RankingFactorError",
    "RankingPublishError",
]
