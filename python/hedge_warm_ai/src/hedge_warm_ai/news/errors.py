"""Exception hierarchy for :mod:`hedge_warm_ai.news` (R12).

A dedicated hierarchy lets the Self_Healing_Supervisor (task 41.1) and
operators distinguish *ingestion* failures (recoverable — source
adapter degraded) from *publication* failures (NATS degraded) and
*persistence* failures (Qdrant degraded). The Risk_Engine and
Signal_Engine consume ``ai.news.impact.<sym>`` directly and the
Memory_RAG_Layer pulls headline embeddings from Qdrant — a persistent
failure on either path must surface a typed error rather than be
silently swallowed.

Class layout::

    NewsEngineError                  <- base, never raised directly
    ├── NewsConfigError              <- thresholds invalid / unknown role
    ├── NewsIngestionError           <- source adapter rejected its input
    ├── NewsPublishError             <- NATS publish failed
    └── NewsQdrantError              <- Qdrant upsert failed
"""

from __future__ import annotations


class NewsEngineError(Exception):
    """Base class for every error raised by :mod:`hedge_warm_ai.news`."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class NewsConfigError(NewsEngineError):
    """Raised when :class:`NewsConfig` values are inconsistent.

    Cross-field invariants (e.g. a slow-path Ollama role that is not
    in the configured registry, or a dedup window of zero) raise this
    on :class:`NewsConfig` construction so the engine fails closed at
    startup rather than silently degrading.
    """


class NewsIngestionError(NewsEngineError):
    """Raised when a source adapter rejects an inbound headline.

    Distinct from :class:`NewsConfigError` (config invalid) because it
    identifies a *runtime* anomaly — typically a malformed feed
    payload — rather than a permanent configuration bug.
    """


class NewsPublishError(NewsEngineError):
    """Raised when the NATS publisher fails to emit ``ai.news.impact.<sym>``.

    Surfaced to the Self_Healing_Supervisor so it can detect a
    degraded NATS connection and trigger a reconnect.
    """


class NewsQdrantError(NewsEngineError):
    """Raised when the Qdrant ``news`` collection cannot be written.

    The Memory_RAG_Layer retrieval pipeline depends on this collection
    (task 34.x); persistent failure means the slow-path reasoning step
    will fall back to keyword-only retrieval until the cache recovers.
    """


__all__ = [
    "NewsConfigError",
    "NewsEngineError",
    "NewsIngestionError",
    "NewsPublishError",
    "NewsQdrantError",
]
