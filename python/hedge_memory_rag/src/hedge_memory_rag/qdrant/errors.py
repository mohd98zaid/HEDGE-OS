"""Exception hierarchy for :mod:`hedge_memory_rag.qdrant`.

Mirrors the rationale used in :mod:`hedge_warm_ai.ollama_client.errors`:
a narrow, typed hierarchy lets call sites distinguish *configuration*
failures (caller bug, do not retry) from *connection* failures
(transient, may retry / failover) without parsing exception messages.

Class layout::

    QdrantClientError                       <- base, never raised directly
    ├── QdrantConfigurationError            <- bad settings, vector dim, distance
    ├── QdrantConnectionError               <- TCP / gRPC / HTTP layer fault
    └── CollectionDimensionMismatchError    <- existing collection has different vector dim
"""

from __future__ import annotations

from typing import Any


class QdrantClientError(Exception):
    """Base class for every error raised by :class:`MemoryRagQdrant`.

    Holds the *collection* identifier (``None`` when the failure is
    not collection-scoped) so structured-log scrapers can filter on it
    without having to parse the message.
    """

    def __init__(self, message: str, *, collection: str | None = None) -> None:
        super().__init__(message)
        self.collection = collection

    def __reduce__(self) -> tuple[Any, ...]:  # pragma: no cover - pickling
        return (self.__class__, (str(self), self.collection))


class QdrantConfigurationError(QdrantClientError):
    """Raised when :class:`QdrantSettings` or a :class:`CollectionSpec` is malformed.

    Examples: empty host, vector dimension <= 0, unknown distance metric,
    duplicate collection name. These are caller bugs — fail closed.
    """


class QdrantConnectionError(QdrantClientError):
    """Raised when the Qdrant daemon refuses, resets, or otherwise cannot be reached.

    Wraps the upstream :class:`qdrant_client.http.exceptions.UnexpectedResponse`
    or transport-layer error so callers can implement retry / failover
    without depending on the qdrant-client exception surface.
    """


class CollectionDimensionMismatchError(QdrantClientError):
    """Raised when an existing Qdrant collection has a different vector dimension or distance.

    Provisioning is idempotent **only** when the persisted collection
    parameters match the configured :class:`CollectionSpec`. A mismatch
    is a hard failure — silently re-creating the collection would drop
    every previously persisted vector, which is forbidden.

    Carries both the *expected* and *actual* parameters so an operator
    can decide whether to migrate or rename the collection.
    """

    def __init__(
        self,
        message: str,
        *,
        collection: str,
        expected_dim: int,
        actual_dim: int,
        expected_distance: str,
        actual_distance: str,
    ) -> None:
        super().__init__(message, collection=collection)
        self.expected_dim = expected_dim
        self.actual_dim = actual_dim
        self.expected_distance = expected_distance
        self.actual_distance = actual_distance


__all__ = [
    "CollectionDimensionMismatchError",
    "QdrantClientError",
    "QdrantConfigurationError",
    "QdrantConnectionError",
]
