"""Typed exception hierarchy for :mod:`hedge_memory_rag.retrieval`.

Mirrors the rationale used by :mod:`hedge_memory_rag.qdrant.errors` and
:mod:`hedge_memory_rag.redis_cache.errors`: a narrow hierarchy lets
callers distinguish *configuration* failures (caller bug, do not retry)
from *transient* failures (timeout, Ollama degraded — may retry / route
to a fallback) without parsing exception messages.

Class layout::

    RetrievalError                       <- base, never raised directly
    ├── RetrievalConfigurationError      <- bad settings / missing dependency injection
    ├── RetrievalTimeoutError            <- pipeline budget exceeded
    ├── OllamaReasoningFailedError       <- every fallback in the Ollama chain failed
    └── RecommendationParseError         <- final-stage parsing failure on the streamed JSON
"""

from __future__ import annotations

from typing import Any


class RetrievalError(Exception):
    """Base class for every error raised by the retrieval pipeline.

    Holds the *correlation_id* of the originating
    :class:`~hedge_memory_rag.retrieval.RetrievalRequest` (``None`` when
    the failure is not request-scoped) so structured-log scrapers can
    join failures to the upstream trader event without parsing the
    message.
    """

    def __init__(self, message: str, *, correlation_id: str | None = None) -> None:
        super().__init__(message)
        self.correlation_id = correlation_id

    def __reduce__(self) -> tuple[Any, ...]:  # pragma: no cover - pickling
        return (self.__class__, (str(self), self.correlation_id))


class RetrievalConfigurationError(RetrievalError):
    """Raised when :class:`RetrievalSettings` or dependency injection is malformed.

    Examples: ``k <= 0``, missing Qdrant / Timescale / Ollama injectee,
    empty Ollama role string. These are caller bugs — fail closed.
    """


class RetrievalTimeoutError(RetrievalError):
    """Raised when the overall :meth:`RetrievalPipeline.run` exceeds its budget.

    The Memory_RAG_Layer is reachable from the Warm_AI_Pipeline only
    (R19.7); a runaway pipeline call therefore never blocks the
    Hot_Path, but we still bound it so a stuck Ollama daemon does not
    pin the calling Warm_AI_Pipeline coroutine indefinitely.
    """


class OllamaReasoningFailedError(RetrievalError):
    """Raised when every model in the Ollama fallback chain has failed.

    Wraps the upstream
    :class:`hedge_warm_ai.ollama_client.OllamaAllFallbacksExhaustedError`
    so callers do not need to depend on the ``hedge_warm_ai`` exception
    surface.
    """

    def __init__(
        self,
        message: str,
        *,
        correlation_id: str | None = None,
        role: str | None = None,
    ) -> None:
        super().__init__(message, correlation_id=correlation_id)
        self.role = role


class RecommendationParseError(RetrievalError):
    """Raised when the streamed Ollama reasoning cannot be coerced into a Recommendation.

    Carries the raw text so the caller can log / persist it for
    post-mortem inspection without re-deriving it.
    """

    def __init__(
        self,
        message: str,
        *,
        correlation_id: str | None = None,
        raw_text: str = "",
    ) -> None:
        super().__init__(message, correlation_id=correlation_id)
        self.raw_text = raw_text


__all__ = [
    "OllamaReasoningFailedError",
    "RecommendationParseError",
    "RetrievalConfigurationError",
    "RetrievalError",
    "RetrievalTimeoutError",
]
