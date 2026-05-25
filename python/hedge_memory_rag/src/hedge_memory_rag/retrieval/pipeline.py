"""Public entry point — :class:`RetrievalPipeline` (R19.5, R19.7, task 34.1).

Composes the five stages via ``await``::

    trader_event_lookup → memory_retrieval (Qdrant kNN ⊕ Timescale window)
                       → context_assembly → ollama_reasoning
                       → recommendation_generation

The pipeline is the **only** public surface of the
:mod:`hedge_memory_rag.retrieval` subpackage. It is reachable from the
Warm_AI_Pipeline only — see the package ``README.md`` for the
machine-readable invariant. The class deliberately does not register
any NATS subscriber: it is invoked directly by Warm_AI_Pipeline
services (``ai.*`` / ``mem.*`` / ``trader.*`` request handlers) that
already enforce subject-level reachability.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Final

import structlog

from .config import RetrievalSettings, load_retrieval_settings
from .context_assembly import context_assembly
from .errors import (
    RetrievalConfigurationError,
    RetrievalError,
    RetrievalTimeoutError,
)
from .memory_retrieval import memory_retrieval
from .ollama_reasoning import ollama_reasoning
from .recommendation import Recommendation
from .recommendation_generation import recommendation_generation
from .records import (
    AssembledContext,
    EventContext,
    MemoryHits,
    RetrievalRequest,
    StreamedReasoning,
)
from .trader_event_lookup import trader_event_lookup

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_warm_ai.ollama_client import OllamaClient

    from ..qdrant.store import MemoryRagQdrant
    from ..redis_cache.cache import RedisHotCache
    from ..timescale.readers import TimescaleReader

_LOG: Final = structlog.get_logger(__name__)


class RetrievalPipeline:
    """Five-stage retrieval pipeline orchestrator.

    Construct once per service with shared dependencies::

        pipeline = RetrievalPipeline(
            settings=RetrievalSettings.load(),
            qdrant=qdrant_store,
            timescale=timescale_reader,
            redis=redis_cache,
            ollama=ollama_client,
        )

    Then call :meth:`run` per trader-event reasoning request. The
    pipeline is **async-safe** — multiple coroutines may call
    :meth:`run` concurrently.

    Args:
        ollama: Connected :class:`hedge_warm_ai.ollama_client.OllamaClient`.
            Required — Stage 4 has no sensible "skip" path.
        qdrant: Connected :class:`hedge_memory_rag.qdrant.MemoryRagQdrant`,
            or ``None`` to skip the kNN step. The pipeline still
            functions in the latter case but degrades to pure
            time-window memory.
        timescale: Connected :class:`hedge_memory_rag.timescale.TimescaleReader`,
            or ``None`` to skip the time-window step.
        redis: Connected :class:`hedge_memory_rag.redis_cache.RedisHotCache`,
            or ``None`` to skip the hot-cache lookup. A miss is never
            fatal in any case.
        settings: Resolved :class:`RetrievalSettings`. Defaults to
            :func:`load_retrieval_settings` so production callers do
            not need to construct one explicitly.
    """

    def __init__(
        self,
        *,
        ollama: "OllamaClient",
        qdrant: "MemoryRagQdrant | None" = None,
        timescale: "TimescaleReader | None" = None,
        redis: "RedisHotCache | None" = None,
        settings: RetrievalSettings | None = None,
    ) -> None:
        if ollama is None:
            raise RetrievalConfigurationError(
                "RetrievalPipeline requires an OllamaClient — Stage 4 cannot be skipped"
            )
        self._ollama = ollama
        self._qdrant = qdrant
        self._timescale = timescale
        self._redis = redis
        self._settings = settings or load_retrieval_settings()

    # -----------------------------------------------------------------
    # Introspection ----------------------------------------------------
    # -----------------------------------------------------------------

    @property
    def settings(self) -> RetrievalSettings:
        """Read-only view of the resolved settings."""
        return self._settings

    # -----------------------------------------------------------------
    # Public entry point ----------------------------------------------
    # -----------------------------------------------------------------

    async def run(self, request: RetrievalRequest) -> Recommendation:
        """Execute the five-stage pipeline for one ``request``.

        Args:
            request: One trader-event reasoning request.

        Raises:
            RetrievalTimeoutError: the pipeline exceeded
                :attr:`RetrievalSettings.request_timeout_s`.
            OllamaReasoningFailedError: every model in the Ollama
                fallback chain failed.
            RecommendationParseError: the streamed text did not
                produce a valid :class:`Recommendation`.

        Returns:
            The typed :class:`Recommendation`.
        """
        if not isinstance(request, RetrievalRequest):  # pragma: no cover - defensive
            raise RetrievalConfigurationError(
                f"RetrievalPipeline.run expects RetrievalRequest, got {type(request).__name__}"
            )

        try:
            return await asyncio.wait_for(
                self._run_inner(request),
                timeout=self._settings.request_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise RetrievalTimeoutError(
                (
                    f"retrieval pipeline exceeded "
                    f"{self._settings.request_timeout_s:.1f}s budget"
                ),
                correlation_id=request.correlation_id,
            ) from exc

    # -----------------------------------------------------------------
    # Sub-stages (also exposed for testing) ---------------------------
    # -----------------------------------------------------------------

    async def stage_trader_event_lookup(
        self, request: RetrievalRequest
    ) -> EventContext:
        """Run Stage 1 in isolation."""
        return await trader_event_lookup(
            request,
            redis=self._redis,
            settings=self._settings,
        )

    async def stage_memory_retrieval(self, event: EventContext) -> MemoryHits:
        """Run Stage 2 in isolation."""
        return await memory_retrieval(
            event,
            qdrant=self._qdrant,
            timescale=self._timescale,
            settings=self._settings,
        )

    def stage_context_assembly(self, memory: MemoryHits) -> AssembledContext:
        """Run Stage 3 in isolation. Synchronous — no I/O."""
        return context_assembly(memory, settings=self._settings)

    async def stage_ollama_reasoning(
        self, context: AssembledContext
    ) -> StreamedReasoning:
        """Run Stage 4 in isolation."""
        return await ollama_reasoning(
            context,
            ollama=self._ollama,
            settings=self._settings,
        )

    def stage_recommendation_generation(
        self, reasoning: StreamedReasoning
    ) -> Recommendation:
        """Run Stage 5 in isolation. Synchronous — no I/O."""
        return recommendation_generation(reasoning)

    # -----------------------------------------------------------------
    # Internal --------------------------------------------------------
    # -----------------------------------------------------------------

    async def _run_inner(self, request: RetrievalRequest) -> Recommendation:
        correlation_id = request.correlation_id
        _LOG.debug(
            "retrieval.start",
            correlation_id=correlation_id,
            kind=request.event.kind,
            symbol=request.event.symbol,
        )

        event = await self.stage_trader_event_lookup(request)
        memory = await self.stage_memory_retrieval(event)
        context = self.stage_context_assembly(memory)
        reasoning = await self.stage_ollama_reasoning(context)
        recommendation = self.stage_recommendation_generation(reasoning)

        _LOG.info(
            "retrieval.done",
            correlation_id=correlation_id,
            action=recommendation.action,
            symbol=recommendation.symbol,
            role=recommendation.role,
            model=recommendation.model,
            confidence=recommendation.confidence,
            vector_hits=len(memory.vector_hits),
            timescale_tables=list(memory.timescale_rows.keys()),
        )
        return recommendation


__all__ = ["RetrievalPipeline", "RetrievalError"]
