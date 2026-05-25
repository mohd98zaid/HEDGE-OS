"""Frozen value types passed between the five retrieval pipeline stages.

The pipeline is composed via ``await`` rather than sharing mutable
state, so every record below is a frozen dataclass — pickle-safe,
hashable, and trivially testable.

Stage flow::

    RetrievalRequest
        |  Stage 1 (trader_event_lookup)
        v
    EventContext
        |  Stage 2 (memory_retrieval — Qdrant kNN ⊕ Timescale window)
        v
    MemoryHits
        |  Stage 3 (context_assembly — deterministic prompt)
        v
    AssembledContext
        |  Stage 4 (ollama_reasoning — streamed inference)
        v
    StreamedReasoning
        |  Stage 5 (recommendation_generation — typed parsing)
        v
    Recommendation       (defined in :mod:`hedge_memory_rag.retrieval.recommendation`)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import numpy as np

from ..qdrant.collections import CollectionName
from ..qdrant.records import KnnHit, PointId


# ---------------------------------------------------------------------------
# Stage 1 — request + event context ----------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TraderEvent:
    """The trader event payload that triggered a reasoning request.

    Mirrors the design's *trader-event* abstraction (R19.5 — "WHEN a
    trader event occurs that triggers reasoning"). The Memory_RAG_Layer
    is intentionally agnostic about which subject produced the event —
    only ``ai.*``, ``mem.*``, and ``trader.*`` subjects can land here
    (the reachability invariant is documented in the package
    ``README.md``).

    Attributes:
        kind: Short event-kind tag (``"trader.intent.order"``,
            ``"ai.psych.intervention"``, etc.). Free-form by design;
            the prompt assembler renders it verbatim.
        symbol: Optional symbol the event refers to. ``None`` for
            account-wide events (e.g. trader_psychology intervention).
        ts: Wall-clock timestamp of the event (UTC). Anchors the
            Timescale ``[start, end)`` window in Stage 2.
        payload: Free-form JSON-compatible mapping with the rest of
            the event fields. Carried verbatim into the prompt.
    """

    kind: str
    symbol: str | None = None
    ts: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.kind:
            raise ValueError("TraderEvent.kind must be a non-empty string")


@dataclass(frozen=True, slots=True)
class RetrievalRequest:
    """Single trader-event reasoning request submitted to :meth:`RetrievalPipeline.run`.

    Attributes:
        correlation_id: Stable identifier propagated through every
            log line and (when applicable) into the persisted
            recommendation. Mirrors the Hot_Path / Warm_AI_Pipeline
            ``correlation_id`` convention.
        event: The trader event that triggered the request.
        query_vector: Pre-computed query embedding used for the kNN
            step. Length must match the configured
            :attr:`hedge_memory_rag.qdrant.QdrantSettings.vector_dim`
            for the targeted collections; mismatches raise
            :class:`hedge_memory_rag.qdrant.QdrantConfigurationError`
            inside Stage 2 rather than at request boundary, so the
            caller can swap embedders without touching this layer.
        instruction: Optional system-style instruction prepended to
            the assembled prompt. ``None`` falls back to a
            deterministic default ("Reason about the trader event
            below using the provided memories ...").
        request_id: Optional sub-id when a correlation id is shared
            across multiple sub-requests. Defaults to ``correlation_id``.
        extra: Free-form metadata persisted alongside the recommendation
            and rendered in the prompt under an "Additional Context"
            block. Useful for caller hints (e.g. "trader_emotional_state",
            "current_regime") that should not influence the kNN search
            but should reach the LLM.
    """

    correlation_id: str
    event: TraderEvent
    query_vector: Sequence[float] | np.ndarray
    instruction: str | None = None
    request_id: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.correlation_id:
            raise ValueError("RetrievalRequest.correlation_id must be non-empty")
        if self.query_vector is None:
            raise ValueError("RetrievalRequest.query_vector must not be None")


@dataclass(frozen=True, slots=True)
class EventContext:
    """Output of Stage 1 (``trader_event_lookup``).

    Carries the trader event verbatim plus best-effort hot-cache
    snapshots. None of the cache fields are required: a Redis miss is
    never fatal because the kNN + Timescale stages are the
    authoritative source of long-term memory.

    Attributes:
        request: The originating request, kept on the context so
            downstream stages do not need to thread it separately.
        recent_trades: Most-recent trades for ``request.event.symbol``
            from the Redis hot cache. Empty list when the symbol is
            ``None`` or the cache is empty.
        recent_news: Most-recent news items for ``request.event.symbol``.
            Empty list when the symbol is ``None``.
        current_regime: Last-known regime label (e.g. ``"Trending"``)
            from the hot cache, or ``None`` when missing.
        current_stability_score: Last-known Trader_Stability_Score
            from the hot cache, or ``None`` when missing.
    """

    request: RetrievalRequest
    recent_trades: tuple[Any, ...] = ()
    recent_news: tuple[Any, ...] = ()
    current_regime: Any | None = None
    current_stability_score: Any | None = None


# ---------------------------------------------------------------------------
# Stage 2 — memory_retrieval -----------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QdrantHitView:
    """One Qdrant hit annotated with the collection it came from.

    The standard :class:`hedge_memory_rag.qdrant.records.KnnHit` does
    not carry the source collection; we attach it here so the prompt
    assembler can render hits per-collection without a separate lookup.
    """

    collection: CollectionName
    point_id: PointId
    score: float
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MemoryHits:
    """Output of Stage 2 (``memory_retrieval``).

    Two parallel lookups are merged here:

    * ``vector_hits`` — concatenated kNN results from every configured
      Qdrant collection, sorted by descending score within their
      original collection group. The assembler renders them grouped
      by collection.
    * ``timescale_rows`` — one entry per configured hypertable, each
      mapping to the typed list returned by
      :meth:`hedge_memory_rag.timescale.TimescaleReader.read_window_any`.
      Empty when the window holds no rows.

    The dataclass is frozen so downstream stages cannot mutate the
    retrieved memory in-place — important for the property-test goal
    of "every reasoning request produces exactly one recommendation"
    (task 34.2).
    """

    event: EventContext
    vector_hits: tuple[QdrantHitView, ...] = ()
    timescale_rows: Mapping[str, tuple[Any, ...]] = field(default_factory=dict)

    @classmethod
    def from_results(
        cls,
        event: EventContext,
        *,
        vector_hits_by_collection: Mapping[CollectionName, Sequence[KnnHit]],
        timescale_rows_by_table: Mapping[str, Sequence[Any]],
    ) -> "MemoryHits":
        """Build :class:`MemoryHits` from the raw per-source results."""
        flattened: list[QdrantHitView] = []
        for collection, hits in vector_hits_by_collection.items():
            for hit in hits:
                flattened.append(
                    QdrantHitView(
                        collection=collection,
                        point_id=hit.point_id,
                        score=float(hit.score),
                        payload=dict(hit.payload),
                    )
                )
        timescale = {
            table: tuple(rows) for table, rows in timescale_rows_by_table.items()
        }
        return cls(
            event=event,
            vector_hits=tuple(flattened),
            timescale_rows=timescale,
        )


# ---------------------------------------------------------------------------
# Stage 3 — context_assembly -----------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AssembledContext:
    """Output of Stage 3 (``context_assembly``).

    Holds the deterministic prompt that will be sent to Ollama plus
    the upstream :class:`MemoryHits` for traceability. The prompt is
    a plain string — no LLM calls happen in this stage.
    """

    memory_hits: MemoryHits
    prompt: str
    instruction: str

    @property
    def correlation_id(self) -> str:
        return self.memory_hits.event.request.correlation_id


# ---------------------------------------------------------------------------
# Stage 4 — ollama_reasoning ------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StreamedReasoning:
    """Output of Stage 4 (``ollama_reasoning``).

    Holds the concatenated streamed text plus the ``role`` / ``model``
    that finally served the response (which can differ from the
    requested role when the OllamaClient fell back). The trailing
    chunk's metrics (``eval_count``, ``eval_duration``, etc.) are
    captured verbatim so the property tests can assert one
    recommendation per request.
    """

    context: AssembledContext
    role: str
    model: str
    text: str
    metrics: Mapping[str, Any] = field(default_factory=dict)

    @property
    def correlation_id(self) -> str:
        return self.context.correlation_id


__all__ = [
    "AssembledContext",
    "EventContext",
    "MemoryHits",
    "QdrantHitView",
    "RetrievalRequest",
    "StreamedReasoning",
    "TraderEvent",
]
