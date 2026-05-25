"""Stage 2 of the retrieval pipeline — memory_retrieval (R19.5).

Two parallel lookups are merged here:

* **Qdrant kNN** against every collection in
  :attr:`RetrievalSettings.qdrant_collections`. Each search runs
  concurrently via :func:`asyncio.gather`; failures on one collection
  do not poison the others — the failed collection contributes zero
  hits and a structured-log warning.
* **Timescale window** against every hypertable in
  :attr:`RetrievalSettings.timescale_tables`. The window is anchored
  at the trader-event timestamp:
  ``[event.ts - window_minutes, event.ts)``. Each table read also
  runs concurrently and is also failure-tolerant.

The merged :class:`MemoryHits` is the input to Stage 3 (deterministic
prompt assembly).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Sequence

import structlog

from ..qdrant.collections import CollectionName
from ..qdrant.records import KnnHit
from .config import RetrievalSettings
from .records import EventContext, MemoryHits

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..qdrant.store import MemoryRagQdrant
    from ..timescale.readers import TimescaleReader

_LOG = structlog.get_logger(__name__)


async def memory_retrieval(
    event: EventContext,
    *,
    qdrant: "MemoryRagQdrant | None",
    timescale: "TimescaleReader | None",
    settings: RetrievalSettings,
) -> MemoryHits:
    """Run Stage 2: kNN + Timescale window in parallel.

    Args:
        event: Output of Stage 1.
        qdrant: Connected Qdrant store, or ``None`` to skip the kNN
            step entirely. The retrieval pipeline always uses it in
            production; the skip path exists for tests and degraded
            deployments.
        timescale: Connected Timescale reader, or ``None`` to skip
            the time-window step.
        settings: Resolved settings (``k``, ``window_minutes``,
            collection list, table list).

    Returns:
        :class:`MemoryHits` with whichever hits / rows were retrieved.
    """
    # Build the two coroutine groups.
    vector_task = _run_vector_lookups(
        event=event,
        qdrant=qdrant,
        settings=settings,
    )
    timescale_task = _run_timescale_lookups(
        event=event,
        timescale=timescale,
        settings=settings,
    )
    vector_hits, timescale_rows = await asyncio.gather(vector_task, timescale_task)

    return MemoryHits.from_results(
        event=event,
        vector_hits_by_collection=vector_hits,
        timescale_rows_by_table=timescale_rows,
    )


async def _run_vector_lookups(
    *,
    event: EventContext,
    qdrant: "MemoryRagQdrant | None",
    settings: RetrievalSettings,
) -> dict[CollectionName, Sequence[KnnHit]]:
    if qdrant is None or not settings.qdrant_collections:
        return {}

    request = event.request

    async def _one(collection: CollectionName) -> tuple[CollectionName, Sequence[KnnHit]]:
        try:
            hits = await qdrant.knn_search(
                collection,
                query_vector=request.query_vector,
                k=settings.k,
                with_payload=True,
                with_vector=False,
            )
        except Exception as exc:  # noqa: BLE001 — collection failure is non-fatal
            _LOG.warning(
                "memory_retrieval.knn_failed",
                correlation_id=request.correlation_id,
                collection=collection.value,
                error=str(exc),
            )
            return collection, ()
        return collection, tuple(hits)

    pairs = await asyncio.gather(*[_one(c) for c in settings.qdrant_collections])
    return {collection: hits for collection, hits in pairs}


async def _run_timescale_lookups(
    *,
    event: EventContext,
    timescale: "TimescaleReader | None",
    settings: RetrievalSettings,
) -> dict[str, Sequence[object]]:
    if timescale is None or not settings.timescale_tables:
        return {}

    request = event.request
    end_ts = request.event.ts
    start_ts = end_ts - timedelta(minutes=settings.window_minutes)

    async def _one(table: str) -> tuple[str, Sequence[object]]:
        try:
            rows = await timescale.read_window_any(
                table,
                start_ts,
                end_ts,
            )
        except Exception as exc:  # noqa: BLE001 — table failure is non-fatal
            _LOG.warning(
                "memory_retrieval.timescale_failed",
                correlation_id=request.correlation_id,
                table=table,
                error=str(exc),
            )
            return table, ()
        return table, tuple(rows)

    pairs = await asyncio.gather(*[_one(t) for t in settings.timescale_tables])
    return {table: rows for table, rows in pairs}


__all__ = ["memory_retrieval"]
