"""Persistence sinks for the AI_Trade_Journal_Engine (R18.2).

Two sinks live here, decoupled behind small Protocol types so the
engine never imports ``hedge_memory_rag`` at module-import time and
unit tests can substitute fakes:

* :class:`JournalRowSink` — writes one
  :class:`hedge_memory_rag.timescale.JournalEntry` row to the
  ``journal_entries`` hypertable. The production binding wraps
  :class:`hedge_memory_rag.timescale.TimescaleWriter.write_journal_entry`.
* :class:`JournalEmbeddingSink` — embeds the narrative via
  :class:`hedge_warm_ai.onnx_runtime.DistilBERTEmbedding` and upserts
  the resulting vector into the
  :data:`hedge_memory_rag.qdrant.CollectionName.JOURNAL_ENTRIES`
  Qdrant collection. The production binding wraps
  :class:`hedge_memory_rag.qdrant.MemoryRagQdrant.upsert`.

Both sinks are async and fail-soft: if one of them raises, the engine
logs at ``warning`` level and proceeds with the rest of the persistence
+ emission steps so a single down dependency cannot cause the journal
to drop the entry. The retrieval pipeline (task 34.x) reads from both
stores; if one is degraded the other still backs read traffic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Final, Protocol

import structlog

from ..schemas import AiJournalEntry

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_memory_rag.qdrant import MemoryRagQdrant
    from hedge_memory_rag.timescale import (
        JournalEntry as JournalEntryRow,
        TimescaleWriter,
    )

    from ..onnx_runtime import DistilBERTEmbedding

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _entry_ts(event: AiJournalEntry) -> datetime:
    """Convert ``ts_ns`` (uint64 nanoseconds) to a UTC :class:`datetime`."""
    return datetime.fromtimestamp(event.ts_ns / 1_000_000_000, tz=timezone.utc)


def journal_point_id(event: AiJournalEntry) -> str:
    """Return the stable Qdrant point id for ``event``.

    The id is the concatenation of ``trade_id`` and ``correlation_id``
    joined by a colon. This guarantees:

    * One Qdrant point per closed trade, no matter how many times the
      engine processes the same ``exec.trade.closed`` (idempotent
      upsert satisfies Property 5 — Persistence Round-Trip).
    * The id remains < 128 chars (Qdrant's max length for string ids
      of this kind) given the schema bounds (32 + 1 + 64 = 97).
    """
    return f"{event.trade_id}:{event.correlation_id}"


def journal_entry_to_row(event: AiJournalEntry) -> "JournalEntryRow":
    """Project an :class:`AiJournalEntry` into the persisted row shape.

    Imports :class:`hedge_memory_rag.timescale.JournalEntry` lazily so
    the journal subpackage stays importable in environments where
    ``hedge_memory_rag`` is not installed (rare, but matches the
    pattern used in :mod:`hedge_warm_ai.psychology.engine`).
    """
    # Local import: see module docstring rationale.
    from hedge_memory_rag.timescale import JournalEntry as _JournalEntryRow

    return _JournalEntryRow(
        ts=_entry_ts(event),
        correlation_id=event.correlation_id,
        trade_id=event.trade_id,
        symbol=event.symbol,
        side=event.side,
        quantity=event.quantity,
        entry_paise=event.entry_paise,
        exit_paise=event.exit_paise,
        pnl_inr=event.pnl_inr,
        narrative=event.narrative,
    )


# ---------------------------------------------------------------------------
# Sinks ---------------------------------------------------------------------
# ---------------------------------------------------------------------------


class JournalRowSink(Protocol):
    """Persists one :class:`AiJournalEntry` to TimescaleDB."""

    async def write_journal_row(self, event: AiJournalEntry) -> None: ...


class JournalEmbeddingSink(Protocol):
    """Persists one :class:`AiJournalEntry` embedding to Qdrant."""

    async def upsert_journal_embedding(self, event: AiJournalEntry) -> None: ...


# ---------------------------------------------------------------------------
# Default no-op sinks -------------------------------------------------------
# ---------------------------------------------------------------------------


class NoopJournalRowSink:
    """Drop-in stub used when Timescale is not wired yet."""

    async def write_journal_row(self, event: AiJournalEntry) -> None:  # noqa: D401
        return


class NoopJournalEmbeddingSink:
    """Drop-in stub used when Qdrant or the embedder is not wired yet."""

    async def upsert_journal_embedding(self, event: AiJournalEntry) -> None:  # noqa: D401
        return


# ---------------------------------------------------------------------------
# Production bindings -------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class TimescaleJournalRowSink:
    """Wraps :class:`TimescaleWriter.write_journal_entry`.

    Failures are logged + swallowed so a transient Timescale outage
    cannot drop the entry from the rest of the persistence pipeline.
    """

    writer: "TimescaleWriter"

    async def write_journal_row(self, event: AiJournalEntry) -> None:
        row = journal_entry_to_row(event)
        try:
            await self.writer.write_journal_entry(row)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "journal_timescale_write_failed",
                trade_id=event.trade_id,
                correlation_id=event.correlation_id,
                error=str(exc),
            )


#: Embedder callable: ``async def embed(text: str) -> list[float]``.
#: Use a Protocol-flat alias so callers can pass either an
#: :class:`hedge_warm_ai.onnx_runtime.DistilBERTEmbedding` (which has
#: ``async def embed(text: str) -> np.ndarray``) wrapped in a thin
#: adapter, or a custom callable for tests.
_EmbeddingCallable = Callable[[str], Awaitable[Any]]


@dataclass
class QdrantJournalEmbeddingSink:
    """Embeds the narrative via DistilBERT and upserts to Qdrant.

    Construction:
        store: Async :class:`MemoryRagQdrant` instance, already
            ``start()``-ed. The sink does not own its lifecycle; the
            owning service (``hedge-journal``) starts/stops it.
        embedder: Async callable producing the narrative embedding.
            For production, wrap
            :class:`hedge_warm_ai.onnx_runtime.DistilBERTEmbedding.embed`
            via :class:`DistilBERTEmbeddingAdapter` so the return
            type is a plain ``Sequence[float]``.
        collection: :data:`CollectionName.JOURNAL_ENTRIES`. Override
            for testing.
        wait: Whether to await Qdrant's confirmation (default
            ``True`` so the round-trip property test sees the row
            immediately).
    """

    store: "MemoryRagQdrant"
    embedder: _EmbeddingCallable
    # Default value is filled in lazily; declared as Any to avoid
    # importing ``hedge_memory_rag.qdrant`` at module-import time.
    collection: Any = None
    wait: bool = True

    async def upsert_journal_embedding(self, event: AiJournalEntry) -> None:
        from hedge_memory_rag.qdrant import CollectionName, VectorRecord

        target_collection = self.collection or CollectionName.JOURNAL_ENTRIES
        try:
            raw_vector = await self.embedder(event.narrative)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "journal_embedding_compute_failed",
                trade_id=event.trade_id,
                correlation_id=event.correlation_id,
                error=str(exc),
            )
            return

        # Coerce numpy / sequence into list[float]; the Qdrant
        # ``upsert`` already accepts both, but normalising up-front
        # keeps the wire format predictable in tests.
        try:
            vector = [float(x) for x in raw_vector]
        except TypeError:
            # ``raw_vector`` is a numpy ndarray with ndim > 1.
            try:
                import numpy as np

                arr = np.asarray(raw_vector).astype(float)
                if arr.ndim != 1:
                    arr = arr.reshape(-1)
                vector = arr.tolist()
            except Exception as exc:  # pragma: no cover
                _LOG.warning(
                    "journal_embedding_coerce_failed",
                    trade_id=event.trade_id,
                    error=str(exc),
                )
                return

        if not vector:
            _LOG.warning(
                "journal_embedding_empty",
                trade_id=event.trade_id,
                correlation_id=event.correlation_id,
            )
            return

        record = VectorRecord(
            point_id=journal_point_id(event),
            vector=vector,
            payload={
                "kind": "journal_entry",
                "correlation_id": event.correlation_id,
                "trade_id": event.trade_id,
                "symbol": event.symbol,
                "side": event.side,
                "quantity": int(event.quantity),
                "entry_paise": int(event.entry_paise),
                "exit_paise": int(event.exit_paise),
                "pnl_inr": float(event.pnl_inr),
                "narrative": event.narrative,
                "ts_ns": int(event.ts_ns),
            },
        )
        try:
            await self.store.upsert(target_collection, record, wait=self.wait)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "journal_qdrant_upsert_failed",
                trade_id=event.trade_id,
                correlation_id=event.correlation_id,
                collection=str(target_collection),
                error=str(exc),
            )


@dataclass
class DistilBERTEmbeddingAdapter:
    """Adapt :class:`DistilBERTEmbedding` to the embedder callable shape.

    The DistilBERT wrapper returns ``np.ndarray`` from
    :meth:`DistilBERTEmbedding.embed`. The Qdrant sink expects a
    plain Sequence of floats; this adapter handles the coercion
    without forcing the engine to import numpy.
    """

    embedding: "DistilBERTEmbedding"

    async def __call__(self, text: str) -> Any:
        return await self.embedding.embed(text)


__all__ = [
    "DistilBERTEmbeddingAdapter",
    "JournalEmbeddingSink",
    "JournalRowSink",
    "NoopJournalEmbeddingSink",
    "NoopJournalRowSink",
    "QdrantJournalEmbeddingSink",
    "TimescaleJournalRowSink",
    "journal_entry_to_row",
    "journal_point_id",
]
