"""AI_Trade_Journal_Engine — task 27.1 (R18).

This is the orchestrator that ties the journal subpackage together:

1. **Subscribe** to ``exec.trade.closed`` via the configured
   :class:`JournalBusSubscriber`. The subscriber callback decodes
   the canonical :class:`TradeClosedEvent` payload and **immediately**
   spawns the slow-path narrative generation on a fresh
   :func:`asyncio.Task` so the bus reader returns within microseconds
   (Property 10 — every event delivered to every subscriber exactly
   once, even when one subscriber is slow).

2. **Generate the narrative** via the
   :class:`OllamaNarrativeBuilder`:

   * Always invokes Qwen2.5:14B (``OllamaRole.PRIMARY``) for the
     standard narrative.
   * Optionally invokes DeepSeek-R1 (``OllamaRole.DEEP``) for a
     deeper post-mortem when the trade outcome warrants it
     (default: ``pnl_inr < 0``).

3. **Persist** the resulting :class:`AiJournalEntry`:

   * Timescale row via :class:`JournalRowSink` (production binding:
     :class:`hedge_memory_rag.timescale.TimescaleWriter`).
   * Qdrant embedding via :class:`JournalEmbeddingSink` (production
     binding: :class:`hedge_memory_rag.qdrant.MemoryRagQdrant` upsert
     on the ``journal_entries`` collection, narrative embedded by
     :class:`hedge_warm_ai.onnx_runtime.DistilBERTEmbedding`).

4. **Emit** ``ai.journal.entry`` to NATS via the configured
   :class:`JournalEntryPublisher` for live consumers (UI gateway,
   AI_Governance, AI_Shadow_Mode replay analyser, retrieval
   pipeline). The emission happens **after** persistence so any
   subscriber that performs a follow-up
   :class:`MemoryRagQdrant.knn_search` or Timescale window read
   sees the new row.

5. **Serve** ``mem.journal.query`` request-reply via the configured
   :class:`JournalRequestReplyServer`. The handler hits Timescale
   for the windowed read and (optionally) Qdrant for similarity
   search.

Slow-path invariant
-------------------

The trade-closed callback **never blocks** on Ollama, Timescale, or
Qdrant. The flow is:

    bus.subscribe(exec.trade.closed) -> _on_trade_closed [returns ~immediately]
                                            \\
                                             asyncio.create_task(_process_trade)
                                                 - generate narrative (Ollama)
                                                 - write to Timescale
                                                 - upsert to Qdrant
                                                 - publish ai.journal.entry

The bus reader is therefore never delayed by an LLM call, satisfying
both the "produce a journal entry on every closed trade" requirement
(R18.1) and the spec brief's slow-path dispatch invariant.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import TracebackType
from typing import TYPE_CHECKING, Awaitable, Callable, Final, Type

import structlog

from ..schemas import AiJournalEntry
from .bus import (
    JournalBusPublisher,
    JournalBusSubscriber,
    JournalRequestReplyServer,
)
from .config import JournalConfig
from .narrative import NarrativeProvider
from .persistence import (
    JournalEmbeddingSink,
    JournalRowSink,
    NoopJournalEmbeddingSink,
    NoopJournalRowSink,
)
from .publisher import JournalEntryPublisher, NoopJournalEntryPublisher
from .query import (
    JournalQueryError,
    JournalQueryHit,
    JournalQueryReply,
    JournalQueryRequest,
    hit_from_qdrant_payload,
    row_to_entry,
)
from .state import TradeClosedEvent
from .subjects import (
    SUBJECT_AI_JOURNAL_ENTRY,
    SUBJECT_EXEC_TRADE_CLOSED,
    SUBJECT_MEM_JOURNAL_QUERY,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_memory_rag.qdrant import MemoryRagQdrant
    from hedge_memory_rag.timescale import TimescaleReader

_LOG: Final = structlog.get_logger(__name__)


def _wall_ns() -> int:
    return time.time_ns()


# ---------------------------------------------------------------------------
# Errors --------------------------------------------------------------------
# ---------------------------------------------------------------------------


class JournalEngineError(RuntimeError):
    """Raised by :class:`AiTradeJournalEngine` for engine-level failures."""


# ---------------------------------------------------------------------------
# Engine --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class AiTradeJournalEngine:
    """Owns the ``ai.journal.entry`` persistence + emission pipeline (R18).

    Construct via the keyword-only fields and call :meth:`start` /
    :meth:`aclose` to manage its lifecycle. Use as an async context
    manager for the simple case::

        async with AiTradeJournalEngine(
            config=cfg,
            subscriber=subscriber,
            publisher=publisher,
            reply_server=reply_server,
            narrative_provider=narrative,
            row_sink=row_sink,
            embedding_sink=embedding_sink,
            timescale_reader=reader,
            qdrant=qdrant,
        ) as engine:
            ...

    Fields:
        config: Resolved :class:`JournalConfig` (Ollama roles +
            embedding dim derived from :class:`HedgeConfig`).
        subscriber: Subscription registrar for ``exec.trade.closed``.
        publisher: Sink for ``ai.journal.entry`` event publications.
        reply_server: Request-reply server for ``mem.journal.query``.
        narrative_provider: Pluggable narrative + post-mortem
            generator. Production binding:
            :class:`OllamaNarrativeBuilder`. Tests substitute a
            deterministic stub.
        row_sink: Persists each :class:`AiJournalEntry` to Timescale.
            Defaults to :class:`NoopJournalRowSink` — wire
            :class:`TimescaleJournalRowSink` in production.
        embedding_sink: Embeds + upserts each entry to Qdrant.
            Defaults to :class:`NoopJournalEmbeddingSink` — wire
            :class:`QdrantJournalEmbeddingSink` in production.
        timescale_reader: Optional :class:`TimescaleReader` used by
            the query handler. ``None`` makes
            :data:`SUBJECT_MEM_JOURNAL_QUERY` reply with empty
            results.
        qdrant: Optional :class:`MemoryRagQdrant` used by the query
            handler for similarity search. ``None`` disables
            similarity search at the API level.
        similarity_query_embedder: Optional async callable
            ``async def embed(text: str) -> Sequence[float]`` used to
            embed similarity-search query strings server-side. Pass
            the same :class:`DistilBERTEmbeddingAdapter` you used for
            ingestion to keep the similarity space consistent.
            ``None`` makes the engine reject query strings (callers
            must supply ``similarity_vector`` explicitly).
        clock_ns: Override for the wall-clock nanosecond timestamp.
            Useful for deterministic tests.
    """

    config: JournalConfig
    subscriber: JournalBusSubscriber
    publisher: JournalBusPublisher
    reply_server: JournalRequestReplyServer
    narrative_provider: NarrativeProvider

    row_sink: JournalRowSink = field(default_factory=NoopJournalRowSink)
    embedding_sink: JournalEmbeddingSink = field(
        default_factory=NoopJournalEmbeddingSink
    )
    entry_publisher: JournalEntryPublisher = field(
        default_factory=NoopJournalEntryPublisher
    )

    timescale_reader: "TimescaleReader | None" = None
    qdrant: "MemoryRagQdrant | None" = None
    similarity_query_embedder: Callable[[str], Awaitable[object]] | None = None

    clock_ns: Callable[[], int] = _wall_ns

    # --- Internals --------------------------------------------------

    _query_ctx: object = field(default=None, init=False, repr=False)
    _trade_closed_ctx: object = field(default=None, init=False, repr=False)
    _processing_tasks: set[asyncio.Task[None]] = field(
        default_factory=set, init=False, repr=False
    )
    _started: bool = field(default=False, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    # ----- async-context-manager hooks ------------------------------

    async def __aenter__(self) -> "AiTradeJournalEngine":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    # ----- lifecycle ------------------------------------------------

    async def start(self) -> None:
        """Register the request-reply handler and ``exec.trade.closed`` subscription."""
        if self._started:
            return
        if self._closed:
            raise JournalEngineError("engine has been closed and cannot restart")

        # Register the request-reply server for ``mem.journal.query``.
        self._query_ctx = self.reply_server.serve(
            SUBJECT_MEM_JOURNAL_QUERY,
            self._handle_query,
        )
        await self._query_ctx.__aenter__()  # type: ignore[union-attr]

        # Subscribe to ``exec.trade.closed``. The handler MUST return
        # quickly — the slow Ollama narrative generation runs on a
        # fresh task spawned via :func:`asyncio.create_task`.
        self._trade_closed_ctx = self.subscriber.subscribe(
            SUBJECT_EXEC_TRADE_CLOSED,
            self._on_trade_closed,
        )
        await self._trade_closed_ctx.__aenter__()  # type: ignore[union-attr]

        self._started = True
        _LOG.info(
            "journal_engine_started",
            trade_closed_subject=SUBJECT_EXEC_TRADE_CLOSED,
            entry_subject=SUBJECT_AI_JOURNAL_ENTRY,
            query_subject=SUBJECT_MEM_JOURNAL_QUERY,
            narrative_role=self.config.narrative_role_key,
            postmortem_role=self.config.postmortem_role_key,
        )

    async def aclose(self) -> None:
        """Tear down subscriptions and wait for in-flight processing tasks."""
        if self._closed:
            return
        self._closed = True

        # Unwind subscriptions first so no new tasks are spawned.
        if self._trade_closed_ctx is not None:
            await self._trade_closed_ctx.__aexit__(None, None, None)  # type: ignore[union-attr]
            self._trade_closed_ctx = None
        if self._query_ctx is not None:
            await self._query_ctx.__aexit__(None, None, None)  # type: ignore[union-attr]
            self._query_ctx = None

        # Drain in-flight processing tasks. We **wait** rather than
        # cancel because cancelling mid-Ollama-stream would lose the
        # journal entry for that trade, violating R18.1 ("produce a
        # journal entry on every closed trade").
        if self._processing_tasks:
            tasks = list(self._processing_tasks)
            await asyncio.gather(*tasks, return_exceptions=True)

        self._started = False
        _LOG.info("journal_engine_closed")

    # ----- trade-closed pipeline ------------------------------------

    async def _on_trade_closed(self, payload: bytes) -> None:
        """Bus callback: decode and dispatch the slow narrative work.

        Returns within microseconds — the actual narrative + persist +
        emit pipeline runs on a fresh :class:`asyncio.Task` so the
        bus reader is never blocked. This preserves Property 10
        (subscriber delivery) and the spec brief's slow-path
        invariant.
        """
        try:
            obj = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            _LOG.warning(
                "journal_trade_closed_payload_malformed",
                error=str(exc),
            )
            return

        try:
            event = TradeClosedEvent.model_validate(obj)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "journal_trade_closed_payload_invalid",
                error=str(exc),
            )
            return

        # Spawn the processing task — never await it from here.
        task = asyncio.create_task(
            self._process_trade(event),
            name=f"journal_process:{event.trade_id}",
        )
        self._processing_tasks.add(task)
        task.add_done_callback(self._processing_tasks.discard)

    async def process_trade_event(self, event: TradeClosedEvent) -> AiJournalEntry:
        """Synchronous-ish API for tests + replay: process one event end-to-end.

        This is the same code path the bus callback's spawned task
        runs, exposed for callers that want the resulting
        :class:`AiJournalEntry` back. Production code should always
        go through the bus subscription so the task lifecycle is
        managed.
        """
        return await self._process_trade(event)

    async def _process_trade(self, event: TradeClosedEvent) -> AiJournalEntry:
        """Generate, persist, and emit one journal entry for ``event``.

        Failures in any step are logged but do not abort the others;
        the engine prefers a partially-persisted entry over a dropped
        entry (R18.1). The publication still happens so live
        consumers receive the narrative even when persistence is
        degraded.
        """
        narrative = await self.narrative_provider.build(event)
        # Schema clamps to ``maxLength: 8192``; truncate defensively.
        narrative = narrative[:8192]

        ts_ns = int(self.clock_ns())
        entry = AiJournalEntry(
            correlation_id=event.correlation_id,
            trade_id=event.trade_id,
            symbol=event.symbol,
            side=event.side,
            quantity=event.quantity,
            entry_paise=event.entry_paise,
            exit_paise=event.exit_paise,
            pnl_inr=event.pnl_inr,
            narrative=narrative,
            ts_ns=ts_ns,
        )

        # 1. Timescale persistence.
        await self.row_sink.write_journal_row(entry)

        # 2. Qdrant embedding upsert. Both sinks are fail-soft.
        await self.embedding_sink.upsert_journal_embedding(entry)

        # 3. NATS emission via both the explicit publisher and the
        #    bus surface. Most callers wire only one of them; we
        #    invoke both so a service that opts into the typed
        #    :class:`NatsJournalEntryPublisher` (for the bound
        #    schema validation) does not also have to register the
        #    same subject on the generic :class:`JournalBusPublisher`.
        await self.entry_publisher.publish_entry(entry)
        await self._publish_bus_entry(entry)

        _LOG.info(
            "journal_entry_emitted",
            trade_id=entry.trade_id,
            correlation_id=entry.correlation_id,
            symbol=entry.symbol,
            pnl_inr=entry.pnl_inr,
            narrative_len=len(entry.narrative),
        )
        return entry

    async def _publish_bus_entry(self, entry: AiJournalEntry) -> None:
        payload = json.dumps(
            entry.model_dump(mode="json"), separators=(",", ":")
        ).encode("utf-8")
        try:
            await self.publisher.publish(SUBJECT_AI_JOURNAL_ENTRY, payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "journal_bus_publish_failed",
                trade_id=entry.trade_id,
                correlation_id=entry.correlation_id,
                subject=SUBJECT_AI_JOURNAL_ENTRY,
                error=str(exc),
            )

    # ----- query handler --------------------------------------------

    async def _handle_query(self, payload: bytes) -> bytes:
        """Decode a ``mem.journal.query`` request, build the reply."""
        try:
            request = JournalQueryRequest.from_json_bytes(payload)
        except (JournalQueryError, ValueError) as exc:
            return json.dumps(
                {"error": str(exc), "ts_ns": int(self.clock_ns())},
                separators=(",", ":"),
            ).encode("utf-8")

        window_hits = await self._run_window_query(request)
        similarity_hits = await self._run_similarity_query(request)

        reply = JournalQueryReply(
            window_hits=window_hits,
            similarity_hits=similarity_hits,
            ts_ns=int(self.clock_ns()),
        )
        return reply.to_json_bytes()

    async def _run_window_query(
        self, request: JournalQueryRequest
    ) -> list[JournalQueryHit]:
        if self.timescale_reader is None:
            return []
        start_ns, end_ns = request.resolve_window(now_ns=self.clock_ns())
        start_dt = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ns / 1_000_000_000, tz=timezone.utc)
        try:
            rows = await self.timescale_reader.read_journal_entries(
                start_dt,
                end_dt,
                symbol=request.symbol,
                trade_id=request.trade_id,
                limit=request.limit,
            )
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "journal_query_window_failed",
                start=start_dt.isoformat(),
                end=end_dt.isoformat(),
                error=str(exc),
            )
            return []
        return [
            JournalQueryHit(entry=row_to_entry(row), source="window", score=None)
            for row in rows
        ]

    async def _run_similarity_query(
        self, request: JournalQueryRequest
    ) -> list[JournalQueryHit]:
        if self.qdrant is None:
            return []
        # Resolve the query vector. ``similarity_vector`` wins over
        # ``similarity_query`` when both are supplied.
        vector: list[float] | None = None
        if request.similarity_vector is not None:
            vector = list(request.similarity_vector)
        elif request.similarity_query is not None:
            if self.similarity_query_embedder is None:
                _LOG.warning(
                    "journal_query_similarity_no_embedder",
                    query=request.similarity_query[:64],
                )
                return []
            try:
                raw = await self.similarity_query_embedder(request.similarity_query)
                vector = [float(x) for x in raw]
            except Exception as exc:  # pragma: no cover - logged + dropped
                _LOG.warning(
                    "journal_query_similarity_embed_failed",
                    error=str(exc),
                )
                return []
        if not vector:
            return []

        # Lazy-import to avoid forcing every consumer of this module
        # to bring ``hedge_memory_rag`` into scope.
        from hedge_memory_rag.qdrant import CollectionName

        try:
            results = await self.qdrant.knn_search(
                CollectionName.JOURNAL_ENTRIES,
                vector,
                k=request.similarity_k,
                payload_filter=request.similarity_filter or None,
                with_payload=True,
                with_vector=False,
            )
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "journal_query_similarity_failed",
                error=str(exc),
            )
            return []

        hits: list[JournalQueryHit] = []
        for hit in results:
            converted = hit_from_qdrant_payload(hit.payload, hit.score)
            if converted is not None:
                hits.append(converted)
            else:
                _LOG.debug(
                    "journal_query_similarity_skip_unstructured",
                    point_id=str(hit.point_id),
                )
        return hits


__all__ = [
    "AiTradeJournalEngine",
    "JournalEngineError",
]
