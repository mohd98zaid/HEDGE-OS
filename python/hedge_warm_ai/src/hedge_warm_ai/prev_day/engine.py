"""Previous_Day_Memory_Engine implementation (task 24.1, R15).

Top-level orchestrator that ties together:

* :mod:`hedge_warm_ai.prev_day.bus` for the NATS surface,
* :mod:`hedge_warm_ai.prev_day.compute` for the pure builders,
* :class:`hedge_memory_rag.timescale.TimescaleWriter` /
  :class:`TimescaleReader` for the Timescale persistence,
* :class:`hedge_memory_rag.qdrant.MemoryRagQdrant` for the
  ``market_memory`` Qdrant embeddings.

Lifecycle:

1. The owning service constructs the engine with concrete
   :class:`PrevDayBusPublisher`, :class:`PrevDayBusSubscriber`, and
   :class:`PrevDayRequestReplyServer` (in production these are the
   NATS-backed adapters; in tests they are :class:`InMemoryPrevDayBus`).
2. The service awaits :meth:`PrevDayMemoryEngine.start`, which
   registers the request-reply handler for ``mem.prev_day.query`` and
   subscribes to ``ops.session.end``.
3. The engine sits idle until either a query arrives or the session
   manager publishes ``ops.session.end``.

   * On query, it loads the latest persisted row for each requested
     symbol, projects to the canonical ``PreviousDayMemory`` schema, and
     replies on the NATS reply subject. The ``mem.prev_day.query``
     subject uses NATS request-reply semantics — the client's
     ``request()`` call resolves the reply implicitly through the bus.
   * On ``ops.session.end``, the engine spawns a fresh
     :class:`asyncio.Task` (via :func:`asyncio.create_task`) so the
     subscriber callback returns immediately. The task runs the
     :class:`PrevDayComputeJob`, which:
       a. Pulls per-symbol session inputs from the Memory_RAG_Layer
          via the configured :class:`SessionInputProvider` (the
          aggregator is owned by the session manager — wired here
          through a callable so testing can mock it cleanly).
       b. Builds typed :class:`PreviousDayMemoryRow`s and upserts
          them into Timescale.
       c. Writes the per-row embedding-friendly summary into the
          Qdrant ``market_memory`` collection.
       d. Publishes the canonical schema record on
          ``mem.prev_day.<symbol_id>`` for each persisted row.
       e. Emits one ``mem.prev_day.ready`` event when the whole
          batch is durable.
4. :meth:`PrevDayMemoryEngine.aclose` cancels any in-flight compute
   tasks and unwinds the bus subscriptions.

Concurrency invariants:

* The compute job is **never awaited inside** the ``ops.session.end``
  subscriber callback. The callback returns within microseconds; the
  job runs on a separate task. This satisfies "do not block the
  subscriber callback" from the task brief.
* At most one compute job runs at a time per ``session_date``. A
  second ``ops.session.end`` arriving while the first is still
  executing is a no-op (logged at warning level).
* The job's wall-clock budget (deadline) is configurable; if it
  exceeds the deadline, it is cancelled and a warning is logged but
  ``mem.prev_day.ready`` is **not** emitted — consumers fall back to
  whatever was previously persisted.

Failure handling:

* Any persistence error during the job is logged and the job is
  aborted; partial Timescale writes remain (the rows already upserted
  are valid). The ``mem.prev_day.ready`` event is **not** emitted on
  failure so consumers do not get a false "fresh data" signal.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Final, Mapping, Protocol, Type

import structlog

from hedge_memory_rag.qdrant import (
    CollectionName,
    MemoryRagQdrant,
    VectorRecord,
)
from hedge_memory_rag.timescale.models import PreviousDayMemoryRow
from hedge_memory_rag.timescale.readers import TimescaleReader
from hedge_memory_rag.timescale.writers import TimescaleWriter

from ..schemas.mem_prev_day import PreviousDayMemory
from .bus import (
    PrevDayBusPublisher,
    PrevDayBusSubscriber,
    PrevDayRequestReplyServer,
)
from .compute import (
    PrevDaySessionInputs,
    SymbolSessionData,
    build_prev_day_event,
    build_prev_day_row,
    format_prev_day_summary,
    stable_embedding_point_id,
)
from .subjects import (
    SUBJECT_MEM_PREV_DAY_QUERY,
    SUBJECT_MEM_PREV_DAY_READY,
    SUBJECT_OPS_SESSION_END,
    mem_prev_day_subject,
)

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Errors --------------------------------------------------------------------
# ---------------------------------------------------------------------------


class PrevDayEngineError(RuntimeError):
    """Raised by :class:`PrevDayMemoryEngine` for engine-level failures."""


# ---------------------------------------------------------------------------
# DTOs for the request-reply API + ready event ------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PrevDayQueryRequest:
    """Payload of a ``mem.prev_day.query`` request.

    Either ``symbol_ids`` (numeric) or ``symbols`` (textual) — at least
    one must be non-empty. ``session_date`` is the optional ISO-8601
    date filter; when ``None``, the latest persisted row per symbol is
    returned.
    """

    symbol_ids: Sequence[int] = field(default_factory=tuple)
    symbols: Sequence[str] = field(default_factory=tuple)
    session_date: str | None = None

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            {
                "symbol_ids": list(self.symbol_ids),
                "symbols": list(self.symbols),
                "session_date": self.session_date,
            },
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> "PrevDayQueryRequest":
        try:
            obj = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise PrevDayEngineError(f"malformed query payload: {exc}") from exc
        if not isinstance(obj, dict):
            raise PrevDayEngineError(
                f"query payload must be a JSON object, got {type(obj).__name__}"
            )
        symbol_ids = tuple(int(x) for x in obj.get("symbol_ids") or ())
        symbols = tuple(str(x) for x in obj.get("symbols") or ())
        session_date_raw = obj.get("session_date")
        session_date = None if session_date_raw is None else str(session_date_raw)
        if not symbol_ids and not symbols:
            raise PrevDayEngineError(
                "query payload must include at least one of "
                "'symbol_ids' or 'symbols'"
            )
        return cls(
            symbol_ids=symbol_ids,
            symbols=symbols,
            session_date=session_date,
        )


@dataclass(frozen=True, slots=True)
class PrevDayQueryReply:
    """Reply to a ``mem.prev_day.query`` request."""

    records: Sequence[PreviousDayMemory]
    missing: Sequence[str]
    ts_ns: int

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            {
                "records": [rec.model_dump(mode="json") for rec in self.records],
                "missing": list(self.missing),
                "ts_ns": int(self.ts_ns),
            },
            separators=(",", ":"),
        ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class PrevDayReady:
    """Payload of the ``mem.prev_day.ready`` announcement."""

    session_date: str  # ISO-8601 date the dataset belongs to
    symbol_count: int
    ts_ns: int

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            {
                "session_date": self.session_date,
                "symbol_count": int(self.symbol_count),
                "ts_ns": int(self.ts_ns),
            },
            separators=(",", ":"),
        ).encode("utf-8")


# ---------------------------------------------------------------------------
# Provider protocol for the next-session inputs -----------------------------
# ---------------------------------------------------------------------------


class SessionInputProvider(Protocol):
    """Callable that returns the next-session inputs.

    Decoupled so the engine has no opinion on where the data comes
    from — the production aggregator pulls from Timescale and the live
    Hot_Path tape; tests pass a synchronous in-memory builder.

    Receives the calendar date the previous session belongs to (the
    one whose ``ops.session.end`` triggered the compute job).
    """

    async def __call__(
        self, session_date_iso: str
    ) -> PrevDaySessionInputs: ...


# ---------------------------------------------------------------------------
# Engine --------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _wall_ns() -> int:
    return time.time_ns()


@dataclass
class PrevDayMemoryEngine:
    """Owns the ``mem.prev_day.*`` persistence and exposure (R15).

    Construct via the keyword-only fields and call :meth:`start` /
    :meth:`aclose` to manage its lifecycle. Use as an async context
    manager for the simple case::

        async with PrevDayMemoryEngine(
            publisher=publisher,
            subscriber=subscriber,
            reply_server=reply_server,
            timescale_writer=writer,
            timescale_reader=reader,
            qdrant=qdrant,
            session_input_provider=provider,
        ) as engine:
            ...

    Fields:
        publisher: Sink for ``mem.prev_day.*`` event publications.
        subscriber: Subscription registrar for ``ops.session.end``.
        reply_server: Request-reply server for ``mem.prev_day.query``.
        timescale_writer: Persists ``PreviousDayMemoryRow`` rows.
        timescale_reader: Loads rows back for query replies.
        qdrant: Optional Qdrant gateway. ``None`` skips the
            ``market_memory`` upsert step (useful for tests that don't
            want to provision Qdrant). The Timescale write still runs.
        session_input_provider: Callable returning the next-session
            inputs. ``None`` causes the compute job to log a warning
            and exit without persisting — the engine still serves
            queries from previously-persisted rows.
        compute_deadline_s: Wall-clock budget for the compute job. The
            job is cancelled and ``mem.prev_day.ready`` is not emitted
            on overrun.
        clock_ns: Override for the wall-clock nanosecond timestamp
            stamped on every persisted row. Useful for deterministic
            tests.
        chunk_size: Number of rows per Timescale executemany batch.
            Larger values amortise the round-trip but raise the worst
            case partial-failure surface; the default of 64 is
            conservative.
    """

    publisher: PrevDayBusPublisher
    subscriber: PrevDayBusSubscriber
    reply_server: PrevDayRequestReplyServer
    timescale_writer: TimescaleWriter
    timescale_reader: TimescaleReader
    qdrant: MemoryRagQdrant | None = None
    session_input_provider: SessionInputProvider | None = None
    compute_deadline_s: float = 600.0
    clock_ns: Callable[[], int] = _wall_ns
    chunk_size: int = 64

    # Internals -------------------------------------------------------

    _query_ctx: Any = field(default=None, init=False, repr=False)
    _session_end_ctx: Any = field(default=None, init=False, repr=False)
    _compute_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)
    _running_for_session: str | None = field(default=None, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    # ----- async-context-manager hooks -------------------------------

    async def __aenter__(self) -> "PrevDayMemoryEngine":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    # ----- lifecycle --------------------------------------------------

    async def start(self) -> None:
        """Register the request-reply handler and ``ops.session.end`` subscription."""
        if self._started:
            return
        if self._closed:
            raise PrevDayEngineError("engine has been closed and cannot restart")

        # Register the request-reply server for `mem.prev_day.query`.
        # The context manager is held open for the engine's lifetime.
        self._query_ctx = self.reply_server.serve(
            SUBJECT_MEM_PREV_DAY_QUERY,
            self._handle_query,
        )
        await self._query_ctx.__aenter__()

        # Subscribe to `ops.session.end`. The handler MUST return
        # quickly — it spawns the compute job on a fresh task.
        self._session_end_ctx = self.subscriber.subscribe(
            SUBJECT_OPS_SESSION_END,
            self._on_session_end,
        )
        await self._session_end_ctx.__aenter__()

        self._started = True
        _LOG.info(
            "prev_day_engine_started",
            query_subject=SUBJECT_MEM_PREV_DAY_QUERY,
            session_end_subject=SUBJECT_OPS_SESSION_END,
        )

    async def aclose(self) -> None:
        """Tear down subscriptions and cancel any in-flight compute task."""
        if self._closed:
            return
        self._closed = True
        # Cancel the compute task first so its persistence stops mid-flight.
        if self._compute_task is not None and not self._compute_task.done():
            self._compute_task.cancel()
            try:
                await self._compute_task
            except (asyncio.CancelledError, BaseException):  # noqa: BLE001
                pass
        # Unwind the bus context managers in reverse registration order.
        if self._session_end_ctx is not None:
            await self._session_end_ctx.__aexit__(None, None, None)
            self._session_end_ctx = None
        if self._query_ctx is not None:
            await self._query_ctx.__aexit__(None, None, None)
            self._query_ctx = None
        self._started = False
        _LOG.info("prev_day_engine_closed")

    # ----- query handler ---------------------------------------------

    async def _handle_query(self, payload: bytes) -> bytes:
        """Decode a ``mem.prev_day.query`` request, build the reply payload."""
        try:
            request = PrevDayQueryRequest.from_json_bytes(payload)
        except PrevDayEngineError as exc:
            # Reply with a structured error payload so the requester
            # can distinguish malformed input from missing data.
            return json.dumps(
                {"error": str(exc), "ts_ns": self.clock_ns()},
                separators=(",", ":"),
            ).encode("utf-8")

        records: list[PreviousDayMemory] = []
        missing: list[str] = []

        # Resolve symbol_ids first.
        for sid in request.symbol_ids:
            row = await self._load_latest_for_symbol(symbol_id=sid)
            if row is None:
                missing.append(str(sid))
            else:
                records.append(build_prev_day_event(row))

        # Resolve textual symbols by latest-row scan, scoped to a small
        # 30-day window — the retrieval pipeline never asks for older
        # data from this engine (it goes to Qdrant for that). 30 days
        # spans the longest market holiday cluster comfortably.
        if request.symbols:
            now = datetime.now(timezone.utc)
            window_start = now.replace(hour=0, minute=0, second=0, microsecond=0).replace(
                day=1
            )
            # Pull a wide enough window to be safe (90 days covers
            # any plausible NSE/BSE holiday cluster).
            window_start = datetime.fromtimestamp(
                max(0.0, now.timestamp() - 90 * 86400), tz=timezone.utc
            )
            for sym in request.symbols:
                rows = await self.timescale_reader.read_prev_day_memory(
                    window_start,
                    now,
                    symbol=sym,
                    limit=1,
                )
                # ``read_prev_day_memory`` returns ASC-by-ts; the latest is the last.
                if not rows:
                    missing.append(sym)
                else:
                    records.append(build_prev_day_event(rows[-1]))

        reply = PrevDayQueryReply(
            records=tuple(records),
            missing=tuple(missing),
            ts_ns=self.clock_ns(),
        )
        return reply.to_json_bytes()

    async def _load_latest_for_symbol(
        self, *, symbol_id: int
    ) -> PreviousDayMemoryRow | None:
        return await self.timescale_reader.read_prev_day_memory_latest(symbol_id=symbol_id)

    # ----- session-end subscriber ------------------------------------

    async def _on_session_end(self, payload: bytes) -> None:
        """Schedule the compute job; the callback returns within microseconds."""
        try:
            obj = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            _LOG.warning(
                "prev_day_session_end_payload_malformed",
                error=str(exc),
            )
            return
        if not isinstance(obj, dict) or obj.get("phase") != "end":
            _LOG.warning(
                "prev_day_session_end_payload_unexpected",
                payload=obj,
            )
            return

        # The session manager only puts the session_id on the wire.
        # Use today's UTC date as the canonical session_date — the
        # provider may override via the inputs it returns.
        session_date_iso = datetime.now(timezone.utc).date().isoformat()

        # If a job is already running for this date, ignore — exactly
        # one persisted next-session record per symbol per day.
        if self._compute_task is not None and not self._compute_task.done():
            running_for = self._running_for_session
            if running_for == session_date_iso:
                _LOG.info(
                    "prev_day_compute_already_running",
                    session_date=session_date_iso,
                )
                return
            # A different date is running — let it finish; new job
            # will be scheduled at the next session_end.
            _LOG.warning(
                "prev_day_compute_overlap",
                running_for=running_for,
                requested=session_date_iso,
            )
            return

        self._running_for_session = session_date_iso
        # Spawn the compute job — never await it from inside this callback.
        self._compute_task = asyncio.create_task(
            self._run_compute_job(session_date_iso),
            name=f"prev_day_compute:{session_date_iso}",
        )

    # ----- compute job -----------------------------------------------

    async def _run_compute_job(self, session_date_iso: str) -> None:
        """Compute and persist the next-session dataset; emit ``mem.prev_day.ready``."""
        provider = self.session_input_provider
        if provider is None:
            _LOG.warning(
                "prev_day_compute_no_provider",
                session_date=session_date_iso,
            )
            self._running_for_session = None
            return

        try:
            await asyncio.wait_for(
                self._do_compute(provider, session_date_iso),
                timeout=self.compute_deadline_s,
            )
        except asyncio.TimeoutError:
            _LOG.error(
                "prev_day_compute_timeout",
                session_date=session_date_iso,
                deadline_s=self.compute_deadline_s,
            )
        except asyncio.CancelledError:
            _LOG.warning(
                "prev_day_compute_cancelled",
                session_date=session_date_iso,
            )
            raise
        except Exception as exc:  # noqa: BLE001
            _LOG.error(
                "prev_day_compute_failed",
                session_date=session_date_iso,
                error=str(exc),
            )
        finally:
            self._running_for_session = None

    async def _do_compute(
        self,
        provider: SessionInputProvider,
        session_date_iso: str,
    ) -> None:
        inputs = await provider(session_date_iso)
        computed_ts_ns = self.clock_ns()
        rows: list[PreviousDayMemoryRow] = []
        for sym in inputs.symbols:
            try:
                row = build_prev_day_row(
                    sym,
                    computed_ts_ns=computed_ts_ns,
                    embedding_point_id=None,  # filled in below
                )
            except ValueError as exc:
                # One bad symbol must not abort the whole job — log
                # and skip. The per-symbol publish below also skips it.
                _LOG.warning(
                    "prev_day_row_invalid",
                    symbol=sym.symbol,
                    symbol_id=sym.symbol_id,
                    error=str(exc),
                )
                continue
            rows.append(row)

        if not rows:
            _LOG.warning(
                "prev_day_compute_no_rows",
                session_date=session_date_iso,
            )
            return

        # Attach stable embedding point ids before the Timescale write
        # so the persisted row knows which Qdrant point it points at.
        rows = [
            PreviousDayMemoryRow(
                **{**row.model_dump(), "embedding_point_id": stable_embedding_point_id(row)}
            )
            for row in rows
        ]

        # 1. Timescale upsert (chunked).
        for start in range(0, len(rows), self.chunk_size):
            chunk = rows[start : start + self.chunk_size]
            await self.timescale_writer.write_prev_day_memory(chunk)

        # 2. Qdrant ``market_memory`` upsert (best-effort).
        if self.qdrant is not None:
            await self._upsert_market_memory(rows)

        # 3. Per-symbol publication.
        for row in rows:
            event = build_prev_day_event(row)
            payload = event.model_dump_json().encode("utf-8")
            subject = mem_prev_day_subject(row.symbol_id)
            try:
                await self.publisher.publish(subject, payload)
            except Exception as exc:  # noqa: BLE001 - logged + skip
                _LOG.warning(
                    "prev_day_publish_failed",
                    subject=subject,
                    error=str(exc),
                )

        # 4. Single ``mem.prev_day.ready`` announcement.
        ready = PrevDayReady(
            session_date=session_date_iso,
            symbol_count=len(rows),
            ts_ns=self.clock_ns(),
        )
        try:
            await self.publisher.publish(
                SUBJECT_MEM_PREV_DAY_READY, ready.to_json_bytes()
            )
        except Exception as exc:  # noqa: BLE001 - logged + dropped
            _LOG.warning(
                "prev_day_ready_publish_failed",
                session_date=session_date_iso,
                error=str(exc),
            )
        else:
            _LOG.info(
                "prev_day_ready_emitted",
                session_date=session_date_iso,
                symbol_count=len(rows),
            )

    async def _upsert_market_memory(
        self, rows: Sequence[PreviousDayMemoryRow]
    ) -> None:
        """Best-effort Qdrant upsert of the embedding-friendly summaries.

        We do not run an embedder inline here — the actual vector is
        materialised by a downstream embedder service. What we
        persist now is a 1-D placeholder vector tagged with the
        canonical text summary so the kNN index has a stable point id
        per (symbol_id, session_date). The downstream embedder
        rewrites the vector when it runs.

        This keeps the engine's responsibilities scoped to
        persistence + exposure; the embedder lives in the broader
        retrieval pipeline (task 34.x).
        """
        if self.qdrant is None:
            return
        spec = self.qdrant.specs.get(CollectionName.MARKET_MEMORY)
        if spec is None:
            _LOG.warning("prev_day_qdrant_market_memory_unconfigured")
            return
        # Stable zero-vector placeholder: same length as the configured
        # collection dim. Cosine distance against this placeholder is
        # undefined, but Qdrant will refuse a zero-norm cosine search,
        # which is the desired fail-loud behaviour until the embedder
        # rewrites the vector.
        zero_vector = [0.0] * spec.vector_dim

        records = [
            VectorRecord(
                point_id=row.embedding_point_id or stable_embedding_point_id(row),
                vector=zero_vector,
                payload={
                    "kind": "prev_day_memory",
                    "symbol_id": int(row.symbol_id),
                    "symbol": row.symbol,
                    "session_date": row.session_date.isoformat(),
                    "summary": format_prev_day_summary(row),
                    "computed_ts_ns": int(row.computed_ts_ns),
                    "open_paise": int(row.open_paise),
                    "high_paise": int(row.high_paise),
                    "low_paise": int(row.low_paise),
                    "close_paise": int(row.close_paise),
                    "vwap_paise": int(row.vwap_paise),
                    "delivery_volume": int(row.delivery_volume),
                    "total_volume": int(row.total_volume),
                },
            )
            for row in rows
        ]
        try:
            # ``attach_embedding=False`` because we do not want a CBOR
            # copy of the placeholder zero-vector polluting the
            # payload — the embedder will own the canonical embedding
            # when it runs.
            await self.qdrant.upsert_batch(
                CollectionName.MARKET_MEMORY,
                records,
                attach_embedding=False,
                wait=True,
            )
        except Exception as exc:  # noqa: BLE001 - logged + dropped
            _LOG.warning(
                "prev_day_qdrant_upsert_failed",
                count=len(records),
                error=str(exc),
            )


__all__ = [
    "PrevDayEngineError",
    "PrevDayMemoryEngine",
    "PrevDayQueryReply",
    "PrevDayQueryRequest",
    "PrevDayReady",
    "SessionInputProvider",
]
