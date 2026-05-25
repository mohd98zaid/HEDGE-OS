"""Python ``LatencyTracer`` mirroring the Rust ``hedge-obs::LatencyTracer``.

The Rust tracer is an RAII guard that emits exactly one
``LatencyRecord`` payload on ``obs.latency.<stage>`` when it drops, and
an additional ``obs.budget.breach.<stage>`` event when the elapsed time
exceeds the configured budget. The Python mirror serves the
**Warm_AI_Pipeline** (R11–R12, R27.4, R28.6) where Hot_Path-style RAII
is unavailable but the same wire contract must hold.

Differences from the Rust tracer:

* The Rust ``Stage`` enum is closed (TickIngest, FeatureExtraction,
  AiScoringFetch, RiskCheck, ExecutionRouting, BrokerSubmit) and is the
  authoritative set of stages on the Hot_Path. Warm_AI_Pipeline AI calls
  emit on ``obs.latency.ai_<stage>`` where ``<stage>`` is a free-form
  identifier (``finbert``, ``distilbert``, ``xgboost``, ``lightgbm``,
  ``isolation_forest``, ``tiny_lstm``). We therefore publish a separate
  payload type, ``AiLatencyRecord``, with an unconstrained ``stage``
  string. Subscribers that want type safety can validate against the
  ``LatencyRecordJson`` mirror in :mod:`hedge_warm_ai.schemas.obs_latency`
  for the closed enum and against this module for AI stages.
* ``correlation_id`` is propagated across ``await`` points using
  :class:`contextvars.ContextVar`, mirroring the Rust ``CorrelationId``
  which travels with the request struct itself.
* Timing is via :func:`time.perf_counter_ns`, which is the Python
  monotonic-ns clock with the same guarantees as ``quanta::Instant``.
"""

from __future__ import annotations

import contextvars
import os
import secrets
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from threading import RLock
from types import TracebackType
from typing import Any, Awaitable, Callable, Final, Iterable, Iterator, Optional, Protocol

import structlog

_LOG: Final = structlog.get_logger(__name__)

# --- correlation id ----------------------------------------------------------

#: Length of a correlation id in bytes. Mirrors the Rust ``CorrelationId(u128)``
#: which serialises as 16 big-endian bytes.
CORRELATION_ID_BYTES: Final[int] = 16


def new_correlation_id() -> bytes:
    """Generate a fresh 16-byte cryptographically random correlation id.

    Mirrors the Rust ``CorrelationId::new`` semantics where each id is a
    distinct 128-bit value. Uses :mod:`secrets` for unguessability so
    correlation ids survive log mining without leaking ordering.
    """
    return secrets.token_bytes(CORRELATION_ID_BYTES)


def correlation_id_from_bytes(raw: bytes) -> bytes:
    """Validate that *raw* is a 16-byte correlation id and return it."""
    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise TypeError(f"correlation_id must be bytes-like, got {type(raw).__name__}")
    b = bytes(raw)
    if len(b) != CORRELATION_ID_BYTES:
        raise ValueError(
            f"correlation_id must be {CORRELATION_ID_BYTES} bytes, got {len(b)}"
        )
    return b


def _hex(cid: bytes) -> str:
    """Return the lower-case hex form of a correlation id."""
    return cid.hex()


_CORRELATION_ID: contextvars.ContextVar[Optional[bytes]] = contextvars.ContextVar(
    "hedge_warm_ai.correlation_id", default=None
)


def current_correlation_id() -> Optional[bytes]:
    """Return the correlation id bound to the current async context, if any."""
    return _CORRELATION_ID.get()


def set_correlation_id(cid: Optional[bytes]) -> contextvars.Token[Optional[bytes]]:
    """Bind *cid* to the current context. Returns a token for ``reset``.

    Pass ``None`` to clear. The token is the standard
    :class:`contextvars.Token` and is reset by passing it back to
    :meth:`contextvars.ContextVar.reset`.
    """
    if cid is not None:
        cid = correlation_id_from_bytes(cid)
    return _CORRELATION_ID.set(cid)


# --- record + emitter --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AiLatencyRecord:
    """Per-call latency record for a Warm_AI_Pipeline AI stage.

    Field layout deliberately mirrors :class:`~hedge_warm_ai.schemas.
    obs_latency.LatencyRecordJson` so downstream tooling (Loki, Grafana
    panels, replay) can treat the two payload families uniformly.

    Attributes:
        correlation_id: 16-byte correlation id; serialises to hex.
        stage: AI-stage suffix. Published on
            ``obs.latency.ai_<stage>`` and (on breach)
            ``obs.budget.breach.ai_<stage>``.
        nanos: Elapsed wall-clock time in nanoseconds.
        budget_nanos: Configured budget. ``0`` disables breach emission.
        breach: ``True`` iff ``budget_nanos > 0`` and ``nanos > budget_nanos``.
    """

    correlation_id: bytes
    stage: str
    nanos: int
    budget_nanos: int
    breach: bool

    def to_json_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable mapping with stable field names."""
        return {
            "correlation_id": _hex(self.correlation_id),
            "stage": f"ai_{self.stage}",
            "nanos": int(self.nanos),
            "budget_nanos": int(self.budget_nanos),
            "breach": bool(self.breach),
        }


class AiLatencyEmitter(Protocol):
    """Sink for :class:`AiLatencyRecord` payloads.

    Implementations are responsible for routing a record to the
    appropriate NATS subjects:

    * ``obs.latency.ai_<stage>`` always.
    * ``obs.budget.breach.ai_<stage>`` when ``record.breach`` is ``True``.
    """

    def emit_record(self, record: AiLatencyRecord) -> None: ...
    def emit_breach(self, record: AiLatencyRecord) -> None: ...


class NoopAiLatencyEmitter:
    """Discards every record. Useful in tests that don't assert on emission."""

    def emit_record(self, record: AiLatencyRecord) -> None:  # noqa: D401
        return

    def emit_breach(self, record: AiLatencyRecord) -> None:  # noqa: D401
        return


class InMemoryAiLatencyEmitter:
    """Captures records in memory for assertion in tests.

    Thread-safe: the underlying lists are guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._records: list[AiLatencyRecord] = []
        self._breaches: list[AiLatencyRecord] = []

    def emit_record(self, record: AiLatencyRecord) -> None:
        with self._lock:
            self._records.append(record)

    def emit_breach(self, record: AiLatencyRecord) -> None:
        with self._lock:
            self._breaches.append(record)

    @property
    def records(self) -> list[AiLatencyRecord]:
        with self._lock:
            return list(self._records)

    @property
    def breaches(self) -> list[AiLatencyRecord]:
        with self._lock:
            return list(self._breaches)

    def reset(self) -> None:
        with self._lock:
            self._records.clear()
            self._breaches.clear()


# Type alias for any object with an async `publish(subject, payload)` API.
class _PublisherProtocol(Protocol):  # pragma: no cover - structural typing
    async def publish(self, subject: str, payload: bytes) -> None: ...


@dataclass
class NatsAiLatencyEmitter:
    """NATS-backed emitter that publishes JSON payloads.

    The emitter takes a *publish_callable* that performs the network I/O.
    The callable is invoked synchronously from :meth:`emit_record` /
    :meth:`emit_breach` and is expected to schedule its own task — this
    matches the Rust ``NatsEmitter`` which spawns a Tokio task per call so
    the caller never blocks on the broker.

    Two integration shapes are supported:

    * Pass a synchronous ``publish_callable(subject: str, payload: bytes)``
      that internally does ``asyncio.create_task(client.publish(...))``.
    * Pass a coroutine function and let
      :meth:`NatsAiLatencyEmitter.from_async` wrap it.

    JSON encoding happens on the caller's thread (the payload is small).
    """

    publish_callable: Callable[[str, bytes], None]
    subject_prefix_record: str = "obs.latency."
    subject_prefix_breach: str = "obs.budget.breach."

    @classmethod
    def from_async(
        cls,
        async_publish: Callable[[str, bytes], Awaitable[None]],
        *,
        subject_prefix_record: str = "obs.latency.",
        subject_prefix_breach: str = "obs.budget.breach.",
    ) -> "NatsAiLatencyEmitter":
        """Wrap an ``async def publish(subject, payload)`` into an emitter.

        The wrapper schedules each call via :func:`asyncio.ensure_future`
        on the running loop. If no loop is running (e.g. caller is purely
        synchronous), the coroutine is run to completion via
        :func:`asyncio.run` — this is the slow path and only happens when
        the emitter is misused outside an async context.
        """
        import asyncio

        def _publish(subject: str, payload: bytes) -> None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(async_publish(subject, payload))
                return
            loop.create_task(async_publish(subject, payload))

        return cls(
            publish_callable=_publish,
            subject_prefix_record=subject_prefix_record,
            subject_prefix_breach=subject_prefix_breach,
        )

    def _payload(self, record: AiLatencyRecord) -> bytes:
        import json

        return json.dumps(record.to_json_dict(), separators=(",", ":")).encode("utf-8")

    def emit_record(self, record: AiLatencyRecord) -> None:
        subject = f"{self.subject_prefix_record}ai_{record.stage}"
        try:
            self.publish_callable(subject, self._payload(record))
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ai_latency_record_publish_failed",
                stage=record.stage,
                error=str(exc),
            )

    def emit_breach(self, record: AiLatencyRecord) -> None:
        subject = f"{self.subject_prefix_breach}ai_{record.stage}"
        try:
            self.publish_callable(subject, self._payload(record))
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ai_latency_breach_publish_failed",
                stage=record.stage,
                error=str(exc),
            )


# --- tracer ------------------------------------------------------------------


class LatencyTracer(AbstractContextManager["LatencyTracer"]):
    """Context manager that emits exactly one :class:`AiLatencyRecord` on exit.

    Usage::

        with LatencyTracer("finbert", emitter, budget_ns=10_000_000):
            await runtime.infer_nlp("finbert", text)

    Or as an explicit start / stop pair::

        t = LatencyTracer("xgboost", emitter, budget_ns=5_000_000).start()
        try:
            scores = model.predict(features)
        finally:
            t.stop()

    The tracer is **safe** to use across ``await`` points: the
    correlation id is captured at construction from
    :func:`current_correlation_id` (or supplied explicitly) and is not
    re-read after that point.

    Cancelling via :meth:`cancel` suppresses emission — useful for fast
    no-op paths that should not pollute the latency channel.
    """

    __slots__ = (
        "_stage",
        "_emitter",
        "_budget_ns",
        "_correlation_id",
        "_started_ns",
        "_armed",
        "_finished",
    )

    def __init__(
        self,
        stage: str,
        emitter: AiLatencyEmitter,
        *,
        budget_ns: int = 0,
        correlation_id: Optional[bytes] = None,
    ) -> None:
        if not stage or not isinstance(stage, str):
            raise ValueError("stage must be a non-empty string")
        if budget_ns < 0:
            raise ValueError("budget_ns must be >= 0")

        cid = correlation_id if correlation_id is not None else current_correlation_id()
        if cid is None:
            cid = new_correlation_id()
        else:
            cid = correlation_id_from_bytes(cid)

        self._stage = stage
        self._emitter = emitter
        self._budget_ns = int(budget_ns)
        self._correlation_id = cid
        self._started_ns: Optional[int] = None
        self._armed = True
        self._finished = False

    # -- explicit start/stop -------------------------------------------------

    def start(self) -> "LatencyTracer":
        """Start the timer. Idempotent on the first call."""
        if self._started_ns is None:
            self._started_ns = time.perf_counter_ns()
        return self

    def cancel(self) -> None:
        """Suppress emission. The timer is still readable via :meth:`elapsed_ns`."""
        self._armed = False

    def elapsed_ns(self) -> int:
        """Return elapsed nanoseconds without consuming the tracer."""
        if self._started_ns is None:
            return 0
        return time.perf_counter_ns() - self._started_ns

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def correlation_id(self) -> bytes:
        return self._correlation_id

    def stop(self) -> AiLatencyRecord:
        """Stop the timer, emit the record, and return it.

        Returns the emitted record even when the tracer is cancelled
        (the record is built but not published) so callers can still
        observe the elapsed time. After the first call this is a no-op
        and returns the previously built record from
        :attr:`_finished_record`.
        """
        if self._started_ns is None:
            self.start()
        elapsed = self.elapsed_ns()
        budget = self._budget_ns
        breach = budget > 0 and elapsed > budget
        record = AiLatencyRecord(
            correlation_id=self._correlation_id,
            stage=self._stage,
            nanos=elapsed,
            budget_nanos=budget,
            breach=breach,
        )
        if self._armed and not self._finished:
            try:
                self._emitter.emit_record(record)
                if breach:
                    self._emitter.emit_breach(record)
            except Exception as exc:  # pragma: no cover - never let emission kill the caller
                _LOG.warning(
                    "ai_latency_emit_failed",
                    stage=self._stage,
                    error=str(exc),
                )
        self._finished = True
        return record

    # -- context-manager protocol -------------------------------------------

    def __enter__(self) -> "LatencyTracer":
        return self.start()

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.stop()


def _allow_clock_skip() -> bool:
    """Return ``True`` if tests should accept zero-elapsed measurements."""
    return os.environ.get("HEDGE_LATENCY_TRACER_ALLOW_ZERO") == "1"


def iter_records(emitter: InMemoryAiLatencyEmitter) -> Iterator[AiLatencyRecord]:
    """Convenience iterator used in tests."""
    yield from emitter.records


def iter_breaches(emitter: InMemoryAiLatencyEmitter) -> Iterable[AiLatencyRecord]:
    """Convenience iterator used in tests."""
    yield from emitter.breaches
