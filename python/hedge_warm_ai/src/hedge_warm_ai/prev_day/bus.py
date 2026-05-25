"""Bus abstractions for the Previous_Day_Memory_Engine (task 24.1).

The engine talks to NATS through three surfaces:

* :class:`PrevDayBusPublisher` — fire-and-forget publishes onto
  ``mem.prev_day.<sym>`` and ``mem.prev_day.ready``.
* :class:`PrevDayBusSubscriber` — subscribe to ``ops.session.end``
  (and optionally ``ops.session.start``) and dispatch the decoded
  payload to a coroutine.
* :class:`PrevDayRequestReplyServer` — register a coroutine handler
  for ``mem.prev_day.query`` request-reply.

The actual NATS client (``nats-py``) is wired only at the service-entry
boundary; tests pass :class:`InMemoryPrevDayBus` so the engine can be
exercised without the broker. This mirrors the
:class:`hedge_warm_ai.ollama_client.publisher.InMemoryDegradedPublisher`
pattern used elsewhere in the Warm_AI_Pipeline.

The publish / subscribe / request callables match the same
``(subject, payload_bytes)`` shape as ``hedge_warm_ai.ollama_client.NatsDegradedPublisher``,
so a service can plug an existing ``async def publish(subject, payload)``
helper directly into :class:`PrevDayBusPublisher`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from threading import RLock
from typing import AsyncIterator, Final, Protocol

import structlog

_LOG: Final = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Type aliases for the wire-level callables --------------------------------
# ---------------------------------------------------------------------------

#: ``async def publish(subject: str, payload: bytes) -> None``.
PrevDayPublishCallable = Callable[[str, bytes], Awaitable[None]]

#: ``async def request(subject: str, payload: bytes, *, timeout_s: float) -> bytes``.
PrevDayRequestCallable = Callable[[str, bytes, float], Awaitable[bytes]]

#: Subscription-side handler: ``async def handle(payload: bytes) -> None``.
PrevDaySubscribeCallable = Callable[[bytes], Awaitable[None]]

#: Request-reply server-side handler: ``async def handle(payload: bytes) -> bytes``.
PrevDayReplyHandler = Callable[[bytes], Awaitable[bytes]]


# ---------------------------------------------------------------------------
# Protocols ----------------------------------------------------------------
# ---------------------------------------------------------------------------


class PrevDayBusPublisher(Protocol):
    """Sink for ``mem.prev_day.*`` event publications."""

    async def publish(self, subject: str, payload: bytes) -> None: ...


class PrevDayBusSubscriber(Protocol):
    """Subscription registration: takes a subject and a coroutine handler.

    Returns an async context manager whose ``__aenter__`` registers the
    subscription and whose ``__aexit__`` tears it down. Implementations
    must guarantee the handler is invoked on a separate task so the
    callback never blocks the bus reader (R26 / design § Hot-path
    purity for the Warm_AI_Pipeline).
    """

    def subscribe(
        self, subject: str, handler: PrevDaySubscribeCallable
    ) -> "AsyncContextManager[None]": ...


class PrevDayRequestReplyServer(Protocol):
    """Server side of NATS request-reply.

    Implementations register a handler for ``subject`` and return an
    async context manager; on exit the handler is unregistered.
    """

    def serve(
        self, subject: str, handler: PrevDayReplyHandler
    ) -> "AsyncContextManager[None]": ...


# ``AsyncContextManager`` is used in the Protocols above. Importing it
# eagerly via ``contextlib.AbstractAsyncContextManager`` keeps the
# annotations resolvable at runtime for tests that introspect them.
from contextlib import AbstractAsyncContextManager as AsyncContextManager  # noqa: E402


# ---------------------------------------------------------------------------
# In-memory implementation (used by tests + the engine fixture) -------------
# ---------------------------------------------------------------------------


@dataclass
class _Subscription:
    subject: str
    handler: PrevDaySubscribeCallable


@dataclass
class _ReplyServer:
    subject: str
    handler: PrevDayReplyHandler


class InMemoryPrevDayBus:
    """Single-process in-memory bus implementing all three protocols.

    Designed for unit tests and the local `service.py` integration
    fixture. Behaviour is intentionally simple:

    * ``publish(subject, payload)`` invokes every registered subscriber
      for ``subject`` exactly once on a fresh ``asyncio.Task`` so the
      callback never blocks the publisher.
    * ``request(subject, payload, timeout_s)`` invokes the registered
      :class:`PrevDayReplyHandler` for ``subject`` and returns its
      bytes; raises :class:`asyncio.TimeoutError` on overrun and
      :class:`KeyError` if no handler is registered.
    * Captured publications are kept in :attr:`captured` for assertion
      in tests.

    Thread-safe: subscribe/unsubscribe registry is guarded by an
    :class:`RLock`. Async coroutines run inside the event loop the
    caller invoked them on; the lock is only held while mutating the
    registry, never across ``await``.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._subscribers: dict[str, list[_Subscription]] = {}
        self._reply_servers: dict[str, _ReplyServer] = {}
        self._captured: list[tuple[str, bytes]] = []
        self._tasks: set[asyncio.Task[None]] = set()

    # -------------------------------------------------------------------
    # Publisher / publish
    # -------------------------------------------------------------------

    async def publish(self, subject: str, payload: bytes) -> None:
        with self._lock:
            self._captured.append((subject, bytes(payload)))
            subs = list(self._subscribers.get(subject, ()))

        for sub in subs:
            task = asyncio.create_task(self._safe_invoke(sub, payload))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _safe_invoke(self, sub: _Subscription, payload: bytes) -> None:
        try:
            await sub.handler(payload)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "prev_day_bus_subscriber_raised",
                subject=sub.subject,
                error=str(exc),
            )

    # -------------------------------------------------------------------
    # Subscriber
    # -------------------------------------------------------------------

    @asynccontextmanager
    async def subscribe(
        self, subject: str, handler: PrevDaySubscribeCallable
    ) -> AsyncIterator[None]:
        sub = _Subscription(subject=subject, handler=handler)
        with self._lock:
            self._subscribers.setdefault(subject, []).append(sub)
        try:
            yield
        finally:
            with self._lock:
                bucket = self._subscribers.get(subject, [])
                if sub in bucket:
                    bucket.remove(sub)
                if not bucket and subject in self._subscribers:
                    del self._subscribers[subject]

    # -------------------------------------------------------------------
    # Request-reply
    # -------------------------------------------------------------------

    @asynccontextmanager
    async def serve(
        self, subject: str, handler: PrevDayReplyHandler
    ) -> AsyncIterator[None]:
        server = _ReplyServer(subject=subject, handler=handler)
        with self._lock:
            if subject in self._reply_servers:
                raise RuntimeError(
                    f"a request-reply handler is already registered for {subject!r}"
                )
            self._reply_servers[subject] = server
        try:
            yield
        finally:
            with self._lock:
                if self._reply_servers.get(subject) is server:
                    del self._reply_servers[subject]

    async def request(
        self, subject: str, payload: bytes, timeout_s: float = 5.0
    ) -> bytes:
        """Synthetic NATS-style request-reply for the in-memory bus.

        Used by tests to drive ``mem.prev_day.query`` without spinning
        up real NATS. Raises :class:`KeyError` if no server is
        registered, :class:`asyncio.TimeoutError` on overrun.
        """
        with self._lock:
            server = self._reply_servers.get(subject)
        if server is None:
            raise KeyError(f"no request-reply handler registered for {subject!r}")
        return await asyncio.wait_for(server.handler(bytes(payload)), timeout=timeout_s)

    # -------------------------------------------------------------------
    # Test helpers
    # -------------------------------------------------------------------

    @property
    def captured(self) -> list[tuple[str, bytes]]:
        """Return a snapshot of captured (subject, payload) tuples."""
        with self._lock:
            return list(self._captured)

    def clear(self) -> None:
        with self._lock:
            self._captured.clear()

    async def aclose(self) -> None:
        """Wait for any in-flight subscriber tasks to finish."""
        # Snapshot under lock so a late-arriving publish doesn't grow the set.
        with self._lock:
            tasks = list(self._tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# NATS adaptor (lightweight wiring around an async publish callable) --------
# ---------------------------------------------------------------------------


@dataclass
class CallablePrevDayPublisher:
    """Concrete :class:`PrevDayBusPublisher` wrapping a publish callable.

    Production callers wire an existing ``async def publish(subject,
    payload)`` helper (the one used by
    :class:`hedge_warm_ai.ollama_client.NatsDegradedPublisher` and
    :class:`hedge_warm_ai.onnx_runtime.NatsAiLatencyEmitter`) here.
    """

    publish_callable: PrevDayPublishCallable

    async def publish(self, subject: str, payload: bytes) -> None:
        await self.publish_callable(subject, payload)


__all__ = [
    "CallablePrevDayPublisher",
    "InMemoryPrevDayBus",
    "PrevDayBusPublisher",
    "PrevDayBusSubscriber",
    "PrevDayPublishCallable",
    "PrevDayReplyHandler",
    "PrevDayRequestCallable",
    "PrevDayRequestReplyServer",
    "PrevDaySubscribeCallable",
]
