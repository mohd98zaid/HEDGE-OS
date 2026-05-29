"""Shared NATS service-runtime helpers for the Warm_AI_Pipeline binaries.

The four Warm_AI microservices (``hedge-rank``, ``hedge-news``,
``hedge-regime``, ``hedge-psych``) all need the same plumbing:

* connect to NATS with sane reconnect behaviour,
* expose an ``async def publish(subject, payload: bytes)`` callable that
  matches the ``NatsRankPublisher`` / ``NatsPsychPublisher`` /
  ``NatsRegimePublisher`` / ``NatsNewsPublisher`` constructor contract,
* subscribe to one or more subjects and dispatch each message to an async
  handler,
* run until SIGINT / SIGTERM, then drain and close cleanly.

This module centralises that plumbing so each ``service.py`` stays a thin,
readable wiring file. It depends only on ``nats-py`` and ``structlog`` —
both already pinned in ``pyproject.toml`` — so importing it never pulls in
the heavy ML stack.

Graceful degradation is a first-class concern: a service must still *run*
(and keep the cockpit panel alive) even when an optional backend (Redis,
Ollama, Qdrant, ONNX weights) is unavailable. The helpers here never raise
on a publish failure; they log and continue.
"""

from __future__ import annotations

import asyncio
import os
import signal
from typing import Awaitable, Callable, Final, Sequence

import structlog

_LOG: Final = structlog.get_logger(__name__)

#: Default NATS URL — overridable via ``HEDGE_NATS_URL`` to match the Rust
#: binaries and ``start.bat``.
DEFAULT_NATS_URL: Final[str] = "nats://127.0.0.1:4222"


def nats_url() -> str:
    """Resolve the NATS URL from the environment (or the dev default)."""
    return os.environ.get("HEDGE_NATS_URL", DEFAULT_NATS_URL)


def configure_logging() -> None:
    """Configure structlog for human-readable console output.

    The Warm_AI services run in dev terminals (``start.bat`` ``cmd /k``
    windows) where an operator wants to *see* events, mirroring the Rust
    feed shims' ``compact`` format rather than the Hot_Path JSON format.
    """
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%H:%M:%S"),
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        cache_logger_on_first_use=True,
    )


class NatsService:
    """A connected NATS client plus subscription + publish plumbing.

    Usage::

        svc = await NatsService.connect("hedge-rank")
        await svc.subscribe("sig.emitted", on_signal)
        await svc.run_forever()   # blocks until SIGINT/SIGTERM

    The :meth:`publish` bound method matches the
    ``Callable[[str, bytes], Awaitable[None]]`` contract every
    ``Nats*Publisher`` wrapper expects, so a service wires it as::

        publisher = NatsRankPublisher(async_publish=svc.publish)
    """

    def __init__(self, service_name: str, nc: "object") -> None:
        self._name = service_name
        self._nc = nc
        self._stop = asyncio.Event()
        self._subs: list[object] = []

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    async def connect(cls, service_name: str) -> "NatsService":
        """Connect to NATS with reconnection enabled.

        Raises:
            Exception: only if the *initial* connection fails. Once
                connected, transient drops are handled by nats-py's
                reconnect logic and surfaced via the callbacks below.
        """
        import nats

        url = nats_url()

        async def _disconnected_cb() -> None:
            _LOG.warning("nats_disconnected", service=service_name)

        async def _reconnected_cb() -> None:
            _LOG.info("nats_reconnected", service=service_name, url=url)

        async def _error_cb(exc: Exception) -> None:
            _LOG.warning("nats_error", service=service_name, error=str(exc))

        nc = await nats.connect(
            url,
            name=service_name,
            max_reconnect_attempts=-1,  # retry forever
            reconnect_time_wait=2,
            disconnected_cb=_disconnected_cb,
            reconnected_cb=_reconnected_cb,
            error_cb=_error_cb,
        )
        _LOG.info("nats_connected", service=service_name, url=url)
        return cls(service_name, nc)

    # ------------------------------------------------------------------
    # Publish / subscribe
    # ------------------------------------------------------------------

    async def publish(self, subject: str, payload: bytes) -> None:
        """Publish raw bytes. Matches the ``Nats*Publisher`` callable shape.

        Never raises — a transient publish failure is logged and dropped so
        a single degraded emission cannot crash the service loop.
        """
        try:
            await self._nc.publish(subject, payload)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "nats_publish_failed",
                service=self._name,
                subject=subject,
                error=str(exc),
            )

    async def subscribe(
        self,
        subject: str,
        handler: Callable[[str, bytes], Awaitable[None]],
    ) -> None:
        """Subscribe to *subject*; dispatch each message to *handler*.

        The handler receives ``(subject, payload_bytes)``. Handler
        exceptions are caught and logged so one malformed message cannot
        tear down the subscription.
        """

        async def _on_msg(msg: "object") -> None:
            try:
                await handler(msg.subject, msg.data)  # type: ignore[attr-defined]
            except Exception as exc:  # noqa: BLE001
                _LOG.warning(
                    "handler_error",
                    service=self._name,
                    subject=getattr(msg, "subject", subject),
                    error=str(exc),
                )

        sub = await self._nc.subscribe(subject, cb=_on_msg)
        self._subs.append(sub)
        _LOG.info("subscribed", service=self._name, subject=subject)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def request_stop(self) -> None:
        """Signal the run loop to terminate."""
        self._stop.set()

    async def run_forever(self) -> None:
        """Block until SIGINT/SIGTERM (or :meth:`request_stop`), then clean up."""
        self._install_signal_handlers()
        await self._stop.wait()
        await self.close()

    async def run_until(
        self,
        background: Callable[[asyncio.Event], Awaitable[None]],
    ) -> None:
        """Run a background coroutine alongside the stop watcher.

        ``background`` receives the stop :class:`asyncio.Event` so a periodic
        producer loop (regime poller, psych recompute) can exit cleanly.
        """
        self._install_signal_handlers()
        bg_task = asyncio.create_task(background(self._stop))
        await self._stop.wait()
        bg_task.cancel()
        try:
            await bg_task
        except asyncio.CancelledError:
            pass
        await self.close()

    async def close(self) -> None:
        """Drain and close the NATS connection."""
        try:
            await self._nc.drain()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("nats_drain_failed", service=self._name, error=str(exc))
        _LOG.info("nats_closed", service=self._name)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except (NotImplementedError, RuntimeError):
                # add_signal_handler is unavailable on Windows ProactorEventLoop;
                # the KeyboardInterrupt path in asyncio.run covers SIGINT there.
                pass

    @property
    def client(self) -> "object":
        """The underlying nats-py client (for advanced use)."""
        return self._nc


def tracked_symbols_from_env(default: Sequence[str]) -> tuple[str, ...]:
    """Parse ``HEDGE_DEMO_SYMBOLS`` (comma-separated) or fall back to *default*.

    The Warm_AI services key their per-symbol logic on the same large-cap
    basket the Rust ``hedge-bus`` symbol table and the demo-synth use:
    RELIANCE, INFY, SBIN, HDFCBANK, ICICIBANK.
    """
    raw = os.environ.get("HEDGE_DEMO_SYMBOLS", "")
    if raw.strip():
        parsed = tuple(s.strip() for s in raw.split(",") if s.strip())
        if parsed:
            return parsed
    return tuple(default)


#: The canonical demo basket, mirroring ``hedge_bus::symbol_table`` and the
#: Rust demo-synth ``DEMO_BASKET``. Used as the default tracked-symbol
#: universe for the ranking + news services.
DEFAULT_SYMBOL_BASKET: Final[tuple[str, ...]] = (
    "RELIANCE",
    "INFY",
    "SBIN",
    "HDFCBANK",
    "ICICIBANK",
)


__all__ = [
    "DEFAULT_NATS_URL",
    "DEFAULT_SYMBOL_BASKET",
    "NatsService",
    "configure_logging",
    "nats_url",
    "tracked_symbols_from_env",
]
