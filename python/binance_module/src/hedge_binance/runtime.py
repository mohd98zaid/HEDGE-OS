"""Shared runtime helpers for the Binance crypto module.

Mirrors the design of ``hedge_warm_ai.service_runtime`` but uses
``crypto.*`` NATS subjects and reads credentials from ``BINANCE_*``
environment variables — never touching ``HEDGE_*`` keys.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from typing import Awaitable, Callable, Final, Sequence

import structlog

_LOG: Final = structlog.get_logger(__name__)

DEFAULT_NATS_URL: Final[str] = "nats://127.0.0.1:4222"
DEFAULT_REDIS_URL: Final[str] = "redis://127.0.0.1:6379"

# ── Crypto symbols traded on Binance ────────────────────────────────────────
DEFAULT_SYMBOLS: Final[tuple[str, ...]] = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
)


def nats_url() -> str:
    """Resolve the shared NATS URL (reuses infra, different subjects)."""
    return os.environ.get("HEDGE_NATS_URL", DEFAULT_NATS_URL)


def redis_url() -> str:
    return os.environ.get("HEDGE_REDIS_URL", DEFAULT_REDIS_URL)


def binance_api_key() -> str:
    return os.environ.get("BINANCE_API_KEY", "")


def binance_api_secret() -> str:
    return os.environ.get("BINANCE_API_SECRET", "")


def is_testnet() -> bool:
    return os.environ.get("BINANCE_TESTNET", "false").lower() in ("true", "1", "yes")


def symbols_from_env() -> tuple[str, ...]:
    raw = os.environ.get("BINANCE_SYMBOLS", "")
    if raw.strip():
        parsed = tuple(s.strip().upper() for s in raw.split(",") if s.strip())
        if parsed:
            return parsed
    return DEFAULT_SYMBOLS


def rest_base() -> str:
    if is_testnet():
        return "https://testnet.binance.vision"
    return "https://api.binance.com"


def ws_base() -> str:
    if is_testnet():
        return "wss://testnet.binance.vision"
    return "wss://stream.binance.com:9443"


def configure_logging() -> None:
    """Configure structlog for human-readable console output (same style as warm-ai)."""
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
    """Connected NATS client scoped to the Binance module (``crypto.*`` subjects only)."""

    def __init__(self, service_name: str, nc: object) -> None:
        self._name = service_name
        self._nc = nc
        self._stop = asyncio.Event()
        self._subs: list[object] = []

    @classmethod
    async def connect(cls, service_name: str) -> "NatsService":
        import nats  # type: ignore[import]

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
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            disconnected_cb=_disconnected_cb,
            reconnected_cb=_reconnected_cb,
            error_cb=_error_cb,
        )
        _LOG.info("nats_connected", service=service_name, url=url)
        return cls(service_name, nc)

    async def publish(self, subject: str, payload: bytes) -> None:
        """Publish to a ``crypto.*`` subject. Never raises."""
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
        async def _on_msg(msg: object) -> None:
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

    def request_stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        self._install_signal_handlers()
        await self._stop.wait()
        await self.close()

    async def run_until(
        self,
        background: Callable[[asyncio.Event], Awaitable[None]],
    ) -> None:
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
                pass  # Windows ProactorEventLoop — KeyboardInterrupt covers SIGINT

    @property
    def client(self) -> object:
        return self._nc


TIME_OFFSET_MS: int = 0

def now_ms() -> int:
    """Current UTC epoch in milliseconds (for Binance REST API timestamps)."""
    return int(time.time() * 1000) + TIME_OFFSET_MS


__all__ = [
    "DEFAULT_SYMBOLS",
    "NatsService",
    "binance_api_key",
    "binance_api_secret",
    "configure_logging",
    "is_testnet",
    "nats_url",
    "now_ms",
    "redis_url",
    "rest_base",
    "symbols_from_env",
    "ws_base",
]
