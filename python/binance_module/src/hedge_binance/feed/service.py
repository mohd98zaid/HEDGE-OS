"""Binance Market-Data Feed — WebSocket subscriber.

Connects to Binance combined stream for configured symbols, decodes
``miniTicker`` + ``bookTicker`` events, and publishes them on NATS
subjects ``crypto.tick.<SYMBOL>`` (e.g. ``crypto.tick.BTCUSDT``).

Also caches the latest tick in Redis under ``binance:tick:<symbol>``
with a 10-second TTL so downstream services can warm-start quickly.

NATS subject schema
-------------------
  crypto.tick.<SYMBOL>  — JSON tick event (see _Tick dataclass)

Redis key schema
----------------
  binance:tick:<symbol>  — JSON tick, TTL 10 s

Environment variables consumed
--------------------------------
  BINANCE_SYMBOLS   — comma-separated symbols (default: BTCUSDT,ETHUSDT,…)
  BINANCE_TESTNET   — "true" → testnet stream endpoint
  HEDGE_NATS_URL    — shared NATS (default: nats://127.0.0.1:4222)
  HEDGE_REDIS_URL   — shared Redis (default: redis://127.0.0.1:6379)
"""

from __future__ import annotations

import asyncio
import json
import time as _time
from dataclasses import dataclass, asdict
from typing import Sequence

import structlog

from ..runtime import (
    NatsService,
    configure_logging,
    redis_url,
    symbols_from_env,
    ws_base,
)

_LOG = structlog.get_logger(__name__)

# ── Prometheus metrics ───────────────────────────────────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server as _prom_start

    _TICKS_RECEIVED = Counter(
        "binance_feed_ticks_total", "WebSocket tick messages received", ["symbol"]
    )
    _LAST_PRICE = Gauge(
        "binance_feed_last_price_usdt", "Latest trade price (USDT)", ["symbol"]
    )
    _PROM_ENABLED = True
except Exception:  # pragma: no cover
    _PROM_ENABLED = False


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Tick:
    symbol: str
    price: float       # last trade price
    bid: float         # best bid
    ask: float         # best ask
    volume_24h: float  # base-asset 24h volume
    ts_ms: int         # event time in epoch-ms


def _parse_mini_ticker(raw: dict) -> Tick | None:
    """Parse a ``<symbol>@miniTicker`` stream event."""
    try:
        return Tick(
            symbol=raw["s"],
            price=float(raw["c"]),
            bid=float(raw.get("b", raw["c"])),
            ask=float(raw.get("a", raw["c"])),
            volume_24h=float(raw["v"]),
            ts_ms=int(raw["E"]),
        )
    except (KeyError, ValueError, TypeError):
        return None


# ── Redis helper ─────────────────────────────────────────────────────────────

class _RedisCache:
    def __init__(self, url: str) -> None:
        self._url = url
        self._r: object | None = None

    async def connect(self) -> None:
        try:
            import redis.asyncio as aioredis  # type: ignore[import]
            self._r = aioredis.from_url(self._url, decode_responses=True)
            await self._r.ping()  # type: ignore[union-attr]
            _LOG.info("redis_connected", url=self._url, service="binance-feed")
        except Exception as exc:
            _LOG.warning("redis_unavailable", error=str(exc), service="binance-feed")
            self._r = None

    async def set_tick(self, symbol: str, payload: str) -> None:
        if self._r is None:
            return
        try:
            await self._r.set(f"binance:tick:{symbol}", payload, ex=10)  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001
            pass


# ── WebSocket feed loop ───────────────────────────────────────────────────────

async def _feed_loop(
    symbols: tuple[str, ...],
    svc: NatsService,
    cache: _RedisCache,
    stop: asyncio.Event,
) -> None:
    """Connect to Binance combined stream and publish ticks until *stop* is set."""
    import websockets  # type: ignore[import]

    streams = "/".join(f"{s.lower()}@miniTicker" for s in symbols)
    url = f"{ws_base()}/stream?streams={streams}"

    _LOG.info("binance_feed_connecting", url=url, symbols=list(symbols))

    backoff = 1.0
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                _LOG.info("binance_feed_connected", symbols=list(symbols))
                backoff = 1.0  # reset on successful connection
                while not stop.is_set():
                    try:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        _LOG.warning("binance_feed_timeout")
                        break  # reconnect
                    try:
                        envelope = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue
                    data = envelope.get("data", envelope)
                    tick = _parse_mini_ticker(data)
                    if tick is None:
                        continue
                    payload_str = json.dumps(asdict(tick), separators=(",", ":"))
                    payload_bytes = payload_str.encode("utf-8")
                    await svc.publish(f"crypto.tick.{tick.symbol}", payload_bytes)
                    await cache.set_tick(tick.symbol, payload_str)
                    if _PROM_ENABLED:
                        _TICKS_RECEIVED.labels(symbol=tick.symbol).inc()
                        _LAST_PRICE.labels(symbol=tick.symbol).set(tick.price)
        except Exception as exc:  # noqa: BLE001
            if stop.is_set():
                break
            _LOG.warning("binance_feed_error", error=str(exc), retry_in=backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    _LOG.info("binance_feed_stopped")


# ── Entry point ───────────────────────────────────────────────────────────────

async def _run() -> int:
    configure_logging()
    symbols = symbols_from_env()
    _LOG.info("binance_feed_starting", symbols=list(symbols))

    cache = _RedisCache(redis_url())
    await cache.connect()

    if _PROM_ENABLED:
        try:
            _prom_start(9300)
            _LOG.info("prometheus_started", port=9300)
        except Exception:
            pass

    svc = await NatsService.connect("binance-feed")
    await svc.run_until(lambda stop: _feed_loop(symbols, svc, cache, stop))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
