"""Binance Position Tracker — account reconciliation.

Polls the Binance REST API every ``BINANCE_POSITION_POLL_S`` seconds
(default 10) to fetch account balances and open orders, then publishes
the current position snapshot on ``crypto.position``.

Also reacts to ``crypto.order.ack`` events to update an in-memory
position view between polls for low-latency UI updates.

NATS subjects
-------------
  Subscribe:  crypto.order.ack
  Publish:    crypto.position  (snapshot every poll interval)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
import urllib.parse
from typing import Any, Dict, Sequence

import httpx
import structlog

from ..runtime import (
    NatsService,
    binance_api_key,
    binance_api_secret,
    configure_logging,
    is_testnet,
    now_ms,
    rest_base,
)

_LOG = structlog.get_logger(__name__)

try:
    from prometheus_client import Gauge, start_http_server as _prom_start
    _POSITION_VALUE = Gauge("binance_position_value_usdt", "Position value USDT", ["asset"])
    _PROM_ENABLED = True
except Exception:
    _PROM_ENABLED = False


# ── HMAC signing ─────────────────────────────────────────────────────────────

def _sign(params: dict[str, Any], secret: str) -> str:
    query = urllib.parse.urlencode(params)
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


# ── REST account poller ────────────────────────────────────────────────────────

class AccountPoller:
    def __init__(self, api_key: str, api_secret: str, base: str) -> None:
        self._key    = api_key
        self._secret = api_secret
        self._base   = base
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self._base,
            headers={"X-MBX-APIKEY": self._key},
            timeout=10.0,
        )
        try:
            import time
            import hedge_binance.runtime as rt
            r = await self._client.get("/api/v3/time")
            r.raise_for_status()
            server_time = r.json()["serverTime"]
            local_time = int(time.time() * 1000)
            rt.TIME_OFFSET_MS = server_time - local_time
            _LOG.info("binance_time_synced", offset_ms=rt.TIME_OFFSET_MS)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("binance_time_sync_failed", error=str(exc))

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()

    async def fetch_balances(self) -> dict[str, float]:
        """Return {asset: free_balance} for non-zero balances."""
        if not self._client or not self._key:
            return {}
        params: dict[str, Any] = {"timestamp": now_ms(), "recvWindow": 5000}
        params["signature"] = _sign(params, self._secret)
        try:
            r = await self._client.get("/api/v3/account", params=params)
            r.raise_for_status()
            data = r.json()
            return {
                b["asset"]: float(b["free"])
                for b in data.get("balances", [])
                if float(b["free"]) > 0
            }
        except httpx.HTTPStatusError as exc:
            _LOG.warning("position_fetch_error", error=str(exc), body=exc.response.text)
            return {}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("position_fetch_error", error=str(exc))
            return {}

    async def fetch_open_orders(self) -> list[dict[str, Any]]:
        if not self._client or not self._key:
            return []
        params: dict[str, Any] = {"timestamp": now_ms(), "recvWindow": 5000}
        params["signature"] = _sign(params, self._secret)
        try:
            r = await self._client.get("/api/v3/openOrders", params=params)
            r.raise_for_status()
            return r.json()  # type: ignore[return-value]
        except httpx.HTTPStatusError as exc:
            _LOG.warning("open_orders_fetch_error", error=str(exc), body=exc.response.text)
            return []
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("open_orders_fetch_error", error=str(exc))
            return []


# ── Service logic ─────────────────────────────────────────────────────────────

async def _run() -> int:
    configure_logging()

    poll_interval = float(os.environ.get("BINANCE_POSITION_POLL_S", "10"))
    testnet = is_testnet()
    base    = rest_base()

    _LOG.info("binance_position_starting", base=base, testnet=testnet, poll_s=poll_interval)

    poller = AccountPoller(binance_api_key(), binance_api_secret(), base)
    await poller.start()

    svc = await NatsService.connect("binance-position")

    if _PROM_ENABLED:
        try:
            _prom_start(9304)
        except Exception:
            pass

    # In-memory incremental position (updated from order acks between polls)
    incremental: Dict[str, float] = {}

    async def on_order_ack(_subject: str, data: bytes) -> None:
        try:
            ack = json.loads(data.decode("utf-8"))
        except Exception:
            return
        symbol = ack.get("symbol", "")
        qty    = float(ack.get("executed_qty", 0.0))
        side   = ack.get("side", "buy").lower()
        if symbol:
            delta = qty if side == "buy" else -qty
            incremental[symbol] = incremental.get(symbol, 0.0) + delta

    async def _poll_loop(stop: asyncio.Event) -> None:
        while not stop.is_set():
            balances = await poller.fetch_balances()
            open_orders = await poller.fetch_open_orders()

            snapshot = {
                "balances": balances,
                "open_orders_count": len(open_orders),
                "incremental": incremental.copy(),
                "testnet": testnet,
                "ts_ms": now_ms(),
            }
            payload = json.dumps(snapshot, separators=(",", ":")).encode("utf-8")
            await svc.publish("crypto.position", payload)

            if _PROM_ENABLED:
                for asset, qty in balances.items():
                    _POSITION_VALUE.labels(asset=asset).set(qty)

            _LOG.info(
                "position_published",
                assets=list(balances.keys()),
                open_orders=len(open_orders),
            )

            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                pass

    await svc.subscribe("crypto.order.ack", on_order_ack)
    await svc.run_until(_poll_loop)
    await poller.stop()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
