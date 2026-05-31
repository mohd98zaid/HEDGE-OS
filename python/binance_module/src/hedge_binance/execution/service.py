"""Binance Order Execution Engine — hardened.

═══════════════════════════════════════════════════════════════════════
  FIXES vs v1
═══════════════════════════════════════════════════════════════════════
  ✅ API rate-limit tracking (Binance weight budget per minute)
  ✅ avg_price extracted from fill response (used by Risk for PnL)
  ✅ stop_loss_price / take_profit_price forwarded in the ACK
     (Risk service needs these to open a tracked position)
  ✅ Retry on transient 5xx errors (max 2 retries, exponential backoff)
  ✅ Proper close() called on rest client before exit
  ✅ Credentials loaded from env — NOT from command-line args
     (secrets are never visible in process list / Task Manager)

NATS subjects
─────────────
  Subscribe:  crypto.signal.approved
  Publish:    crypto.order.ack
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import urllib.parse
from typing import Any, Optional, Sequence

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
    from prometheus_client import Counter, Histogram, Gauge, start_http_server as _prom_start
    _ORDERS_SENT   = Counter(  "binance_exec_orders_total",      "Orders sent",         ["symbol", "side", "status"])
    _ORDER_LATENCY = Histogram("binance_exec_order_latency_ms",  "Order round-trip ms", ["symbol"])
    _API_WEIGHT    = Gauge(    "binance_exec_api_weight_used",    "Binance API weight used (per min)")
    _PROM_ENABLED  = True
except Exception:
    _PROM_ENABLED  = False


# ─────────────────────────────────────────────────────────────────────────────
# Rate limiter — Binance allows 1200 weight/min on Spot
# ─────────────────────────────────────────────────────────────────────────────

class WeightBudget:
    """
    Tracks the Binance request-weight header (X-MBX-USED-WEIGHT-1M).
    Pauses execution if we're close to the limit to avoid 429 bans.
    """
    LIMIT = 1100   # conservative — hard limit is 1200

    def __init__(self) -> None:
        self._used: int = 0
        self._reset_at: float = 0.0

    def update_from_headers(self, headers: httpx.Headers) -> None:
        w = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("x-mbx-used-weight-1m")
        if w:
            self._used = int(w)
            if _PROM_ENABLED:
                _API_WEIGHT.set(self._used)

    async def wait_if_needed(self) -> None:
        if self._used >= self.LIMIT:
            wait = max(5.0, 60.0 - (time.monotonic() - self._reset_at))
            _LOG.warning("api_rate_limit_pause", used=self._used, wait_s=round(wait, 1))
            await asyncio.sleep(wait)
            self._used = 0
            self._reset_at = time.monotonic()


# ─────────────────────────────────────────────────────────────────────────────
# HMAC signing
# ─────────────────────────────────────────────────────────────────────────────

def _sign(params: dict[str, Any], secret: str) -> str:
    """HMAC-SHA256 hex digest of the URL-encoded params (Binance standard)."""
    query = urllib.parse.urlencode(params)
    return hmac.new(
        secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# REST client
# ─────────────────────────────────────────────────────────────────────────────

class BinanceRestClient:

    MAX_RETRIES = 2

    def __init__(self, api_key: str, api_secret: str, base: str) -> None:
        self._key    = api_key
        self._secret = api_secret
        self._base   = base
        self._client: Optional[httpx.AsyncClient] = None
        self._budget = WeightBudget()

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
            self._client = None

    async def place_market_order(
        self,
        symbol:   str,
        side:     str,
        qty_usdt: float,
        executed_qty: float = 0.0,
    ) -> dict[str, Any]:
        """
        Fire a MARKET order. If executed_qty > 0, sell that exact amount.
        """
        if not self._client:
            return {"error": "client_not_started"}

        await self._budget.wait_if_needed()

        params: dict[str, Any] = {
            "symbol":        symbol,
            "side":          side.upper(),
            "type":          "MARKET",
            "timestamp":     now_ms(),
            "recvWindow":    5000,
        }
        if executed_qty > 0:
            # When closing a position, we must sell the exact amount we bought
            # Binance uses string formatting for exact LOT_SIZE, but float works for MARKETS
            # if we truncate to avoid floating point dust errors. We just pass it directly.
            params["quantity"] = executed_qty
        else:
            params["quoteOrderQty"] = round(qty_usdt, 2)
        params["signature"] = _sign(params, self._secret)

        last_error: dict[str, Any] = {}
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                resp = await self._client.post("/api/v3/order", params=params)
                self._budget.update_from_headers(resp.headers)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                body = exc.response.text
                self._budget.update_from_headers(exc.response.headers)
                _LOG.warning("binance_order_http_error",
                             status=code, body=body[:200], attempt=attempt)

                # If insufficient balance when closing, fetch actual balance and retry
                if code == 400 and "-2010" in body and executed_qty > 0 and attempt == 0:
                    try:
                        base_asset = symbol.replace("USDT", "")
                        acc_params: dict[str, Any] = {"timestamp": now_ms(), "recvWindow": 5000}
                        acc_params["signature"] = _sign(acc_params, self._secret)
                        acc_res = await self._client.get("/api/v3/account", params=acc_params)
                        acc_res.raise_for_status()
                        for b in acc_res.json().get("balances", []):
                            if b["asset"] == base_asset:
                                actual_qty = float(b["free"])
                                if actual_qty > 0:
                                    _LOG.info("retrying_with_actual_balance", asset=base_asset, old_qty=executed_qty, new_qty=actual_qty)
                                    params["quantity"] = f"{actual_qty:.5f}".rstrip('0').rstrip('.')
                                    params["timestamp"] = now_ms()
                                    params.pop("signature", None)
                                    params["signature"] = _sign(params, self._secret)
                                break
                        continue # Retry immediately with new quantity
                    except Exception as e:
                        _LOG.warning("failed_to_fetch_balance_for_retry", error=str(e))

                last_error = {"error": body[:200], "status_code": code}
                if code < 500:
                    break   # 4xx — no retry (bad request / auth / etc.)
                if attempt < self.MAX_RETRIES:
                    await asyncio.sleep(2.0 ** attempt)  # 1 s, 2 s
            except Exception as exc:  # noqa: BLE001
                _LOG.warning("binance_order_error", error=str(exc), attempt=attempt)
                last_error = {"error": str(exc)}
                if attempt < self.MAX_RETRIES:
                    await asyncio.sleep(2.0 ** attempt)

        return last_error

    async def get_avg_fill_price(self, order_id: int, symbol: str) -> float:
        """Query the filled average price for a given orderId."""
        if not self._client or not order_id:
            return 0.0
        params: dict[str, Any] = {
            "symbol":    symbol,
            "orderId":   order_id,
            "timestamp": now_ms(),
            "recvWindow": 5000,
        }
        params["signature"] = _sign(params, self._secret)
        try:
            r = await self._client.get("/api/v3/order", params=params)
            self._budget.update_from_headers(r.headers)
            r.raise_for_status()
            data = r.json()
            return float(data.get("price") or data.get("cummulativeQuoteQty", 0) / max(float(data.get("executedQty", 1)), 1e-12))
        except Exception:  # noqa: BLE001
            return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Service
# ─────────────────────────────────────────────────────────────────────────────

async def _run() -> int:
    configure_logging()

    api_key    = binance_api_key()
    api_secret = binance_api_secret()
    testnet    = is_testnet()
    base       = rest_base()

    if not api_key or not api_secret:
        _LOG.warning("binance_exec_no_credentials",
                     hint="Set BINANCE_API_KEY and BINANCE_API_SECRET in .env.binance")

    _LOG.info("binance_exec_starting", base=base, testnet=testnet)

    rest = BinanceRestClient(api_key, api_secret, base)
    await rest.start()

    svc = await NatsService.connect("binance-exec")

    if _PROM_ENABLED:
        try:
            _prom_start(9303)
        except Exception:
            pass

    async def on_approved(_subject: str, data: bytes) -> None:
        try:
            sig = json.loads(data.decode("utf-8"))
        except Exception:
            return

        symbol   = sig.get("symbol", "")
        side     = sig.get("side",   "buy")
        qty_usdt = float(sig.get("qty_usdt", 50.0))
        exec_qty = float(sig.get("executed_qty", 0.0))
        cid      = sig.get("correlation_id", "")
        sl_price = float(sig.get("stop_loss_price",   0.0))
        tp_price = float(sig.get("take_profit_price", 0.0))

        t0     = time.monotonic()
        result = await rest.place_market_order(symbol, side, qty_usdt, executed_qty=exec_qty)
        lat_ms = (time.monotonic() - t0) * 1000.0

        has_error = "error" in result
        status    = "error" if has_error else "ok"
        order_id  = result.get("orderId")

        # Try to get the actual avg fill price so Risk can compute PnL
        avg_price = 0.0
        if not has_error and order_id:
            # Binance MARKET response includes cummulativeQuoteQty + executedQty
            exec_qty  = float(result.get("executedQty", 0.0) or 0)
            cum_quote = float(result.get("cummulativeQuoteQty", 0.0) or 0)
            if exec_qty > 0:
                avg_price = cum_quote / exec_qty
            if avg_price == 0.0:
                avg_price = await rest.get_avg_fill_price(order_id, symbol)

        ack = {
            "correlation_id":     cid,
            "symbol":             symbol,
            "side":               side,
            "qty_usdt":           qty_usdt,
            "status":             status,
            "binance_order_id":   order_id,
            "executed_qty":       float(result.get("executedQty", 0.0) or 0),
            "avg_price":          round(avg_price, 8),
            "stop_loss_price":    sl_price,    # ← forwarded for Risk position book
            "take_profit_price":  tp_price,    # ← forwarded for Risk position book
            "realised_pnl_usdt":  0.0,         # filled later by Risk on position close
            "latency_ms":         round(lat_ms, 2),
            "testnet":            testnet,
            "ts_ms":              now_ms(),
        }
        if has_error:
            ack["error"] = result.get("error", "unknown")

        await svc.publish(
            "crypto.order.ack",
            json.dumps(ack, separators=(",", ":")).encode("utf-8"),
        )

        if _PROM_ENABLED:
            _ORDERS_SENT.labels(symbol=symbol, side=side, status=status).inc()
            _ORDER_LATENCY.labels(symbol=symbol).observe(lat_ms)

        _LOG.info("order_sent",
                  symbol=symbol, side=side, qty_usdt=qty_usdt,
                  avg_price=round(avg_price, 4),
                  status=status, latency_ms=round(lat_ms, 1))

    await svc.subscribe("crypto.signal.approved", on_approved)
    await svc.run_forever()
    await rest.stop()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
