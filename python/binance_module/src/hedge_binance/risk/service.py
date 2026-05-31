"""Binance Risk Guard — hardened pre-trade + open-position risk engine.

═══════════════════════════════════════════════════════════════════════
  WHAT WAS FIXED vs v1
═══════════════════════════════════════════════════════════════════════
  ✅ Stop-loss / take-profit monitoring via crypto.tick.*
     (subscribes to live ticks, closes positions automatically)
  ✅ Consecutive-loss circuit breaker
     (pauses trading after BINANCE_MAX_CONSEC_LOSSES consecutive losses)
  ✅ Max daily trades limit (BINANCE_MAX_DAILY_TRADES)
  ✅ Per-symbol position book with entry price, SL, TP
  ✅ Proper PnL estimation from MARKET fill prices
  ✅ Rich heartbeat now includes position book + circuit-breaker state

NATS subjects
─────────────
  Subscribe:  crypto.signal          — raw signals from strategy
              crypto.order.ack       — fills from execution engine
              crypto.tick.*          — live ticks for SL/TP monitoring
  Publish:    crypto.signal.approved
              crypto.signal.rejected
              crypto.signal          (SL/TP-triggered EXIT signals)
              crypto.risk.status     (heartbeat every 5 s)

Environment variables
─────────────────────
  BINANCE_MAX_POSITION_USDT    — max notional per symbol (default 500)
  BINANCE_DAILY_LOSS_LIMIT     — halt when PnL < -limit  (default 100)
  BINANCE_MAX_OPEN_ORDERS      — max concurrent fills     (default 5)
  BINANCE_MAX_DAILY_TRADES     — max fills per day        (default 20)
  BINANCE_MAX_CONSEC_LOSSES    — circuit-breaker threshold (default 3)
  BINANCE_MIN_SIGNAL_SCORE     — reject signals below score (default 30)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence

import structlog

from ..runtime import NatsService, configure_logging

_LOG = structlog.get_logger(__name__)

try:
    from prometheus_client import Counter, Gauge, start_http_server as _prom_start
    _SIG_APPROVED   = Counter("binance_risk_approved_total",  "Signals approved",        ["symbol"])
    _SIG_REJECTED   = Counter("binance_risk_rejected_total",  "Signals rejected",        ["reason"])
    _DAILY_PNL      = Gauge(  "binance_risk_daily_pnl_usdt",  "Estimated daily PnL USDT")
    _CONSEC_LOSSES  = Gauge(  "binance_risk_consec_losses",   "Consecutive losing trades")
    _DAILY_TRADES   = Gauge(  "binance_risk_daily_trades",    "Trades executed today")
    _PROM_ENABLED   = True
except Exception:
    _PROM_ENABLED = False


# ─────────────────────────────────────────────────────────────────────────────
# Open-position book entry
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OpenPosition:
    """Tracks one live position until SL or TP is hit (or manual close)."""
    symbol:            str
    side:              str    # "buy" or "sell"
    entry_price:       float
    qty_usdt:          float
    stop_loss_price:   float
    take_profit_price: float
    opened_at:         float = field(default_factory=time.time)
    order_id:          Optional[str] = None
    executed_qty:      float = 0.0

    def pnl_usdt(self, current_price: float) -> float:
        """Unrealised PnL estimate (USDT)."""
        if self.entry_price <= 0:
            return 0.0
        pct = (current_price - self.entry_price) / self.entry_price
        if self.side == "sell":
            pct = -pct
        return round(self.qty_usdt * pct, 4)

    def sl_hit(self, price: float) -> bool:
        if self.side == "buy":
            return price <= self.stop_loss_price
        return price >= self.stop_loss_price   # short position

    def tp_hit(self, price: float) -> bool:
        if self.side == "buy":
            return price >= self.take_profit_price
        return price <= self.take_profit_price  # short position


# ─────────────────────────────────────────────────────────────────────────────
# Risk state
# ─────────────────────────────────────────────────────────────────────────────

class RiskState:
    """
    Single-threaded (asyncio) risk ledger — no locks needed.
    Manages pre-trade gates AND open-position SL/TP monitoring.
    """

    def __init__(self) -> None:
        # ── Limits from env ──────────────────────────────────────────
        self.max_position_usdt:  float = float(os.environ.get("BINANCE_MAX_POSITION_USDT",  "500"))
        self.daily_loss_limit:   float = float(os.environ.get("BINANCE_DAILY_LOSS_LIMIT",   "100"))
        self.max_open_orders:    int   = int(  os.environ.get("BINANCE_MAX_OPEN_ORDERS",     "5"))
        self.max_daily_trades:   int   = int(  os.environ.get("BINANCE_MAX_DAILY_TRADES",    "20"))
        self.max_consec_losses:  int   = int(  os.environ.get("BINANCE_MAX_CONSEC_LOSSES",   "3"))
        self.min_signal_score:   int   = int(  os.environ.get("BINANCE_MIN_SIGNAL_SCORE",    "30"))

        # ── Runtime counters ─────────────────────────────────────────
        self.open_orders:        int   = 0
        self.daily_pnl:          float = 0.0
        self.daily_trades:       int   = 0
        self.consec_losses:      int   = 0
        self.day_epoch:          int   = self._today()

        # ── Position book: symbol → OpenPosition ─────────────────────
        self.positions: Dict[str, OpenPosition] = {}

        # ── Circuit-breaker reset (ISO date of last reset) ───────────
        self._paused_until: float = 0.0   # epoch-s; 0 = not paused

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _today() -> int:
        import datetime
        return datetime.date.today().toordinal()

    def _reset_day_if_needed(self) -> None:
        today = self._today()
        if today != self.day_epoch:
            self.daily_pnl    = 0.0
            self.daily_trades = 0
            self.consec_losses = 0
            self._paused_until = 0.0
            self.day_epoch    = today
            _LOG.info("risk_daily_reset")

    # ── Pre-trade gate ────────────────────────────────────────────────────────

    def check(self, sig: dict) -> tuple[bool, str]:
        """Return (approved, reject_reason). Empty reason = approved."""
        self._reset_day_if_needed()

        symbol    = sig.get("symbol", "")
        qty_usdt  = float(sig.get("qty_usdt", 0.0))
        score     = int(sig.get("signal_score", 100))

        # Circuit-breaker (cooldown after consecutive losses)
        if time.time() < self._paused_until:
            remaining = int(self._paused_until - time.time())
            return False, f"circuit_breaker_{remaining}s"

        # Daily loss cap
        if self.daily_pnl <= -self.daily_loss_limit:
            return False, "daily_loss_limit_hit"

        # Daily trade cap
        if self.daily_trades >= self.max_daily_trades:
            return False, "daily_trade_limit_hit"

        # Open orders cap
        if self.open_orders >= self.max_open_orders:
            return False, "max_open_orders_reached"

        # Signal quality gate
        if score < self.min_signal_score:
            return False, f"signal_score_too_low_{score}"

        # Per-symbol position limit
        existing = self.positions.get(symbol)
        if existing:
            return False, f"position_already_open_{symbol}"

        # Notional limit
        if qty_usdt > self.max_position_usdt:
            return False, f"notional_too_large_{qty_usdt}"

        return True, ""

    def on_approved(self, sig: dict) -> None:
        symbol   = sig.get("symbol", "")
        qty_usdt = float(sig.get("qty_usdt", 50.0))
        self.open_orders += 1
        _LOG.debug("risk_position_reserved", symbol=symbol, qty=qty_usdt)

    def on_fill(self, ack: dict) -> None:
        """Called when a fill arrives from crypto.order.ack."""
        self._reset_day_if_needed()
        symbol   = ack.get("symbol",        "")
        side     = ack.get("side",          "buy")
        qty_usdt = float(ack.get("qty_usdt", 0.0))
        exec_qty = float(ack.get("executed_qty", 0.0))
        entry_p  = float(ack.get("avg_price", 0.0))
        sl       = float(ack.get("stop_loss_price",   0.0))
        tp       = float(ack.get("take_profit_price", 0.0))
        order_id = str(ack.get("binance_order_id", ""))
        status   = ack.get("status", "ok")

        self.open_orders = max(0, self.open_orders - 1)
        self.daily_trades += 1

        if status != "ok":
            _LOG.warning("fill_error_skipped", symbol=symbol, ack=ack)
            return

        if entry_p > 0 and sl > 0 and tp > 0:
            self.positions[symbol] = OpenPosition(
                symbol=symbol, side=side,
                entry_price=entry_p, qty_usdt=qty_usdt,
                executed_qty=exec_qty,
                stop_loss_price=sl, take_profit_price=tp,
                order_id=order_id,
            )
            _LOG.info("position_opened", symbol=symbol, side=side,
                      entry=entry_p, sl=sl, tp=tp)

        if _PROM_ENABLED:
            _DAILY_TRADES.set(self.daily_trades)

    def on_position_closed(self, symbol: str, realised_pnl: float, reason: str) -> None:
        """Remove position from book and update PnL + circuit-breaker."""
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return

        self.daily_pnl = round(self.daily_pnl + realised_pnl, 4)

        if realised_pnl >= 0:
            self.consec_losses = 0
            _LOG.info("position_closed_profit", symbol=symbol,
                      pnl=realised_pnl, reason=reason)
        else:
            self.consec_losses += 1
            _LOG.warning("position_closed_loss", symbol=symbol,
                         pnl=realised_pnl, consec=self.consec_losses, reason=reason)
            if self.consec_losses >= self.max_consec_losses:
                pause_s = 900  # 15-minute cooling-off after N consecutive losses
                self._paused_until = time.time() + pause_s
                _LOG.warning("circuit_breaker_tripped",
                             consec=self.consec_losses, pause_s=pause_s)

        if _PROM_ENABLED:
            _DAILY_PNL.set(self.daily_pnl)
            _CONSEC_LOSSES.set(self.consec_losses)

    # ── SL/TP tick monitor ────────────────────────────────────────────────────

    def check_sltp(self, symbol: str, price: float) -> Optional[dict]:
        """
        Called on every tick for symbols with open positions.
        Returns a close-signal dict if SL or TP has been breached, else None.
        """
        pos = self.positions.get(symbol)
        if pos is None:
            return None

        triggered = None
        if pos.sl_hit(price):
            triggered = ("sell" if pos.side == "buy" else "buy", "stop_loss")
        elif pos.tp_hit(price):
            triggered = ("sell" if pos.side == "buy" else "buy", "take_profit")

        if triggered is None:
            return None

        close_side, reason = triggered
        realised_pnl = pos.pnl_usdt(price)
        self.on_position_closed(symbol, realised_pnl, reason)

        return {
            "correlation_id":  str(uuid.uuid4()),
            "symbol":          symbol,
            "side":            close_side,
            "price":           price,
            "qty_usdt":        pos.qty_usdt,
            "executed_qty":    pos.executed_qty,
            "stop_loss_price": 0.0,   # closing order has no SL
            "take_profit_price": 0.0,
            "signal_score":    100,   # exit orders always approved
            "strategy":        reason,
            "realised_pnl_usdt": realised_pnl,
            "ts_ms":           int(time.time() * 1000),
        }

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def heartbeat(self) -> bytes:
        self._reset_day_if_needed()
        pos_summary = {
            sym: {
                "side":  p.side,
                "entry": p.entry_price,
                "sl":    p.stop_loss_price,
                "tp":    p.take_profit_price,
                "usdt":  p.qty_usdt,
            }
            for sym, p in self.positions.items()
        }
        payload = {
            "open_orders":       self.open_orders,
            "daily_pnl_usdt":    self.daily_pnl,
            "daily_trades":      self.daily_trades,
            "consec_losses":     self.consec_losses,
            "paused":            time.time() < self._paused_until,
            "paused_until_s":    max(0, int(self._paused_until - time.time())),
            "positions":         pos_summary,
            "daily_loss_limit":  self.daily_loss_limit,
            "max_daily_trades":  self.max_daily_trades,
            "circuit_breaker_at": self.max_consec_losses,
            "ts_ms":             int(time.time() * 1000),
        }
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Service entry point
# ─────────────────────────────────────────────────────────────────────────────

async def _run() -> int:
    configure_logging()
    _LOG.info("binance_risk_starting")

    state = RiskState()
    svc   = await NatsService.connect("binance-risk")

    if _PROM_ENABLED:
        try:
            _prom_start(9301)
        except Exception:
            pass

    # ── Handler: incoming raw signal from strategy ────────────────────────────
    async def on_signal(_subject: str, data: bytes) -> None:
        try:
            sig = json.loads(data.decode("utf-8"))
        except Exception:
            return

        symbol = sig.get("symbol", "")
        approved, reason = state.check(sig)

        if approved:
            state.on_approved(sig)
            await svc.publish("crypto.signal.approved",
                              json.dumps(sig, separators=(",", ":")).encode("utf-8"))
            if _PROM_ENABLED:
                _SIG_APPROVED.labels(symbol=symbol).inc()
            _LOG.info("signal_approved", symbol=symbol,
                      score=sig.get("signal_score"), side=sig.get("side"))
        else:
            sig["reject_reason"] = reason
            await svc.publish("crypto.signal.rejected",
                              json.dumps(sig, separators=(",", ":")).encode("utf-8"))
            if _PROM_ENABLED:
                _SIG_REJECTED.labels(reason=reason).inc()
            _LOG.info("signal_rejected", symbol=symbol, reason=reason)

    # ── Handler: order fill acknowledgement ───────────────────────────────────
    async def on_order_ack(_subject: str, data: bytes) -> None:
        try:
            ack = json.loads(data.decode("utf-8"))
        except Exception:
            return
        state.on_fill(ack)

    # ── Handler: live tick — SL/TP monitor ───────────────────────────────────
    async def on_tick(_subject: str, data: bytes) -> None:
        if not state.positions:
            return
        try:
            tick = json.loads(data.decode("utf-8"))
        except Exception:
            return

        symbol = tick.get("symbol", "")
        price  = float(tick.get("price", 0.0))
        if price <= 0 or symbol not in state.positions:
            return

        exit_signal = state.check_sltp(symbol, price)
        if exit_signal is None:
            return

        _LOG.info("sltp_exit_triggered",
                  symbol=symbol, price=price, reason=exit_signal["strategy"],
                  pnl=exit_signal["realised_pnl_usdt"])

        # Publish as a pre-approved signal (bypasses signal gate — it's a close)
        await svc.publish(
            "crypto.signal.approved",
            json.dumps(exit_signal, separators=(",", ":")).encode("utf-8"),
        )

    # ── Heartbeat loop ────────────────────────────────────────────────────────
    async def _heartbeat(stop: asyncio.Event) -> None:
        while not stop.is_set():
            await svc.publish("crypto.risk.status", state.heartbeat())
            try:
                await asyncio.wait_for(stop.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

    await svc.subscribe("crypto.signal",    on_signal)
    await svc.subscribe("crypto.order.ack", on_order_ack)
    await svc.subscribe("crypto.tick.*",    on_tick)
    await svc.run_until(_heartbeat)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
