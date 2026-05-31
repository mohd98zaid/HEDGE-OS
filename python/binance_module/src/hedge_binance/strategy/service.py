"""Binance Strategy Engine — Multi-factor signal generation.

═══════════════════════════════════════════════════════════════════════
  STRATEGY: EMA Crossover + RSI Filter + Volume Confirmation
═══════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────┐
  │  THREE-GATE SIGNAL MODEL                                        │
  │                                                                 │
  │  Gate 1 — TREND (EMA crossover)                                │
  │    BUY:  fast EMA crosses above slow EMA (momentum bullish)     │
  │    SELL: fast EMA crosses below slow EMA (momentum bearish)     │
  │                                                                 │
  │  Gate 2 — MOMENTUM (RSI-14 filter)                             │
  │    BUY only when RSI < 65  (not overbought — room to run)       │
  │    SELL only when RSI > 35 (not oversold — room to fall)        │
  │                                                                 │
  │  Gate 3 — VOLUME (surge confirmation)                          │
  │    Trade only when volume > 1.5× the 20-tick moving average     │
  │    (avoids false breakouts on thin volume)                      │
  │                                                                 │
  │  Gate 4 — COOLDOWN                                             │
  │    Minimum 300 s between signals for the same symbol            │
  │    (avoids whipsawing on choppy markets)                        │
  │                                                                 │
  │  Gate 5 — SPREAD                                               │
  │    Skip if bid-ask spread > BINANCE_MIN_SPREAD %               │
  │    (avoids illiquid / high-slippage moments)                    │
  └─────────────────────────────────────────────────────────────────┘

  Each approved signal carries:
    • stop_loss_price  — ATR-based (1.5× ATR below/above entry)
    • take_profit_price — 2:1 R/R ratio (3× ATR from entry)
    • signal_score     — 0–100 quality score (gate confidence blend)

  These fields are passed through to the Risk Guard and Execution
  Engine so the position tracker can monitor open trades for exit
  conditions.

Environment variables
---------------------
  BINANCE_EMA_FAST    — fast EMA period (default 9)
  BINANCE_EMA_SLOW    — slow EMA period (default 21)
  BINANCE_RSI_PERIOD  — RSI period (default 14)
  BINANCE_RSI_BUY_MAX — RSI upper threshold to permit BUY (default 65)
  BINANCE_RSI_SELL_MIN— RSI lower threshold to permit SELL (default 35)
  BINANCE_ATR_PERIOD  — ATR period for SL/TP sizing (default 14)
  BINANCE_ATR_SL_MULT — SL = entry ± (ATR × mult) (default 1.5)
  BINANCE_ATR_TP_MULT — TP = entry ± (ATR × mult) (default 3.0)
  BINANCE_VOL_SURGE   — volume must be > avg × this (default 1.5)
  BINANCE_COOLDOWN_S  — min seconds between signals per symbol (300)
  BINANCE_QTY_USDT    — notional per trade in USDT (default 50)
  BINANCE_MIN_SPREAD  — max bid-ask spread % (default 0.05)

NATS subjects
-------------
  Subscribe:  crypto.tick.*
  Publish:    crypto.signal
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Deque, Dict, List, Optional, Sequence

import structlog

from ..runtime import NatsService, configure_logging, symbols_from_env

_LOG = structlog.get_logger(__name__)

try:
    from prometheus_client import Counter, Gauge, start_http_server as _prom_start
    _SIGNALS_EMITTED = Counter(
        "binance_strategy_signals_total", "Signals emitted", ["symbol", "side"]
    )
    _SIGNAL_SCORE = Gauge(
        "binance_strategy_signal_score", "Latest signal quality score 0-100", ["symbol"]
    )
    _PROM_ENABLED = True
except Exception:
    _PROM_ENABLED = False


# ─────────────────────────────────────────────────────────────────────────────
# Technical Indicators
# ─────────────────────────────────────────────────────────────────────────────

class EMA:
    """Exponential moving average — standard formula."""

    def __init__(self, period: int) -> None:
        self._k = 2.0 / (period + 1)
        self._period = period
        self._value: Optional[float] = None
        self._count = 0

    @property
    def value(self) -> Optional[float]:
        return self._value if self._count >= self._period else None

    def update(self, price: float) -> Optional[float]:
        self._count += 1
        if self._value is None:
            self._value = price
        else:
            self._value = price * self._k + self._value * (1.0 - self._k)
        return self.value


class RSI:
    """Wilder's RSI-N.  Returns None until the warm-up window is filled."""

    def __init__(self, period: int) -> None:
        self._period = period
        self._gains: Deque[float] = deque(maxlen=period)
        self._losses: Deque[float] = deque(maxlen=period)
        self._prev: Optional[float] = None
        self._avg_gain: Optional[float] = None
        self._avg_loss: Optional[float] = None
        self._count = 0

    @property
    def value(self) -> Optional[float]:
        if self._avg_gain is None or self._avg_loss is None:
            return None
        if self._avg_loss == 0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def update(self, price: float) -> Optional[float]:
        if self._prev is None:
            self._prev = price
            return None
        delta = price - self._prev
        self._prev = price
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        self._count += 1
        if self._count <= self._period:
            self._gains.append(gain)
            self._losses.append(loss)
            if self._count == self._period:
                self._avg_gain = sum(self._gains) / self._period
                self._avg_loss = sum(self._losses) / self._period
        else:
            # Wilder's smoothing
            assert self._avg_gain is not None
            assert self._avg_loss is not None
            self._avg_gain = (self._avg_gain * (self._period - 1) + gain) / self._period
            self._avg_loss = (self._avg_loss * (self._period - 1) + loss) / self._period
        return self.value


class ATR:
    """Average True Range — measures volatility for dynamic SL/TP sizing."""

    def __init__(self, period: int) -> None:
        self._period = period
        self._prev_close: Optional[float] = None
        self._tr_buf: Deque[float] = deque(maxlen=period)
        self._atr: Optional[float] = None

    @property
    def value(self) -> Optional[float]:
        return self._atr

    def update(self, high: float, low: float, close: float) -> Optional[float]:
        if self._prev_close is None:
            self._prev_close = close
            return None
        tr = max(
            high - low,
            abs(high - self._prev_close),
            abs(low - self._prev_close),
        )
        self._prev_close = close
        self._tr_buf.append(tr)
        if len(self._tr_buf) == self._period:
            if self._atr is None:
                self._atr = sum(self._tr_buf) / self._period
            else:
                self._atr = (self._atr * (self._period - 1) + tr) / self._period
        return self._atr


class VolumeMA:
    """Simple moving average of volume for surge detection."""

    def __init__(self, period: int) -> None:
        self._buf: Deque[float] = deque(maxlen=period)

    @property
    def avg(self) -> Optional[float]:
        if len(self._buf) < self._buf.maxlen:  # type: ignore[arg-type]
            return None
        return sum(self._buf) / len(self._buf)

    def update(self, volume: float) -> None:
        self._buf.append(volume)


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol state machine
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SymbolState:
    symbol: str
    fast: EMA
    slow: EMA
    rsi: RSI
    atr: ATR
    vol_ma: VolumeMA

    # Gate configuration (filled at construction)
    rsi_buy_max: float = 65.0
    rsi_sell_min: float = 35.0
    atr_sl_mult: float = 1.5
    atr_tp_mult: float = 3.0
    vol_surge: float = 1.5
    min_spread_pct: float = 0.05
    cooldown_s: float = 300.0

    # Runtime state
    prev_fast: Optional[float] = field(default=None, repr=False)
    prev_slow: Optional[float] = field(default=None, repr=False)
    last_signal_ts: float = field(default=0.0, repr=False)
    last_price: float = field(default=0.0, repr=False)

    @classmethod
    def create(cls, symbol: str, cfg: "StrategyConfig") -> "SymbolState":
        return cls(
            symbol=symbol,
            fast=EMA(cfg.ema_fast),
            slow=EMA(cfg.ema_slow),
            rsi=RSI(cfg.rsi_period),
            atr=ATR(cfg.atr_period),
            vol_ma=VolumeMA(20),
            rsi_buy_max=cfg.rsi_buy_max,
            rsi_sell_min=cfg.rsi_sell_min,
            atr_sl_mult=cfg.atr_sl_mult,
            atr_tp_mult=cfg.atr_tp_mult,
            vol_surge=cfg.vol_surge,
            min_spread_pct=cfg.min_spread_pct,
            cooldown_s=cfg.cooldown_s,
        )

    def on_tick(
        self,
        price: float,
        bid: float,
        ask: float,
        volume: float,
        ts_s: float,
    ) -> Optional[dict]:
        """
        Process one tick.  Returns a signal dict or None.
        All five gates must pass before a signal is emitted.
        """
        self.last_price = price

        # ── Gate 5: Spread ────────────────────────────────────────────
        if bid > 0 and ask > 0:
            spread_pct = (ask - bid) / bid * 100.0
            if spread_pct > self.min_spread_pct:
                return None

        # ── Update indicators ─────────────────────────────────────────
        fast_val = self.fast.update(price)
        slow_val = self.slow.update(price)
        rsi_val  = self.rsi.update(price)
        # ATR: use price as proxy for high/low/close (tick data only)
        atr_val  = self.atr.update(price * 1.001, price * 0.999, price)
        self.vol_ma.update(volume)
        vol_avg = self.vol_ma.avg

        # ── Gate 1: Trend — EMA crossover ────────────────────────────
        if fast_val is None or slow_val is None:
            self.prev_fast = fast_val
            self.prev_slow = slow_val
            return None

        was_above = (self.prev_fast or 0) > (self.prev_slow or 0)
        is_above  = fast_val > slow_val
        self.prev_fast = fast_val
        self.prev_slow = slow_val

        if not (not was_above and is_above):
            return None  # Long-only strategy: ignore bearish crossovers
        
        side = "buy"

        # ── Gate 2: RSI momentum filter ───────────────────────────────
        if rsi_val is not None:
            if side == "buy"  and rsi_val >= self.rsi_buy_max:
                return None   # overbought — skip BUY
            if side == "sell" and rsi_val <= self.rsi_sell_min:
                return None   # oversold   — skip SELL

        # ── Gate 3: Volume surge confirmation ─────────────────────────
        if vol_avg is not None and vol_avg > 0:
            if volume < vol_avg * self.vol_surge:
                return None   # insufficient volume — likely false breakout

        # ── Gate 4: Cooldown ──────────────────────────────────────────
        if ts_s - self.last_signal_ts < self.cooldown_s:
            return None

        # ── All gates passed — build signal payload ───────────────────
        self.last_signal_ts = ts_s

        # ATR-based SL / TP
        atr = atr_val if atr_val else price * 0.005  # fallback: 0.5% of price
        if side == "buy":
            sl_price = round(price - atr * self.atr_sl_mult, 8)
            tp_price = round(price + atr * self.atr_tp_mult, 8)
        else:
            sl_price = round(price + atr * self.atr_sl_mult, 8)
            tp_price = round(price - atr * self.atr_tp_mult, 8)

        # Signal quality score (0–100)
        score = self._compute_score(side, rsi_val, volume, vol_avg, fast_val, slow_val, atr)

        return {
            "side":              side,
            "price":             price,
            "stop_loss_price":   sl_price,
            "take_profit_price": tp_price,
            "atr":               round(atr, 8),
            "rsi":               round(rsi_val, 2) if rsi_val else None,
            "volume":            volume,
            "signal_score":      score,
        }

    def _compute_score(
        self,
        side: str,
        rsi: Optional[float],
        volume: float,
        vol_avg: Optional[float],
        fast: float,
        slow: float,
    ) -> int:
        """Blend indicator readings into a 0–100 quality score."""
        # ── EMA separation (wider = stronger trend) ───────────────────
        spread_score = min(abs(fast - slow) / slow * 5000, 40)  # max 40 pts

        # ── RSI score (further from extreme = better entry) ───────────
        rsi_score = 0.0
        if rsi is not None:
            if side == "buy":
                # Best RSI for BUY: 40–55 (strong but not overbought)
                rsi_score = max(0, 30 - abs(rsi - 50) * 0.8)
            else:
                # Best RSI for SELL: 45–60
                rsi_score = max(0, 30 - abs(rsi - 50) * 0.8)

        # ── Volume score (higher relative volume = better) ────────────
        vol_score = 0.0
        if vol_avg and vol_avg > 0:
            ratio = volume / vol_avg
            vol_score = min((ratio - 1.0) * 15, 30)  # max 30 pts

        return max(0, min(100, int(spread_score + rsi_score + vol_score)))

    def _compute_score(  # noqa: F811 — override with correct signature
        self,
        side: str,
        rsi: Optional[float],
        volume: float,
        vol_avg: Optional[float],
        fast: float,
        slow: float,
        atr: float,
    ) -> int:
        spread_score = min(abs(fast - slow) / slow * 5000, 40)
        rsi_score = 0.0
        if rsi is not None:
            distance_from_neutral = abs(rsi - 50)
            rsi_score = max(0, 30.0 - distance_from_neutral * 0.6)
        vol_score = 0.0
        if vol_avg and vol_avg > 0:
            vol_score = min((volume / vol_avg - 1.0) * 15, 30)
        return max(0, min(100, int(spread_score + rsi_score + vol_score)))


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StrategyConfig:
    ema_fast:       int   = 9
    ema_slow:       int   = 21
    rsi_period:     int   = 14
    rsi_buy_max:    float = 65.0
    rsi_sell_min:   float = 35.0
    atr_period:     int   = 14
    atr_sl_mult:    float = 1.5   # stop-loss = entry ± 1.5 × ATR
    atr_tp_mult:    float = 3.0   # take-profit = entry ± 3.0 × ATR  (2:1 R/R)
    vol_surge:      float = 1.5   # volume must be 1.5× the 20-tick avg
    cooldown_s:     float = 300.0 # 5 min between signals per symbol
    qty_usdt:       float = 50.0
    min_spread_pct: float = 0.05

    @classmethod
    def from_env(cls) -> "StrategyConfig":
        return cls(
            ema_fast       = int(os.environ.get("BINANCE_EMA_FAST",       "9")),
            ema_slow       = int(os.environ.get("BINANCE_EMA_SLOW",       "21")),
            rsi_period     = int(os.environ.get("BINANCE_RSI_PERIOD",     "14")),
            rsi_buy_max    = float(os.environ.get("BINANCE_RSI_BUY_MAX",  "65")),
            rsi_sell_min   = float(os.environ.get("BINANCE_RSI_SELL_MIN", "35")),
            atr_period     = int(os.environ.get("BINANCE_ATR_PERIOD",     "14")),
            atr_sl_mult    = float(os.environ.get("BINANCE_ATR_SL_MULT",  "1.5")),
            atr_tp_mult    = float(os.environ.get("BINANCE_ATR_TP_MULT",  "3.0")),
            vol_surge      = float(os.environ.get("BINANCE_VOL_SURGE",    "1.5")),
            cooldown_s     = float(os.environ.get("BINANCE_COOLDOWN_S",   "300")),
            qty_usdt       = float(os.environ.get("BINANCE_QTY_USDT",     "50")),
            min_spread_pct = float(os.environ.get("BINANCE_MIN_SPREAD",   "0.05")),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Service
# ─────────────────────────────────────────────────────────────────────────────

async def _run() -> int:
    configure_logging()
    cfg = StrategyConfig.from_env()
    symbols = symbols_from_env()

    _LOG.info(
        "binance_strategy_starting",
        symbols=list(symbols),
        ema=f"{cfg.ema_fast}/{cfg.ema_slow}",
        rsi_period=cfg.rsi_period,
        rsi_gates=f"buy<{cfg.rsi_buy_max} sell>{cfg.rsi_sell_min}",
        atr=f"SL×{cfg.atr_sl_mult} TP×{cfg.atr_tp_mult}",
        vol_surge=f">{cfg.vol_surge}×avg",
        cooldown_s=cfg.cooldown_s,
        qty_usdt=cfg.qty_usdt,
    )

    states: Dict[str, SymbolState] = {
        sym: SymbolState.create(sym, cfg) for sym in symbols
    }
    svc = await NatsService.connect("binance-strategy")

    if _PROM_ENABLED:
        try:
            _prom_start(9302)
        except Exception:
            pass

    async def on_tick(_subject: str, data: bytes) -> None:
        try:
            tick = json.loads(data.decode("utf-8"))
        except Exception:
            return

        symbol = tick.get("symbol", "")
        state  = states.get(symbol)
        if state is None:
            return

        price  = float(tick.get("price",  0.0))
        bid    = float(tick.get("bid",    price))
        ask    = float(tick.get("ask",    price))
        volume = float(tick.get("volume_24h", 0.0))
        if price <= 0:
            return

        result = state.on_tick(price, bid, ask, volume, time.time())
        if result is None:
            return

        cid = str(uuid.uuid4())
        signal = {
            "correlation_id":    cid,
            "symbol":            symbol,
            "side":              result["side"],
            "price":             result["price"],
            "stop_loss_price":   result["stop_loss_price"],
            "take_profit_price": result["take_profit_price"],
            "atr":               result["atr"],
            "rsi":               result["rsi"],
            "signal_score":      result["signal_score"],
            "qty_usdt":          cfg.qty_usdt,
            "strategy":          f"ema{cfg.ema_fast}/{cfg.ema_slow}+rsi{cfg.rsi_period}+atr",
            "ts_ms":             int(time.time() * 1000),
        }

        await svc.publish(
            "crypto.signal",
            json.dumps(signal, separators=(",", ":")).encode("utf-8"),
        )

        if _PROM_ENABLED:
            _SIGNALS_EMITTED.labels(symbol=symbol, side=result["side"]).inc()
            _SIGNAL_SCORE.labels(symbol=symbol).set(result["signal_score"])

        _LOG.info(
            "signal_emitted",
            symbol=symbol,
            side=result["side"],
            price=price,
            sl=result["stop_loss_price"],
            tp=result["take_profit_price"],
            rsi=result["rsi"],
            score=result["signal_score"],
            cid=cid[:8],
        )

    await svc.subscribe("crypto.tick.*", on_tick)
    await svc.run_forever()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
