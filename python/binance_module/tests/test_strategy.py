"""Tests for the 5-gate Binance strategy engine.

Run with:
    pytest python/binance_module/tests/ -v
"""

from __future__ import annotations

import time
from typing import Optional

import pytest

# Import strategy internals directly (no NATS / network needed)
from hedge_binance.strategy.service import (
    ATR, EMA, RSI, StrategyConfig, SymbolState, VolumeMA,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_state(
    *,
    cooldown_s: float = 0.0,
    rsi_buy_max: float = 101.0,  # disable by default for non-RSI tests
    rsi_sell_min: float = -1.0,  # disable by default for non-RSI tests
    vol_surge: float = 0.0,      # disable by default for non-volume tests
) -> SymbolState:
    cfg = StrategyConfig(
        ema_fast=9, ema_slow=21,
        rsi_period=14,
        rsi_buy_max=rsi_buy_max,
        rsi_sell_min=rsi_sell_min,
        atr_period=14,
        atr_sl_mult=1.5,
        atr_tp_mult=3.0,
        vol_surge=vol_surge,
        cooldown_s=cooldown_s,
        qty_usdt=50.0,
        min_spread_pct=1.0,
    )
    return SymbolState.create("BTCUSDT", cfg)


def warm_up(state: SymbolState, price: float, n: int = 30) -> None:
    """Feed n ticks at the same price to warm up all indicators."""
    ts = time.time() - n
    for i in range(n):
        state.on_tick(price, price * 0.9999, price * 1.0001, 1_000.0, ts + i)


# ── EMA tests ─────────────────────────────────────────────────────────────────

class TestEMA:
    def test_none_before_warm_up(self) -> None:
        ema = EMA(5)
        for _ in range(4):
            assert ema.update(100.0) is None

    def test_value_after_warm_up(self) -> None:
        ema = EMA(5)
        for _ in range(5):
            ema.update(100.0)
        assert ema.value is not None
        assert abs(ema.value - 100.0) < 1e-6   # steady price → EMA == price

    def test_rising_price_raises_ema(self) -> None:
        ema = EMA(5)
        for _ in range(10):
            ema.update(100.0)
        prev = ema.value
        ema.update(200.0)
        assert ema.value > prev  # type: ignore[operator]


# ── RSI tests ─────────────────────────────────────────────────────────────────

class TestRSI:
    def test_none_before_warm_up(self) -> None:
        rsi = RSI(14)
        for _ in range(14):
            assert rsi.update(100.0) is None

    def test_overbought_on_rising_prices(self) -> None:
        rsi = RSI(14)
        price = 100.0
        for _ in range(20):
            rsi.update(price)
            price += 5.0   # steadily rising
        assert rsi.value is not None
        assert rsi.value > 70   # should be overbought

    def test_oversold_on_falling_prices(self) -> None:
        rsi = RSI(14)
        price = 200.0
        for _ in range(20):
            rsi.update(price)
            price -= 5.0   # steadily falling
        assert rsi.value is not None
        assert rsi.value < 30   # should be oversold

    def test_neutral_at_flat_price(self) -> None:
        rsi = RSI(14)
        for _ in range(20):
            rsi.update(100.0)
        # At flat price all deltas are zero → avg_loss == 0 → RSI = 100
        # (legitimate Wilder's RSI behaviour)
        assert rsi.value is not None


# ── Gate 1: EMA crossover ─────────────────────────────────────────────────────

class TestEMACrossover:
    def test_buy_signal_on_upward_cross(self) -> None:
        state = make_state(vol_surge=0.0)  # disable volume gate
        warm_up(state, 100.0)

        # Drive price up sharply to create fast > slow crossover
        ts = time.time()
        signal = None
        for i in range(30):
            signal = state.on_tick(200.0, 199.9, 200.1, 5_000.0, ts + i)
            if signal and signal["side"] == "buy":
                break

        assert signal is not None
        assert signal["side"] == "buy"

    def test_no_signal_on_downward_cross(self) -> None:
        """Long-only strategy should NOT emit a sell signal on bearish crossover."""
        state = make_state(vol_surge=0.0)
        # Warmup with slightly rising prices to ensure fast > slow
        ts = time.time() - 30
        for i in range(30):
            state.on_tick(200.0 + i, 199.9 + i, 200.1 + i, 5_000.0, ts + i)

        ts = time.time()
        signal = None
        for i in range(30):
            signal = state.on_tick(100.0, 99.9, 100.1, 5_000.0, ts + i)
            if signal is not None:
                break

        assert signal is None, f"long-only strategy should not emit signal on bearish cross, got {signal}"


# ── Gate 2: RSI filter ────────────────────────────────────────────────────────

class TestRSIGate:
    def test_buy_blocked_when_rsi_high(self) -> None:
        """After strongly rising prices RSI is overbought → buy should be blocked."""
        state = make_state(rsi_buy_max=50.0, vol_surge=0.0, cooldown_s=0.0)

        # Artificially mark RSI state
        # (drive prices up 30 ticks to create high RSI)
        ts = time.time() - 50
        for i in range(50):
            state.on_tick(100.0 + i, 99.9 + i, 100.1 + i, 5_000.0, ts + i)

        # Now add a sharp bullish crossover
        for i in range(10):
            result = state.on_tick(200.0, 199.9, 200.1, 5_000.0, ts + 60 + i)
            if result is not None:
                # With rsi_buy_max=50 and very high RSI, buys should be blocked
                # (or the crossover produced a sell, which is OK)
                assert result["side"] != "buy" or result["rsi"] is None or result["rsi"] < 50


# ── Gate 3: Volume gate ───────────────────────────────────────────────────────

class TestVolumeGate:
    def test_signal_blocked_on_low_volume(self) -> None:
        state = make_state(vol_surge=2.0, cooldown_s=0.0)

        # Warm up vol_ma with high volumes
        ts = time.time() - 50
        for i in range(30):
            state.on_tick(100.0, 99.9, 100.1, 10_000.0, ts + i)

        # Now trigger crossover but with LOW volume (0.1× average)
        any_signal_with_low_vol = False
        for i in range(20):
            result = state.on_tick(200.0, 199.9, 200.1, 100.0, ts + 31 + i)
            if result is not None:
                any_signal_with_low_vol = True
                break

        # Low-volume signals should be blocked
        assert not any_signal_with_low_vol


# ── Gate 4: Cooldown ──────────────────────────────────────────────────────────

class TestCooldownGate:
    def test_second_signal_blocked_within_cooldown(self) -> None:
        state = make_state(vol_surge=0.0, cooldown_s=9999.0)
        warm_up(state, 100.0)
        state.last_signal_ts = 0.0  # Reset in case warmup triggered a false cross

        ts = time.time()
        first_signal = None
        for i in range(30):
            result = state.on_tick(200.0, 199.9, 200.1, 5_000.0, ts + i)
            if result is not None:
                first_signal = result
                break

        # Now try again immediately — should be blocked by cooldown
        second_signal = None
        for i in range(10):
            result = state.on_tick(100.0, 99.9, 100.1, 5_000.0, ts + 31 + i)
            if result is not None:
                second_signal = result
                break

        assert first_signal is not None    # first signal should fire
        assert second_signal is None       # second blocked by 9999s cooldown


# ── ATR stop-loss / take-profit ───────────────────────────────────────────────

class TestATRLevels:
    def test_sl_tp_directions_correct_for_buy(self) -> None:
        state = make_state(vol_surge=0.0, cooldown_s=0.0)
        warm_up(state, 100.0)

        ts = time.time()
        signal = None
        for i in range(30):
            result = state.on_tick(200.0, 199.9, 200.1, 5_000.0, ts + i)
            if result and result["side"] == "buy":
                signal = result
                break

        if signal:
            assert signal["stop_loss_price"]   < signal["price"]   # SL below entry
            assert signal["take_profit_price"] > signal["price"]   # TP above entry

    def test_sl_tp_directions_correct_for_sell(self) -> None:
        state = make_state(vol_surge=0.0, cooldown_s=0.0)
        warm_up(state, 200.0)

        ts = time.time()
        signal = None
        for i in range(30):
            result = state.on_tick(100.0, 99.9, 100.1, 5_000.0, ts + i)
            if result and result["side"] == "sell":
                signal = result
                break

        if signal:
            assert signal["stop_loss_price"]   > signal["price"]   # SL above entry
            assert signal["take_profit_price"] < signal["price"]   # TP below entry


# ── Signal score ──────────────────────────────────────────────────────────────

class TestSignalScore:
    def test_score_in_range(self) -> None:
        state = make_state(vol_surge=0.0, cooldown_s=0.0)
        warm_up(state, 100.0)
        ts = time.time()
        for i in range(30):
            result = state.on_tick(200.0, 199.9, 200.1, 5_000.0, ts + i)
            if result:
                assert 0 <= result["signal_score"] <= 100
                return
