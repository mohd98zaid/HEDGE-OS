"""Tests for the Binance Risk Guard (pre-trade gate + circuit breakers)."""

from __future__ import annotations

import time
import pytest

from hedge_binance.risk.service import OpenPosition, RiskState


def make_state(**overrides) -> RiskState:
    import os
    # Set env before RiskState reads it
    defaults = {
        "BINANCE_MAX_POSITION_USDT": "500",
        "BINANCE_DAILY_LOSS_LIMIT":  "100",
        "BINANCE_MAX_OPEN_ORDERS":   "5",
        "BINANCE_MAX_DAILY_TRADES":  "10",
        "BINANCE_MAX_CONSEC_LOSSES": "3",
        "BINANCE_MIN_SIGNAL_SCORE":  "30",
    }
    defaults.update(overrides)
    for k, v in defaults.items():
        os.environ[k] = v
    return RiskState()


def good_signal(symbol="BTCUSDT", score=80, qty=50.0, side="buy") -> dict:
    return {
        "correlation_id": "test",
        "symbol": symbol,
        "side": side,
        "price": 50000.0,
        "qty_usdt": qty,
        "stop_loss_price": 49000.0,
        "take_profit_price": 53000.0,
        "signal_score": score,
    }


class TestPreTradeGates:
    def test_good_signal_approved(self) -> None:
        state = make_state()
        ok, reason = state.check(good_signal())
        assert ok
        assert reason == ""

    def test_low_score_rejected(self) -> None:
        state = make_state()
        sig = good_signal(score=20)
        ok, reason = state.check(sig)
        assert not ok
        assert "signal_score_too_low" in reason

    def test_daily_loss_limit_blocks(self) -> None:
        state = make_state(BINANCE_DAILY_LOSS_LIMIT="50")
        state.daily_pnl = -50.0
        ok, reason = state.check(good_signal())
        assert not ok
        assert reason == "daily_loss_limit_hit"

    def test_max_daily_trades_blocks(self) -> None:
        state = make_state(BINANCE_MAX_DAILY_TRADES="2")
        state.daily_trades = 2
        ok, reason = state.check(good_signal())
        assert not ok
        assert reason == "daily_trade_limit_hit"

    def test_notional_too_large_rejected(self) -> None:
        state = make_state(BINANCE_MAX_POSITION_USDT="100")
        ok, reason = state.check(good_signal(qty=200.0))
        assert not ok
        assert "notional_too_large" in reason

    def test_duplicate_position_rejected(self) -> None:
        state = make_state()
        state.positions["BTCUSDT"] = OpenPosition(
            symbol="BTCUSDT", side="buy",
            entry_price=50000.0, qty_usdt=50.0,
            stop_loss_price=49000.0, take_profit_price=53000.0,
        )
        ok, reason = state.check(good_signal())
        assert not ok
        assert "position_already_open" in reason


class TestCircuitBreaker:
    def test_circuit_trips_after_n_consec_losses(self) -> None:
        state = make_state(BINANCE_MAX_CONSEC_LOSSES="2")
        # Simulate 2 consecutive losses
        state.positions["BTCUSDT"] = OpenPosition(
            symbol="BTCUSDT", side="buy",
            entry_price=50000.0, qty_usdt=50.0,
            stop_loss_price=49000.0, take_profit_price=53000.0,
        )
        state.on_position_closed("BTCUSDT", -10.0, "stop_loss")
        state.positions["ETHUSDT"] = OpenPosition(
            symbol="ETHUSDT", side="buy",
            entry_price=3000.0, qty_usdt=50.0,
            stop_loss_price=2900.0, take_profit_price=3200.0,
        )
        state.on_position_closed("ETHUSDT", -15.0, "stop_loss")

        # Next signal should be blocked by circuit breaker
        ok, reason = state.check(good_signal())
        assert not ok
        assert "circuit_breaker" in reason

    def test_profit_resets_consecutive_counter(self) -> None:
        state = make_state(BINANCE_MAX_CONSEC_LOSSES="3")
        state.consec_losses = 2   # two losses already
        state.positions["BTCUSDT"] = OpenPosition(
            symbol="BTCUSDT", side="buy",
            entry_price=50000.0, qty_usdt=50.0,
            stop_loss_price=49000.0, take_profit_price=53000.0,
        )
        state.on_position_closed("BTCUSDT", +25.0, "take_profit")  # profitable!
        assert state.consec_losses == 0


class TestSLTPMonitor:
    def test_stop_loss_triggers_exit_signal(self) -> None:
        state = make_state()
        state.positions["BTCUSDT"] = OpenPosition(
            symbol="BTCUSDT", side="buy",
            entry_price=50000.0, qty_usdt=50.0,
            stop_loss_price=49000.0,
            take_profit_price=53000.0,
        )
        sig = state.check_sltp("BTCUSDT", 48500.0)   # price below SL
        assert sig is not None
        assert sig["side"] == "sell"        # close a BUY with a SELL
        assert sig["strategy"] == "stop_loss"
        assert "BTCUSDT" not in state.positions  # position removed

    def test_take_profit_triggers_exit_signal(self) -> None:
        state = make_state()
        state.positions["BTCUSDT"] = OpenPosition(
            symbol="BTCUSDT", side="buy",
            entry_price=50000.0, qty_usdt=50.0,
            stop_loss_price=49000.0,
            take_profit_price=53000.0,
        )
        sig = state.check_sltp("BTCUSDT", 54000.0)   # price above TP
        assert sig is not None
        assert sig["side"] == "sell"
        assert sig["strategy"] == "take_profit"

    def test_price_between_sl_and_tp_returns_none(self) -> None:
        state = make_state()
        state.positions["BTCUSDT"] = OpenPosition(
            symbol="BTCUSDT", side="buy",
            entry_price=50000.0, qty_usdt=50.0,
            stop_loss_price=49000.0,
            take_profit_price=53000.0,
        )
        sig = state.check_sltp("BTCUSDT", 51000.0)   # in range — no action
        assert sig is None
        assert "BTCUSDT" in state.positions           # position still open

    def test_no_open_position_returns_none(self) -> None:
        state = make_state()
        sig = state.check_sltp("BTCUSDT", 50000.0)
        assert sig is None
