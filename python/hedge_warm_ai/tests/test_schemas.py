"""Unit tests for the pydantic mirrors of the Warm_AI_Pipeline JSON schemas.

Each test instantiates a model with a minimal valid payload and asserts that
out-of-range or extra-field payloads are rejected. PBT round-trip tests are
deferred to task 4.2.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from hedge_warm_ai.schemas import (
    AiGovAction,
    AiJournalEntry,
    BudgetBreach,
    LatencyRecordJson,
    NewsImpact,
    ObsError,
    OllamaDegraded,
    OpsAction,
    OpsSession,
    OpsWarMode,
    PreviousDayMemory,
    PriorityChanged,
    PsychIntervention,
    PsychStability,
    RankedSignal,
    RegimeChanged,
    TraderIntentKillSwitch,
    TraderIntentOrder,
    TraderIntentPriority,
    TraderIntentStrategyToggle,
)


# --------------------------------------------------------------------------- #
# JSON_SCHEMA constants are valid JSON.
# --------------------------------------------------------------------------- #

ALL_MODELS = [
    AiGovAction, AiJournalEntry, BudgetBreach, LatencyRecordJson, NewsImpact,
    ObsError, OllamaDegraded, OpsAction, OpsSession, OpsWarMode,
    PreviousDayMemory, PriorityChanged, PsychIntervention, PsychStability,
    RankedSignal, RegimeChanged, TraderIntentKillSwitch, TraderIntentOrder,
    TraderIntentPriority, TraderIntentStrategyToggle,
]


@pytest.mark.parametrize("model", ALL_MODELS)
def test_json_schema_is_valid_json(model):
    parsed = json.loads(model.JSON_SCHEMA)
    assert parsed["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert parsed["type"] == "object"
    assert parsed["additionalProperties"] is False


# --------------------------------------------------------------------------- #
# Minimal valid payloads.
# --------------------------------------------------------------------------- #

def test_ranked_signal_minimal_valid():
    sig = RankedSignal.model_validate({
        "correlation_id": "01J",
        "signal_id": "01J",
        "trade_confidence_score": 0.71,
        "factors": {
            "orderflow": 0.8,
            "technical_strength": 0.6,
            "news_sentiment": 0.7,
            "market_regime": 0.5,
            "trader_discipline": 0.9,
        },
        "shadow": False,
        "ts_ns": 1730000000000000000,
    })
    assert sig.trade_confidence_score == 0.71
    assert 0.0 <= sig.factors.orderflow <= 1.0


def test_news_impact_minimal_valid():
    msg = NewsImpact.model_validate({
        "correlation_id": "01J",
        "symbol": "RELIANCE",
        "headline_id": "h1",
        "sentiment": -0.6,
        "impact_magnitude": 0.8,
        "fast_path": True,
        "slow_path_pending": True,
        "ts_ns": 1,
    })
    assert msg.sentiment == -0.6


def test_regime_changed_minimal_valid():
    msg = RegimeChanged.model_validate({"from": "Trending", "to": "Panic", "ts_ns": 1})
    assert msg.from_ == "Trending" and msg.to == "Panic"


def test_psych_stability_minimal_valid():
    msg = PsychStability.model_validate({
        "score": 0.42,
        "components": {
            "discipline": 0.5, "emotional_control": 0.3,
            "risk_consistency": 0.4, "patience": 0.5,
        },
        "behaviors": ["revenge_trading", "rapid_re_entry"],
        "ts_ns": 1,
    })
    assert msg.score == 0.42


def test_psych_intervention_minimal_valid():
    msg = PsychIntervention.model_validate({
        "action": "cooldown", "trigger_score": 0.42, "ts_ns": 1,
    })
    assert msg.action == "cooldown"


def test_priority_changed_minimal_valid():
    msg = PriorityChanged.model_validate({
        "symbol": "RELIANCE", "from": "P3", "to": "P1", "ts_ns": 1,
    })
    assert msg.to == "P1"


def test_ai_gov_action_minimal_valid():
    msg = AiGovAction.model_validate({
        "component": "AI_Trade_Ranking_Engine",
        "action": "reduce_influence",
        "metric": "drift", "value": 0.41, "threshold": 0.35,
        "ts_ns": 1,
    })
    assert msg.action == "reduce_influence"


def test_ollama_degraded_minimal_valid():
    msg = OllamaDegraded.model_validate({
        "model": "qwen2.5:14b", "fallback_model": "mistral:7b",
        "reason": "timeout", "ts_ns": 1,
    })
    assert msg.reason == "timeout"


def test_journal_entry_minimal_valid():
    msg = AiJournalEntry.model_validate({
        "correlation_id": "01J", "trade_id": "t1", "symbol": "RELIANCE",
        "side": "Buy", "quantity": 10,
        "entry_paise": 250000, "exit_paise": 250500,
        "pnl_inr": 50.0, "narrative": "good entry on VWAP pullback",
        "ts_ns": 1,
    })
    assert msg.pnl_inr == 50.0


def test_prev_day_memory_minimal_valid():
    msg = PreviousDayMemory.model_validate({
        "symbol": "RELIANCE", "session_date": "2024-12-30",
        "open_paise": 240000, "high_paise": 260000,
        "low_paise": 235000, "close_paise": 255000, "vwap_paise": 248000,
        "key_levels": [{"kind": "vwap", "price_paise": 248000}],
        "ts_ns": 1,
    })
    assert msg.key_levels[0].kind == "vwap"


def test_trader_intent_killswitch_minimal_valid():
    msg = TraderIntentKillSwitch.model_validate({
        "correlation_id": "01J", "engaged": True, "actor": "trader-1", "ts_ns": 1,
    })
    assert msg.engaged is True


def test_trader_intent_strategy_toggle_minimal_valid():
    msg = TraderIntentStrategyToggle.model_validate({
        "correlation_id": "01J", "strategy": "VwapPullback",
        "enabled": False, "actor": "trader-1", "ts_ns": 1,
    })
    assert msg.enabled is False


def test_trader_intent_priority_minimal_valid():
    msg = TraderIntentPriority.model_validate({
        "correlation_id": "01J", "symbol": "RELIANCE", "to": "P1",
        "actor": "trader-1", "ts_ns": 1,
    })
    assert msg.to == "P1"


def test_trader_intent_order_minimal_valid_market():
    msg = TraderIntentOrder.model_validate({
        "correlation_id": "01J", "symbol": "RELIANCE", "side": "Buy",
        "quantity": 10, "order_type": "Market", "exchange": "NSE",
        "actor": "trader-1", "ts_ns": 1,
    })
    assert msg.order_type == "Market"


def test_trader_intent_order_limit_requires_limit_paise():
    with pytest.raises(ValidationError):
        TraderIntentOrder.model_validate({
            "correlation_id": "01J", "symbol": "RELIANCE", "side": "Buy",
            "quantity": 10, "order_type": "Limit", "exchange": "NSE",
            "actor": "trader-1", "ts_ns": 1,
        })


def test_ops_session_minimal_valid():
    msg = OpsSession.model_validate({"session_id": 1, "phase": "start", "ts_ns": 1})
    assert msg.phase == "start"


def test_ops_warmode_minimal_valid():
    msg = OpsWarMode.model_validate({"session_id": 1, "phase": "start", "ts_ns": 1})
    assert msg.phase == "start"


def test_ops_action_minimal_valid():
    msg = OpsAction.model_validate({
        "target": "hedge-features", "action": "restart",
        "reason": "panic", "ts_ns": 1,
    })
    assert msg.action == "restart"


def test_obs_latency_minimal_valid():
    msg = LatencyRecordJson.model_validate({
        "correlation_id": "01J", "stage": "TickIngest",
        "nanos": 1500, "budget_nanos": 2000, "breach": False,
    })
    assert msg.stage == "TickIngest"


def test_obs_budget_breach_minimal_valid():
    msg = BudgetBreach.model_validate({
        "correlation_id": "01J", "stage": "RiskCheck",
        "nanos": 3000, "budget_nanos": 2000, "ts_ns": 1,
    })
    assert msg.nanos > msg.budget_nanos


def test_obs_error_minimal_valid():
    msg = ObsError.model_validate({
        "correlation_id": "01J", "source": "hedge-features", "code": "panic",
        "severity": "critical", "message": "feature panic", "ts_ns": 1,
    })
    assert msg.severity == "critical"


# --------------------------------------------------------------------------- #
# Out-of-range and structural rejections.
# --------------------------------------------------------------------------- #

def test_news_sentiment_above_one_rejected():
    payload = {
        "correlation_id": "01J", "symbol": "RELIANCE", "headline_id": "h",
        "sentiment": 1.5, "impact_magnitude": 0.5,
        "fast_path": True, "slow_path_pending": False, "ts_ns": 1,
    }
    with pytest.raises(ValidationError):
        NewsImpact.model_validate(payload)


def test_news_sentiment_below_negative_one_rejected():
    payload = {
        "correlation_id": "01J", "symbol": "RELIANCE", "headline_id": "h",
        "sentiment": -1.5, "impact_magnitude": 0.5,
        "fast_path": True, "slow_path_pending": False, "ts_ns": 1,
    }
    with pytest.raises(ValidationError):
        NewsImpact.model_validate(payload)


def test_impact_magnitude_above_one_rejected():
    payload = {
        "correlation_id": "01J", "symbol": "RELIANCE", "headline_id": "h",
        "sentiment": 0.0, "impact_magnitude": 1.5,
        "fast_path": True, "slow_path_pending": False, "ts_ns": 1,
    }
    with pytest.raises(ValidationError):
        NewsImpact.model_validate(payload)


def test_ranked_signal_score_above_one_rejected():
    payload = {
        "correlation_id": "01J", "signal_id": "01J",
        "trade_confidence_score": 1.5,
        "factors": {
            "orderflow": 0.5, "technical_strength": 0.5,
            "news_sentiment": 0.5, "market_regime": 0.5,
            "trader_discipline": 0.5,
        },
        "shadow": False, "ts_ns": 1,
    }
    with pytest.raises(ValidationError):
        RankedSignal.model_validate(payload)


def test_psych_stability_score_negative_rejected():
    payload = {
        "score": -0.1,
        "components": {
            "discipline": 0.5, "emotional_control": 0.5,
            "risk_consistency": 0.5, "patience": 0.5,
        },
        "behaviors": [], "ts_ns": 1,
    }
    with pytest.raises(ValidationError):
        PsychStability.model_validate(payload)


def test_extra_field_rejected_everywhere():
    # `extra="forbid"` mirrors `additionalProperties: false`.
    base = {
        "correlation_id": "01J", "engaged": True, "actor": "a", "ts_ns": 1,
        "this_field_is_not_allowed": True,
    }
    with pytest.raises(ValidationError):
        TraderIntentKillSwitch.model_validate(base)


def test_ops_session_invalid_phase_rejected():
    with pytest.raises(ValidationError):
        OpsSession.model_validate({"session_id": 1, "phase": "midday", "ts_ns": 1})


def test_priority_invalid_tier_rejected():
    with pytest.raises(ValidationError):
        PriorityChanged.model_validate({
            "symbol": "RELIANCE", "from": "P3", "to": "P5", "ts_ns": 1,
        })


def test_ranking_factors_above_one_rejected():
    payload = {
        "correlation_id": "01J", "signal_id": "01J",
        "trade_confidence_score": 0.5,
        "factors": {
            "orderflow": 1.5, "technical_strength": 0.5,
            "news_sentiment": 0.5, "market_regime": 0.5,
            "trader_discipline": 0.5,
        },
        "shadow": False, "ts_ns": 1,
    }
    with pytest.raises(ValidationError):
        RankedSignal.model_validate(payload)
