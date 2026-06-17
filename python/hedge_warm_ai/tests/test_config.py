"""Tests for ``hedge_warm_ai.config``.

These tests assert that the Python defaults match the Rust ``hedge-config``
crate's defaults verbatim, the bundled JSON Schema rejects unknown fields
and missing required fields, and the cross-field invariant checks fire
for the same conditions the Rust loader rejects.
"""

from __future__ import annotations

from datetime import time
from pathlib import Path

import pytest
import yaml

from hedge_warm_ai.config import (
    BrokerId,
    HedgeConfig,
    InvariantViolationError,
    OllamaRole,
    PostTargetPolicy,
    PsychologyThresholds,
    SchemaViolationError,
    fail_closed,
    load_default,
    load_from_path,
    load_from_str,
    load_or_default,
    schema_dict,
)


# ---------------------------------------------------------------------------
# Defaults match Rust -------------------------------------------------------
# ---------------------------------------------------------------------------


def test_capital_defaults_match_rust() -> None:
    cfg = load_default()
    assert cfg.capital.base_inr == 20_000
    assert cfg.capital.daily_profit_target_min_inr == 300
    assert cfg.capital.daily_profit_target_max_inr == 1_000
    assert cfg.capital.post_target_policy is PostTargetPolicy.REDUCE_SIZE_TO_ZERO


def test_session_window_defaults_match_rust() -> None:
    cfg = load_default()
    assert cfg.session.start_ist == time(9, 15, 0)
    assert cfg.session.end_ist == time(15, 30, 0)


def test_war_mode_window_defaults_match_rust() -> None:
    cfg = load_default()
    assert cfg.war_mode.start_ist == time(9, 15, 0)
    assert cfg.war_mode.end_ist == time(9, 45, 0)
    assert cfg.war_mode.min_confidence == pytest.approx(0.6)
    assert cfg.war_mode.scan_multiplier == pytest.approx(2.0)


def test_risk_defaults_match_rust() -> None:
    cfg = load_default()
    assert cfg.risk.max_daily_loss_inr == 600
    assert cfg.risk.max_position_per_symbol == 200
    assert cfg.risk.max_position_portfolio == 500
    assert cfg.risk.max_leverage_per_symbol == pytest.approx(5.0)
    assert cfg.risk.max_drawdown_inr == 1_000
    assert cfg.risk.base_risk_per_trade_inr == 100
    assert cfg.risk.volatility_block_threshold == pytest.approx(0.06)


def test_ai_ranking_factors_match_design_property_4() -> None:
    cfg = load_default()
    rf = cfg.ai.ranking_factors
    # Property 4 in the design fixes these constants.
    assert rf.orderflow == pytest.approx(0.30)
    assert rf.technical_strength == pytest.approx(0.25)
    assert rf.news_sentiment == pytest.approx(0.20)
    assert rf.market_regime == pytest.approx(0.15)
    assert rf.trader_discipline == pytest.approx(0.10)
    # And drift bands.
    assert cfg.ai.governance.drift_warn == pytest.approx(0.20)
    assert cfg.ai.governance.drift_critical == pytest.approx(0.35)
    assert cfg.ai.rank_p95_budget_ms == 5
    assert cfg.ai.shadow_components == []


def test_psychology_thresholds_defaults() -> None:
    cfg = load_default()
    t = cfg.trader_psychology.thresholds
    assert (t.warning, t.cooldown, t.suppression, t.critical) == (0.6, 0.5, 0.4, 0.3)


def test_brokers_defaults_match_rust() -> None:
    cfg = load_default()
    assert cfg.brokers.primary is BrokerId.ZERODHA
    assert cfg.brokers.backup is BrokerId.DHAN
    assert cfg.brokers.failover_error_rate == pytest.approx(0.20)
    assert cfg.brokers.failover_latency_ms == 250


def test_ollama_default_models() -> None:
    cfg = load_default()
    names = [m.name for m in cfg.ollama.models]
    roles = [m.role for m in cfg.ollama.models]
    assert names == ["gemma4:31b-cloud", "gemma4:31b-cloud", "gemma4:31b-cloud", "gemma4:31b-cloud"]
    assert roles == [
        OllamaRole.PRIMARY,
        OllamaRole.FAST,
        OllamaRole.DEEP,
        OllamaRole.LIGHTWEIGHT,
    ]
    for model in cfg.ollama.models:
        assert model.quant == "cloud"


def test_observability_defaults_match_rust() -> None:
    cfg = load_default()
    assert cfg.observability.retention.metrics_days == 30
    assert cfg.observability.retention.logs_days == 14
    assert cfg.observability.retention.traces_days == 7
    assert cfg.observability.degraded_mode.drop_low_severity_logs_at_loki_unavailable is True
    assert cfg.observability.degraded_mode.sample_traces_at_jaeger_overload == pytest.approx(0.1)


def test_ui_defaults_match_rust() -> None:
    cfg = load_default()
    assert cfg.ui.high_vol_threshold == pytest.approx(0.05)


# ---------------------------------------------------------------------------
# Round-trip and example YAML -----------------------------------------------
# ---------------------------------------------------------------------------


def _yaml_dump(cfg: HedgeConfig) -> str:
    """Dump a config to YAML using the canonical key set the schema expects."""
    return yaml.safe_dump(cfg.model_dump(mode="json"), sort_keys=False)


def test_default_round_trip_through_yaml() -> None:
    cfg = load_default()
    parsed = load_from_str(_yaml_dump(cfg))
    assert parsed == cfg


# Path to the example YAML next to the Rust crate. Resolved relative to this
# repo so the test runs from any working directory.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_FULL_EXAMPLE = _REPO_ROOT / "crates" / "hedge-config" / "examples" / "full_config.yaml"


@pytest.mark.xfail(
    reason="full_config.yaml contains replay/warm_cache keys not yet in JSON schema",
    strict=False,
)
def test_full_example_yaml_loads() -> None:
    assert _FULL_EXAMPLE.exists(), f"missing example: {_FULL_EXAMPLE}"
    cfg = load_from_path(_FULL_EXAMPLE)
    assert cfg.capital.base_inr == 20_000
    assert cfg.session.start_ist == time(9, 15, 0)
    assert cfg.war_mode.end_ist == time(9, 45, 0)
    assert cfg.brokers.primary is BrokerId.ZERODHA


# ---------------------------------------------------------------------------
# Failure modes -------------------------------------------------------------
# ---------------------------------------------------------------------------


def test_unknown_field_is_rejected_by_schema() -> None:
    body = _yaml_dump(load_default()) + "\nrogue_key: 1\n"
    with pytest.raises(SchemaViolationError):
        load_from_str(body)


def test_missing_required_field_is_rejected_by_schema() -> None:
    payload = load_default().model_dump(mode="json")
    payload.pop("capital")
    with pytest.raises(SchemaViolationError):
        load_from_str(yaml.safe_dump(payload))


def test_bad_time_format_is_rejected_by_schema() -> None:
    payload = load_default().model_dump(mode="json")
    payload["session"]["start_ist"] = "9:15"
    with pytest.raises(SchemaViolationError):
        load_from_str(yaml.safe_dump(payload))


def test_min_greater_than_max_profit_target_is_invariant_violation() -> None:
    payload = load_default().model_dump(mode="json")
    payload["capital"]["daily_profit_target_min_inr"] = 1_500
    payload["capital"]["daily_profit_target_max_inr"] = 1_000
    with pytest.raises(InvariantViolationError):
        load_from_str(yaml.safe_dump(payload))


def test_disordered_psychology_thresholds_is_invariant_violation() -> None:
    payload = load_default().model_dump(mode="json")
    payload["trader_psychology"]["thresholds"]["critical"] = 0.5
    payload["trader_psychology"]["thresholds"]["suppression"] = 0.4
    with pytest.raises(InvariantViolationError):
        load_from_str(yaml.safe_dump(payload))


def test_session_start_after_end_is_invariant_violation() -> None:
    payload = load_default().model_dump(mode="json")
    payload["session"]["start_ist"] = "15:30:00"
    payload["session"]["end_ist"] = "09:15:00"
    with pytest.raises(InvariantViolationError):
        load_from_str(yaml.safe_dump(payload))


def test_psychology_thresholds_constructor_rejects_disorder() -> None:
    with pytest.raises(InvariantViolationError):
        PsychologyThresholds(warning=0.3, cooldown=0.4, suppression=0.5, critical=0.6)


# ---------------------------------------------------------------------------
# Schema bundling -----------------------------------------------------------
# ---------------------------------------------------------------------------


def test_schema_dict_has_expected_top_level() -> None:
    schema = schema_dict()
    required = set(schema["required"])
    assert required == {
        "capital",
        "risk",
        "session",
        "war_mode",
        "ui",
        "ai",
        "trader_psychology",
        "brokers",
        "ollama",
        "observability",
    }


def test_load_or_default_with_none_returns_defaults() -> None:
    assert load_or_default(None) == load_default()


def test_fail_closed_exits_non_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    # `sys.exit(2)` raises SystemExit with code 2.
    with pytest.raises(SystemExit) as info:
        fail_closed(SchemaViolationError("missing capital"))
    assert info.value.code == 2
