//! Property-based tests for `hedge-config` (task 6.2).
//!
//! Validates:
//!   - Default values match design spec verbatim
//!   - PsychologyThresholds ordering invariant
//!   - YAML round-trip structural equality
//!   - ReplaySpeed divisor/as_str consistency

use chrono::Timelike;
use hedge_config::{defaults, PsychologyThresholds, ReplaySpeed, HedgeConfig};
use proptest::prelude::*;

// ---- Default values (deterministic, no proptest needed) -------------------

#[test]
fn capital_defaults_match_design() {
    let c = defaults::capital();
    assert_eq!(c.base_inr, 20_000);
    assert_eq!(c.daily_profit_target_min_inr, 300);
    assert_eq!(c.daily_profit_target_max_inr, 1_000);
}

#[test]
fn session_defaults_match_design() {
    let s = defaults::session();
    assert_eq!(s.start_ist.hour(), 9);
    assert_eq!(s.start_ist.minute(), 15);
    assert_eq!(s.end_ist.hour(), 15);
    assert_eq!(s.end_ist.minute(), 30);
}

#[test]
fn war_mode_defaults_match_design() {
    let w = defaults::war_mode();
    assert_eq!(w.start_ist.hour(), 9);
    assert_eq!(w.start_ist.minute(), 15);
    assert_eq!(w.end_ist.hour(), 9);
    assert_eq!(w.end_ist.minute(), 45);
    assert_eq!(w.min_confidence, 0.6);
    assert_eq!(w.scan_multiplier, 2.0);
}

#[test]
fn trader_psychology_defaults_satisfy_ordering() {
    let tp = defaults::trader_psychology();
    let t = &tp.thresholds;
    assert!(t.critical < t.suppression);
    assert!(t.suppression < t.cooldown);
    assert!(t.cooldown < t.warning);
}

#[test]
fn ranking_factors_weights_finite() {
    let ai = defaults::ai();
    let rf = &ai.ranking_factors;
    assert!(rf.orderflow.is_finite() && rf.orderflow > 0.0);
    assert!(rf.technical_strength.is_finite() && rf.technical_strength > 0.0);
    assert!(rf.news_sentiment.is_finite() && rf.news_sentiment > 0.0);
    assert!(rf.market_regime.is_finite() && rf.market_regime > 0.0);
    assert!(rf.trader_discipline.is_finite() && rf.trader_discipline > 0.0);
}

#[test]
fn warm_cache_defaults_match_design() {
    let wc = defaults::warm_cache();
    assert_eq!(wc.trade_confidence_lru_size, 8_192);
    assert_eq!(wc.staleness_window_ms, 5_000);
    assert_eq!(wc.nats_url, "nats://127.0.0.1:4222");
}

// ---- PsychologyThresholds ordering proptests -----------------------------

proptest! {
    #[test]
    fn valid_thresholds_pass_validation(
        critical in 0.0f32..0.1,
        gap1 in 0.01f32..0.2,
        gap2 in 0.01f32..0.2,
        gap3 in 0.01f32..0.2,
    ) {
        let c = critical;
        let s = c + gap1;
        let cd = s + gap2;
        let w = cd + gap3;
        prop_assume!(w <= 1.0);
        let result = PsychologyThresholds::validated(w, cd, s, c);
        prop_assert!(result.is_ok(), "validation failed: {:?}", result.err());
    }

    #[test]
    fn validated_thresholds_always_ordered(
        critical in 0.0f32..0.1,
        gap1 in 0.01f32..0.2,
        gap2 in 0.01f32..0.2,
        gap3 in 0.01f32..0.2,
    ) {
        let c = critical;
        let s = c + gap1;
        let cd = s + gap2;
        let w = cd + gap3;
        prop_assume!(w <= 1.0);
        let t = PsychologyThresholds::validated(w, cd, s, c).unwrap();
        prop_assert!(t.critical < t.suppression);
        prop_assert!(t.suppression < t.cooldown);
        prop_assert!(t.cooldown < t.warning);
    }
}

// ---- YAML round-trip proptests -------------------------------------------

#[test]
fn default_config_yaml_round_trip() {
    let original = defaults::hedge_config();
    let yaml = serde_yaml::to_string(&original).expect("serialize");
    let restored: HedgeConfig = serde_yaml::from_str(&yaml).expect("deserialize");
    assert_eq!(original, restored);
}

proptest! {
    #[test]
    fn config_yaml_round_trip(
        base_inr in 1u32..1_000_000,
        max_daily_loss in 1u32..10_000,
        max_pos_sym in 1u32..10_000,
    ) {
        let mut cfg = defaults::hedge_config();
        cfg.capital.base_inr = base_inr;
        cfg.risk.max_daily_loss_inr = max_daily_loss;
        cfg.risk.max_position_per_symbol = max_pos_sym;
        let yaml = serde_yaml::to_string(&cfg).expect("serialize");
        let restored: HedgeConfig = serde_yaml::from_str(&yaml).expect("deserialize");
        prop_assert_eq!(cfg, restored);
    }
}

// ---- ReplaySpeed proptests -----------------------------------------------

proptest! {
    #[test]
    fn replay_speed_divisor_str_consistency(variant in prop_oneof![
        Just(ReplaySpeed::X1),
        Just(ReplaySpeed::X10),
        Just(ReplaySpeed::Max),
    ]) {
        match variant {
            ReplaySpeed::X1 => {
                prop_assert_eq!(variant.divisor(), Some(1));
                prop_assert_eq!(variant.as_str(), "x1");
            }
            ReplaySpeed::X10 => {
                prop_assert_eq!(variant.divisor(), Some(10));
                prop_assert_eq!(variant.as_str(), "x10");
            }
            ReplaySpeed::Max => {
                prop_assert_eq!(variant.divisor(), None);
                prop_assert_eq!(variant.as_str(), "max");
            }
        }
    }

    #[test]
    fn replay_speed_serde_round_trip(variant in prop_oneof![
        Just(ReplaySpeed::X1),
        Just(ReplaySpeed::X10),
        Just(ReplaySpeed::Max),
    ]) {
        let json = serde_json::to_string(&variant).expect("serialize");
        let restored: ReplaySpeed = serde_json::from_str(&json).expect("deserialize");
        prop_assert_eq!(variant, restored);
    }
}
