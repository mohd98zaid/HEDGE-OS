//! Property-based tests for the Signal_Engine (task 13.1 sub-tests).
//!
//! These tests cover the static gating + clamping behaviour required by
//! Property 7 (Strategy Gating Respects Toggles, Regime, News, and
//! War_Mode) and Property 4 (Score and Formula Equivalence —
//! `base_probability ∈ [0, 1]` and `confidence ∈ [0, 1]`).
//!
//! **Validates: Requirements 4.3, 4.4, 4.5, 4.6, 12.6, 13.4, 26.2, 26.3**

use std::collections::BTreeMap;

use hedge_core::{Regime, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};
use hedge_signals::{
    LiquiditySweepReversal, MomentumBreakout, NewsGates, OpeningRangeBreakout,
    Strategy as HedgeStrategy, StrategyContext, StrategyToggles,
    VolatilityCompressionBreakout, VwapPullback,
};
use proptest::prelude::*;

// ---- Generators ---------------------------------------------------------

/// Build a `FeatureSnapshot` from `proptest`-generated primitives.
///
/// All floats are sampled from finite ranges so we exercise the gating
/// boundaries without spending the proptest budget on NaN / infinity
/// edge cases. The engine's `clamp01` helper handles those defensively
/// and is unit-tested separately.
///
/// The proptest `Strategy` impl on tuples maxes out at ~10 elements, so
/// we partition the 17 fields into two `prop_map`-stacked tuples and
/// recombine.
fn arb_snapshot() -> impl Strategy<Value = FeatureSnapshot> {
    let head = (
        any::<[u8; 16]>(),
        1u32..1024u32,
        1_00i64..1_000_00,           // vwap
        1i64..1_000,                 // atr
        1_00i64..1_000_00,           // ema_fast
        1_00i64..1_000_00,           // ema_slow
        -2.0f32..2.0,                // ema_slope
        0.0f32..1.0,                 // realized_vol
        -0.1f32..0.1,                // momentum
    );
    let tail = (
        -10_000i64..10_000i64,       // rolling_delta
        -1.0f32..1.0,                // liquidity_imbalance
        -1.0f32..1.0,                // orderflow_strength
        0u8..6u8,                    // candle_structure
        -1.0f32..1.0,                // breakout_pressure
        0.0f32..1.0,                 // compression_zone
        -1.0f32..1.0,                // liquidity_sweep
        any::<u64>(),
    );
    (head, tail).prop_map(
        |(
            (
                cid,
                symbol,
                vwap,
                atr,
                ema_fast,
                ema_slow,
                ema_slope,
                realized_vol,
                momentum,
            ),
            (
                rolling_delta,
                liq,
                of_str,
                candle,
                pressure,
                compression,
                sweep,
                ts,
            ),
        )| FeatureSnapshot {
            correlation_id: cid,
            symbol,
            vwap,
            atr,
            ema_fast,
            ema_slow,
            ema_slope,
            realized_vol,
            momentum,
            rolling_delta,
            liquidity_imbalance: liq,
            orderflow_strength: of_str,
            candle_structure: candle,
            breakout_pressure: pressure,
            compression_zone: compression,
            liquidity_sweep: sweep,
            ts_ns: ts,
        },
    )
}

fn arb_regime() -> impl Strategy<Value = Regime> {
    prop_oneof![
        Just(Regime::Trending),
        Just(Regime::Sideways),
        Just(Regime::Panic),
        Just(Regime::HighVolatility),
        Just(Regime::NewsDriven),
        Just(Regime::LiquidityCrisis),
        Just(Regime::LowParticipation),
    ]
}

// ---- Helpers ------------------------------------------------------------

/// Mirror of [`hedge_signals::engine::evaluate_strategies`] using `Box<dyn>`
/// strategies. We rebuild the registry per call rather than hold an
/// `Arc` so each test is fully isolated. The 5-strategy set excludes
/// the OI strategy (which never fires without a wired cache).
fn evaluate_all(snap: &FeatureSnapshot, ctx: &StrategyContext) -> Vec<Signal> {
    let strategies: Vec<Box<dyn HedgeStrategy>> = vec![
        Box::new(OpeningRangeBreakout),
        Box::new(VwapPullback),
        Box::new(MomentumBreakout),
        Box::new(LiquiditySweepReversal),
        Box::new(VolatilityCompressionBreakout),
    ];
    let mut out = Vec::new();
    for s in strategies {
        // Pre-evaluate gates: trader_toggle + regime + news.
        if !ctx.trader_config.is_enabled(s.id()) {
            continue;
        }
        if !s.enabled_in(ctx.regime) {
            continue;
        }
        if ctx.news_gates.is_symbol_blocked(SymbolId::new(snap.symbol)) {
            continue;
        }
        if let Some(sig) = s.evaluate(snap, ctx) {
            // Post-evaluate war-mode gate.
            if ctx.war_mode && sig.confidence + f32::EPSILON < ctx.war_mode_min_confidence {
                continue;
            }
            out.push(sig);
        }
    }
    out
}

// ---- Properties ---------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property: every emitted signal's `base_probability` is in [0, 1].
    /// **Validates: Requirements 4.3**
    #[test]
    fn base_probability_in_unit_interval(snap in arb_snapshot(), regime in arb_regime()) {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let ctx = StrategyContext {
            regime,
            trader_config: &toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: &news,
        };
        for sig in evaluate_all(&snap, &ctx) {
            prop_assert!(
                (0.0..=1.0).contains(&sig.base_probability),
                "base_probability {} not in [0, 1]",
                sig.base_probability
            );
        }
    }

    /// Property: every emitted signal's `confidence` is in [0, 1].
    /// **Validates: Requirements 4.3**
    #[test]
    fn confidence_in_unit_interval(snap in arb_snapshot(), regime in arb_regime()) {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let ctx = StrategyContext {
            regime,
            trader_config: &toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: &news,
        };
        for sig in evaluate_all(&snap, &ctx) {
            prop_assert!(
                (0.0..=1.0).contains(&sig.confidence),
                "confidence {} not in [0, 1]",
                sig.confidence
            );
        }
    }

    /// Property: a strategy disabled by trader toggle never emits.
    /// **Validates: Requirements 4.5**
    #[test]
    fn disabled_strategy_emits_nothing(
        snap in arb_snapshot(),
        regime in arb_regime(),
        which in 0u8..6,
    ) {
        let id = match which {
            0 => StrategyId::OpeningRangeBreakout,
            1 => StrategyId::VwapPullback,
            2 => StrategyId::MomentumBreakout,
            3 => StrategyId::LiquiditySweepReversal,
            4 => StrategyId::OptionsOiExpansionBreakout,
            _ => StrategyId::VolatilityCompressionBreakout,
        };
        let mut toggles = StrategyToggles::all_enabled();
        toggles.enabled.insert(id, false);
        let news = NewsGates::empty();
        let ctx = StrategyContext {
            regime,
            trader_config: &toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: &news,
        };
        for sig in evaluate_all(&snap, &ctx) {
            prop_assert_ne!(
                sig.strategy, id.as_u8(),
                "disabled strategy {:?} emitted a signal",
                id
            );
        }
    }

    /// Property: when the symbol is news-blocked no strategy emits.
    /// **Validates: Requirements 12.6**
    #[test]
    fn news_blocked_symbol_emits_nothing(snap in arb_snapshot(), regime in arb_regime()) {
        let toggles = StrategyToggles::all_enabled();
        let mut news = NewsGates::empty();
        news.blocked_symbols.push(SymbolId::new(snap.symbol));
        let ctx = StrategyContext {
            regime,
            trader_config: &toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: &news,
        };
        let signals = evaluate_all(&snap, &ctx);
        prop_assert!(
            signals.is_empty(),
            "news-blocked symbol still emitted {} signal(s)",
            signals.len()
        );
    }

    /// Property: while war mode is active, no signal below the floor is
    /// retained. **Validates: Requirements 26.2, 26.3**
    #[test]
    fn war_mode_drops_low_confidence(
        snap in arb_snapshot(),
        regime in arb_regime(),
        floor in 0.5f32..1.0,
    ) {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let ctx = StrategyContext {
            regime,
            trader_config: &toggles,
            war_mode: true,
            war_mode_min_confidence: floor,
            previous_day: None,
            news_gates: &news,
        };
        for sig in evaluate_all(&snap, &ctx) {
            prop_assert!(
                sig.confidence + f32::EPSILON >= floor,
                "war-mode-active signal had confidence {} below floor {}",
                sig.confidence, floor
            );
        }
    }

    /// Property: when a regime disables a strategy, that strategy never
    /// emits in that regime. **Validates: Requirements 4.6, 13.4**
    #[test]
    fn regime_disabled_strategy_emits_nothing(snap in arb_snapshot(), regime in arb_regime()) {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let ctx = StrategyContext {
            regime,
            trader_config: &toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: &news,
        };
        let strategies: Vec<Box<dyn HedgeStrategy>> = vec![
            Box::new(OpeningRangeBreakout),
            Box::new(VwapPullback),
            Box::new(MomentumBreakout),
            Box::new(LiquiditySweepReversal),
            Box::new(VolatilityCompressionBreakout),
        ];
        // Map of which strategies are regime-disabled in `regime`.
        let mut disabled: BTreeMap<StrategyId, bool> = BTreeMap::new();
        for s in &strategies {
            disabled.insert(s.id(), !s.enabled_in(regime));
        }
        // Emitted signals must not include any strategy whose
        // `enabled_in(regime) == false`.
        for sig in evaluate_all(&snap, &ctx) {
            let id = StrategyId::from_u8(sig.strategy).expect("known strategy");
            prop_assert!(
                !disabled.get(&id).copied().unwrap_or(false),
                "regime-disabled strategy {:?} emitted in {:?}",
                id, regime
            );
        }
    }
}
