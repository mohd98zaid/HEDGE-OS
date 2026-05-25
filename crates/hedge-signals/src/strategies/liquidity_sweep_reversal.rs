//! Liquidity_Sweep_Reversal strategy.
//!
//! Fires when the Feature_Extraction_Engine reports a non-zero
//! `liquidity_sweep` signal AND the EMA fast/slow stack confirms a
//! reversal in the **opposite** direction of the sweep.
//!
//! ### Convention
//!
//! `liquidity_sweep` is positive when a fresh local high was tagged then
//! reversed (a long-side stop-run that traps late buyers — fade by going
//! short) and negative when a fresh local low was tagged then reversed
//! (a short-side stop-run — fade by going long). The orderflow strength
//! reading must agree with the reversal direction.
//!
//! ### Inputs
//!
//! * `liquidity_sweep` (f32) — non-zero → potential reversal candidate.
//! * `ema_slope` (f32) — must agree with the reversal direction.
//! * `orderflow_strength` (f32 ∈ [-1, 1]) — confirms the side.
//! * `atr` (i64 paise) — used for the risk profile.
//!
//! ### Regime gating
//!
//! Disabled in `Trending` (no reversal) and `LiquidityCrisis` (sweep
//! signal noisy). Allowed in `Sideways`, `Panic`, `HighVolatility`,
//! `NewsDriven`, `LowParticipation` regimes.

use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::{build_signal, clamp01, risk_profile_for};
use crate::strategy::Strategy;

/// Minimum absolute `liquidity_sweep` magnitude required to fire.
pub const SWEEP_MIN: f32 = 0.5;

/// Minimum absolute `orderflow_strength` magnitude required to confirm.
pub const ORDERFLOW_STRENGTH_MIN: f32 = 0.2;

/// Liquidity_Sweep_Reversal strategy.
#[derive(Copy, Clone, Debug, Default)]
pub struct LiquiditySweepReversal;

impl Strategy for LiquiditySweepReversal {
    fn id(&self) -> StrategyId {
        StrategyId::LiquiditySweepReversal
    }

    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal> {
        let _ = ctx;

        if snap.liquidity_sweep.abs() < SWEEP_MIN {
            return None;
        }
        // Sweep upward (positive) → fade short. Sweep downward → fade long.
        let side = if snap.liquidity_sweep > 0.0 {
            Side::Sell
        } else {
            Side::Buy
        };

        // EMA slope must already point in the reversal direction (the
        // tape has flipped).
        match side {
            Side::Buy if snap.ema_slope <= 0.0 => return None,
            Side::Sell if snap.ema_slope >= 0.0 => return None,
            _ => {}
        }

        // Orderflow strength must confirm the side: positive for long
        // side fades after a downward sweep, negative for short side
        // fades after an upward sweep.
        let of = snap.orderflow_strength;
        if of.abs() < ORDERFLOW_STRENGTH_MIN {
            return None;
        }
        match side {
            Side::Buy if of <= 0.0 => return None,
            Side::Sell if of >= 0.0 => return None,
            _ => {}
        }

        // Confidence — sweep magnitude × orderflow strength.
        let sweep_mag = snap.liquidity_sweep.abs().min(1.0);
        let of_mag = of.abs().min(1.0);
        let confidence = clamp01(0.5 * sweep_mag + 0.5 * of_mag);
        let base_probability = clamp01(0.5 + 0.25 * sweep_mag + 0.25 * of_mag);

        // Use VWAP as the entry proxy — the reversal trade enters near the
        // post-sweep retrace level.
        let entry = if snap.vwap > 0 { snap.vwap } else { snap.ema_fast };
        Some(build_signal(
            snap.correlation_id,
            self.id(),
            SymbolId::new(snap.symbol),
            side,
            base_probability,
            confidence,
            risk_profile_for(entry, side, snap.atr),
            snap.ts_ns,
        ))
    }

    fn enabled_in(&self, regime: Regime) -> bool {
        !matches!(regime, Regime::Trending | Regime::LiquidityCrisis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{NewsGates, StrategyToggles};

    fn snap(
        liquidity_sweep: f32,
        ema_slope: f32,
        orderflow_strength: f32,
        atr: i64,
    ) -> FeatureSnapshot {
        FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap: 100_00,
            atr,
            ema_fast: 100_00,
            ema_slow: 100_00,
            ema_slope,
            realized_vol: 0.0,
            momentum: 0.0,
            rolling_delta: 0,
            liquidity_imbalance: 0.0,
            orderflow_strength,
            candle_structure: 0,
            breakout_pressure: 0.0,
            compression_zone: 0.0,
            liquidity_sweep,
            ts_ns: 0,
        }
    }

    fn ctx<'a>(toggles: &'a StrategyToggles, news: &'a NewsGates) -> StrategyContext<'a> {
        StrategyContext {
            regime: Regime::Sideways,
            trader_config: toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: news,
        }
    }

    #[test]
    fn fires_short_after_upward_sweep_with_negative_slope_and_orderflow() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.8, -0.4, -0.5, 100);
        let sig = LiquiditySweepReversal.evaluate(&s, &c).expect("signal");
        assert_eq!(sig.side, Side::Sell.as_u8());
    }

    #[test]
    fn fires_long_after_downward_sweep_with_positive_slope_and_orderflow() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(-0.8, 0.4, 0.5, 100);
        let sig = LiquiditySweepReversal.evaluate(&s, &c).expect("signal");
        assert_eq!(sig.side, Side::Buy.as_u8());
    }

    #[test]
    fn does_not_fire_when_sweep_below_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.4, -0.4, -0.5, 100);
        assert!(LiquiditySweepReversal.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_slope_does_not_confirm() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // Upward sweep but slope is positive (still trending up — not a
        // reversal).
        let s = snap(0.8, 0.4, -0.5, 100);
        assert!(LiquiditySweepReversal.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_orderflow_disagrees() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // Want short after upward sweep — but orderflow_strength is positive.
        let s = snap(0.8, -0.4, 0.5, 100);
        assert!(LiquiditySweepReversal.evaluate(&s, &c).is_none());
    }

    #[test]
    fn disabled_in_trending_and_liquidity_crisis() {
        assert!(!LiquiditySweepReversal.enabled_in(Regime::Trending));
        assert!(!LiquiditySweepReversal.enabled_in(Regime::LiquidityCrisis));
        assert!(LiquiditySweepReversal.enabled_in(Regime::Sideways));
        assert!(LiquiditySweepReversal.enabled_in(Regime::Panic));
        assert!(LiquiditySweepReversal.enabled_in(Regime::NewsDriven));
    }
}
