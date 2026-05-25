//! VWAP_Pullback continuation strategy.
//!
//! Detects a pullback to VWAP from above (long) or from below (short)
//! that bounces with continuation. The bounce is confirmed by the
//! fast-EMA crossing above (long) or below (short) the slow-EMA, which
//! means the trend is still intact even though price has briefly
//! retraced.
//!
//! ### Inputs (from `FeatureSnapshot`)
//!
//! * `vwap` (i64 paise) — session VWAP, the pullback target.
//! * `ema_fast` (i64 paise), `ema_slow` (i64 paise) — fast/slow EMAs.
//! * `atr` (i64 paise) — used for the pullback band tolerance.
//!
//! ### Long preconditions
//!
//! 1. EMA(fast) > EMA(slow) — uptrend intact.
//! 2. The mark price has touched VWAP from above and is now back above
//!    it (i.e. `ema_fast >= vwap` after a brief `ema_fast < vwap`).
//!    Because the snapshot does not carry the prior tick's EMA, we
//!    detect the pullback by requiring `|ema_fast - vwap| <= 0.5 × ATR`
//!    AND `ema_fast > vwap`.
//!
//! ### Short preconditions
//!
//! Mirror image: EMA(fast) < EMA(slow), `|ema_fast - vwap| <= 0.5 × ATR`
//! AND `ema_fast < vwap`.
//!
//! ### Regime gating
//!
//! Disabled in `Sideways` (no continuation expected) and
//! `LiquidityCrisis` / `LowParticipation` (insufficient flow to fill).

use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::{build_signal, clamp01, risk_profile_for};
use crate::strategy::Strategy;

/// Maximum distance from VWAP, expressed as a fraction of ATR, at which
/// a pullback still qualifies. Tight band — anything farther is treated
/// as a "trend continuation" rather than a pullback to VWAP.
pub const VWAP_PULLBACK_BAND_ATR_RATIO: f64 = 0.5;

/// VWAP_Pullback strategy.
#[derive(Copy, Clone, Debug, Default)]
pub struct VwapPullback;

impl Strategy for VwapPullback {
    fn id(&self) -> StrategyId {
        StrategyId::VwapPullback
    }

    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal> {
        // Need positive ATR to define the pullback band.
        if snap.atr <= 0 {
            return None;
        }
        let band = (snap.atr as f64 * VWAP_PULLBACK_BAND_ATR_RATIO) as i64;
        if band <= 0 {
            return None;
        }
        // The mark price proxy: ema_fast (cheaper than re-deriving).
        let mark = snap.ema_fast;
        // Distance from mark to VWAP.
        let dist = (mark - snap.vwap).abs();
        if dist > band {
            return None;
        }

        // Determine side from the EMA fast/slow alignment.
        let trend_long = snap.ema_fast > snap.ema_slow;
        let trend_short = snap.ema_fast < snap.ema_slow;
        if !trend_long && !trend_short {
            return None;
        }

        let side = if trend_long { Side::Buy } else { Side::Sell };

        // Require the pullback to bounce: mark must be on the trend side
        // of VWAP (above for long, below for short).
        match side {
            Side::Buy if mark < snap.vwap => return None,
            Side::Sell if mark > snap.vwap => return None,
            _ => {}
        }

        // Confidence — closer to VWAP and larger fast/slow gap → higher.
        let normalized_dist = (dist as f64 / band as f64) as f32; // [0, 1]
        let ema_gap = (snap.ema_fast - snap.ema_slow).abs() as f64;
        let normalized_gap = (ema_gap / (snap.atr as f64).max(1.0)).min(1.0) as f32;
        let confidence = clamp01(0.5 * (1.0 - normalized_dist) + 0.5 * normalized_gap);
        let base_probability = clamp01(0.5 + 0.25 * normalized_gap);

        let entry = mark;
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
        !matches!(
            regime,
            Regime::Sideways | Regime::LiquidityCrisis | Regime::LowParticipation
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{NewsGates, StrategyToggles};

    fn snap(vwap: i64, ema_fast: i64, ema_slow: i64, atr: i64) -> FeatureSnapshot {
        FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap,
            atr,
            ema_fast,
            ema_slow,
            ema_slope: 0.0,
            realized_vol: 0.0,
            momentum: 0.0,
            rolling_delta: 0,
            liquidity_imbalance: 0.0,
            orderflow_strength: 0.0,
            candle_structure: 0,
            breakout_pressure: 0.0,
            compression_zone: 0.0,
            liquidity_sweep: 0.0,
            ts_ns: 0,
        }
    }

    fn ctx<'a>(toggles: &'a StrategyToggles, news: &'a NewsGates) -> StrategyContext<'a> {
        StrategyContext {
            regime: Regime::Trending,
            trader_config: toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: news,
        }
    }

    #[test]
    fn fires_long_when_ema_fast_above_slow_and_near_vwap_from_above() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // VWAP=100_00, ema_fast=100_20 (above VWAP, within 0.5×ATR=50 band),
        // ema_slow=99_50 (so trend long).
        let s = snap(100_00, 100_20, 99_50, 100);
        let sig = VwapPullback.evaluate(&s, &c).expect("signal");
        assert_eq!(sig.side, Side::Buy.as_u8());
    }

    #[test]
    fn fires_short_when_ema_fast_below_slow_and_near_vwap_from_below() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(100_00, 99_80, 100_50, 100);
        let sig = VwapPullback.evaluate(&s, &c).expect("signal");
        assert_eq!(sig.side, Side::Sell.as_u8());
    }

    #[test]
    fn does_not_fire_when_distance_exceeds_band() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // Distance to VWAP is 200, band = 0.5×100 = 50.
        let s = snap(100_00, 102_00, 99_50, 100);
        assert!(VwapPullback.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_no_trend_alignment() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // EMA fast == EMA slow.
        let s = snap(100_00, 100_00, 100_00, 100);
        assert!(VwapPullback.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_long_when_mark_below_vwap() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // Long trend (ema_fast > ema_slow) but mark BELOW VWAP — no
        // bounce confirmation yet.
        let s = snap(100_00, 99_80, 99_00, 100);
        assert!(VwapPullback.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_atr_zero() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(100_00, 100_20, 99_50, 0);
        assert!(VwapPullback.evaluate(&s, &c).is_none());
    }

    #[test]
    fn disabled_in_sideways_regime() {
        assert!(!VwapPullback.enabled_in(Regime::Sideways));
        assert!(!VwapPullback.enabled_in(Regime::LiquidityCrisis));
        assert!(!VwapPullback.enabled_in(Regime::LowParticipation));
        assert!(VwapPullback.enabled_in(Regime::Trending));
    }
}
