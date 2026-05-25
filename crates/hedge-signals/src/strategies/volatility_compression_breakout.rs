//! Volatility_Compression_Breakout strategy.
//!
//! Fires when the Feature_Extraction_Engine reports the symbol is in a
//! tight `compression_zone` AND a fresh `breakout_pressure` is starting
//! to release the compression. Both indicators are floats published on
//! `feat.update.<sym>` (R3.2).
//!
//! ### Inputs
//!
//! * `compression_zone` (f32 ∈ [0, 1]) — high values = tight range
//!   sustained over the configured 20-tick window.
//! * `breakout_pressure` (f32 ∈ [-1, 1]) — direction + magnitude of the
//!   developing breakout.
//! * `atr` (i64 paise) — used for the risk profile.
//!
//! ### Preconditions
//!
//! 1. `compression_zone > 0.5` (tight range observed).
//! 2. `|breakout_pressure| > 0.5` (release in motion).
//! 3. Sign of `breakout_pressure` selects the side.
//!
//! ### Regime gating
//!
//! Disabled in `Trending` (no compression to break) and `Panic`
//! (everything is volatile already, the indicator is meaningless).

use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::{build_signal, clamp01, risk_profile_for};
use crate::strategy::Strategy;

/// Minimum `compression_zone` value that qualifies as "tight".
pub const COMPRESSION_MIN: f32 = 0.5;

/// Minimum absolute `breakout_pressure` magnitude required to fire.
pub const BREAKOUT_PRESSURE_MIN: f32 = 0.5;

/// Volatility_Compression_Breakout strategy.
#[derive(Copy, Clone, Debug, Default)]
pub struct VolatilityCompressionBreakout;

impl Strategy for VolatilityCompressionBreakout {
    fn id(&self) -> StrategyId {
        StrategyId::VolatilityCompressionBreakout
    }

    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal> {
        let _ = ctx;

        if snap.compression_zone <= COMPRESSION_MIN {
            return None;
        }
        if snap.breakout_pressure.abs() <= BREAKOUT_PRESSURE_MIN {
            return None;
        }
        let side = if snap.breakout_pressure > 0.0 {
            Side::Buy
        } else {
            Side::Sell
        };

        let comp = snap.compression_zone.min(1.0).max(0.0);
        let pres = snap.breakout_pressure.abs().min(1.0);
        let confidence = clamp01(0.5 * comp + 0.5 * pres);
        let base_probability = clamp01(0.5 + 0.25 * comp + 0.25 * pres);

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
        !matches!(regime, Regime::Trending | Regime::Panic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{NewsGates, StrategyToggles};

    fn snap(compression_zone: f32, breakout_pressure: f32) -> FeatureSnapshot {
        FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap: 100_00,
            atr: 100,
            ema_fast: 100_00,
            ema_slow: 100_00,
            ema_slope: 0.0,
            realized_vol: 0.0,
            momentum: 0.0,
            rolling_delta: 0,
            liquidity_imbalance: 0.0,
            orderflow_strength: 0.0,
            candle_structure: 0,
            breakout_pressure,
            compression_zone,
            liquidity_sweep: 0.0,
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
    fn fires_long_on_high_compression_and_positive_pressure() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.8, 0.7);
        let sig = VolatilityCompressionBreakout.evaluate(&s, &c).expect("signal");
        assert_eq!(sig.side, Side::Buy.as_u8());
    }

    #[test]
    fn fires_short_on_high_compression_and_negative_pressure() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.8, -0.7);
        let sig = VolatilityCompressionBreakout.evaluate(&s, &c).expect("signal");
        assert_eq!(sig.side, Side::Sell.as_u8());
    }

    #[test]
    fn does_not_fire_when_compression_below_threshold() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.4, 0.7);
        assert!(VolatilityCompressionBreakout.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_pressure_below_threshold() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.8, 0.4);
        assert!(VolatilityCompressionBreakout.evaluate(&s, &c).is_none());
    }

    #[test]
    fn disabled_in_trending_and_panic() {
        assert!(!VolatilityCompressionBreakout.enabled_in(Regime::Trending));
        assert!(!VolatilityCompressionBreakout.enabled_in(Regime::Panic));
        assert!(VolatilityCompressionBreakout.enabled_in(Regime::Sideways));
        assert!(VolatilityCompressionBreakout.enabled_in(Regime::HighVolatility));
        assert!(VolatilityCompressionBreakout.enabled_in(Regime::LowParticipation));
    }
}
