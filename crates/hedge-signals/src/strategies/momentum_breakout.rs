//! Momentum_Breakout strategy.
//!
//! Combines `momentum`, `breakout_pressure`, and the EMA fast/slow
//! alignment to detect a directional breakout backed by tape momentum.
//!
//! ### Inputs
//!
//! * `momentum` (f32) — fast-EMA log-return slope; positive = up,
//!   negative = down.
//! * `breakout_pressure` (f32) — combined orderbook / tape pressure.
//! * `ema_slope` (f32), `ema_fast`, `ema_slow` (i64 paise) — slope and
//!   alignment confirm the trend direction.
//! * `atr` (i64 paise) — used for the risk profile.
//!
//! ### Preconditions
//!
//! 1. `|momentum| ≥ MOMENTUM_MIN`.
//! 2. `|breakout_pressure| ≥ BREAKOUT_PRESSURE_MIN`.
//! 3. EMA fast/slow alignment matches the side
//!    (`ema_fast > ema_slow` for long).
//! 4. Sign of momentum, breakout_pressure, and EMA slope all agree.
//!
//! ### Regime gating
//!
//! Disabled in `Sideways`, `LiquidityCrisis`, `LowParticipation`.

use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::{build_signal, clamp01, risk_profile_for};
use crate::strategy::Strategy;

/// Minimum absolute `momentum` required to fire.
pub const MOMENTUM_MIN: f32 = 0.005;

/// Minimum absolute `breakout_pressure` required to fire.
pub const BREAKOUT_PRESSURE_MIN: f32 = 0.5;

/// Momentum_Breakout strategy.
#[derive(Copy, Clone, Debug, Default)]
pub struct MomentumBreakout;

impl Strategy for MomentumBreakout {
    fn id(&self) -> StrategyId {
        StrategyId::MomentumBreakout
    }

    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal> {
        let _ = ctx; // No regime-specific tuning beyond `enabled_in`.

        if snap.momentum.abs() < MOMENTUM_MIN {
            return None;
        }
        if snap.breakout_pressure.abs() < BREAKOUT_PRESSURE_MIN {
            return None;
        }
        // Direction must align across momentum, breakout_pressure, and
        // ema_slope.
        let side = if snap.momentum > 0.0 && snap.breakout_pressure > 0.0 && snap.ema_slope > 0.0 {
            Side::Buy
        } else if snap.momentum < 0.0
            && snap.breakout_pressure < 0.0
            && snap.ema_slope < 0.0
        {
            Side::Sell
        } else {
            return None;
        };

        // EMA alignment confirms the trend.
        match side {
            Side::Buy if snap.ema_fast <= snap.ema_slow => return None,
            Side::Sell if snap.ema_fast >= snap.ema_slow => return None,
            _ => {}
        }

        // Confidence — combine pressure magnitude and momentum magnitude.
        let pressure = snap.breakout_pressure.abs().min(1.0);
        // Saturate momentum at 0.05 (5% per-window log return is a hard
        // upper for intraday in INR equities).
        let mom_norm = (snap.momentum.abs() / 0.05).min(1.0);
        let confidence = clamp01(0.5 * pressure + 0.5 * mom_norm);
        let base_probability = clamp01(0.5 + 0.25 * pressure + 0.25 * mom_norm);

        let entry = if snap.ema_fast > 0 { snap.ema_fast } else { snap.vwap };
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

    fn snap(
        momentum: f32,
        breakout_pressure: f32,
        ema_slope: f32,
        ema_fast: i64,
        ema_slow: i64,
        atr: i64,
    ) -> FeatureSnapshot {
        FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap: 100_00,
            atr,
            ema_fast,
            ema_slow,
            ema_slope,
            realized_vol: 0.0,
            momentum,
            rolling_delta: 0,
            liquidity_imbalance: 0.0,
            orderflow_strength: 0.0,
            candle_structure: 0,
            breakout_pressure,
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
    fn fires_long_when_all_signals_align_up() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.02, 0.7, 0.3, 100_50, 100_00, 100);
        assert_eq!(
            MomentumBreakout.evaluate(&s, &c).unwrap().side,
            Side::Buy.as_u8()
        );
    }

    #[test]
    fn fires_short_when_all_signals_align_down() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(-0.02, -0.7, -0.3, 99_50, 100_00, 100);
        assert_eq!(
            MomentumBreakout.evaluate(&s, &c).unwrap().side,
            Side::Sell.as_u8()
        );
    }

    #[test]
    fn does_not_fire_when_momentum_below_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.001, 0.8, 0.3, 100_50, 100_00, 100);
        assert!(MomentumBreakout.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_breakout_pressure_below_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = snap(0.02, 0.3, 0.3, 100_50, 100_00, 100);
        assert!(MomentumBreakout.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_signs_disagree() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // Momentum up but pressure down.
        let s = snap(0.02, -0.7, 0.3, 100_50, 100_00, 100);
        assert!(MomentumBreakout.evaluate(&s, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_ema_alignment_disagrees() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        // All signals up but EMA fast < EMA slow.
        let s = snap(0.02, 0.7, 0.3, 99_00, 100_00, 100);
        assert!(MomentumBreakout.evaluate(&s, &c).is_none());
    }

    #[test]
    fn disabled_in_sideways_regime() {
        assert!(!MomentumBreakout.enabled_in(Regime::Sideways));
        assert!(!MomentumBreakout.enabled_in(Regime::LiquidityCrisis));
        assert!(!MomentumBreakout.enabled_in(Regime::LowParticipation));
        assert!(MomentumBreakout.enabled_in(Regime::Trending));
    }
}
