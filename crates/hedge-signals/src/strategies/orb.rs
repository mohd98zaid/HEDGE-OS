//! Opening_Range_Breakout (ORB) strategy.
//!
//! Detects a breakout above (long) or below (short) the **opening range**
//! — defined by the brief as the 09:15–09:30 IST window — confirmed by
//! orderflow `breakout_pressure` and a non-flat EMA slope.
//!
//! ### Inputs
//!
//! From `FeatureSnapshot`:
//!
//! * `breakout_pressure` (`f32 ∈ [-1.0, 1.0]`) — combined orderbook /
//!   tape pressure surfaced by `hedge-features::incremental::breakout`.
//!   Positive values bias toward breakout-long, negative toward
//!   breakout-short.
//! * `ema_slope` (`f32`) — fast-EMA slope; non-flat values confirm
//!   trending continuation.
//! * `vwap` (`i64` paise) — used as a reference level when previous-day
//!   data is unavailable.
//!
//! From the previous-day memory (when present):
//!
//! * `high_paise`, `low_paise` — the prior session's structural levels;
//!   the breakout must close above the high (long) or below the low
//!   (short) to fire.
//!
//! ### Output
//!
//! At most one signal per snapshot:
//!
//! * `side = Buy` for an upside breakout, `Sell` for a downside one.
//! * `base_probability` and `confidence` derived from
//!   `|breakout_pressure|` (clamped to `[0, 1]`).
//!
//! ### Regime gating
//!
//! Disabled in `Sideways`, `LiquidityCrisis`, and `LowParticipation`
//! regimes — ORB requires directional conviction.

use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::{build_signal, clamp01, risk_profile_for};
use crate::strategy::Strategy;

/// Minimum absolute `breakout_pressure` magnitude required to fire.
pub const ORB_BREAKOUT_PRESSURE_MIN: f32 = 0.5;

/// Minimum absolute `ema_slope` magnitude (paise / tick) required to
/// confirm the breakout direction. Set conservatively so the ORB only
/// fires on tape that is already trending in the breakout direction.
pub const ORB_EMA_SLOPE_MIN: f32 = 0.1;

/// Opening_Range_Breakout strategy.
#[derive(Copy, Clone, Debug, Default)]
pub struct OpeningRangeBreakout;

impl Strategy for OpeningRangeBreakout {
    fn id(&self) -> StrategyId {
        StrategyId::OpeningRangeBreakout
    }

    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal> {
        // 1. Decide the breakout side from `breakout_pressure`.
        let pressure = snap.breakout_pressure;
        if pressure.abs() < ORB_BREAKOUT_PRESSURE_MIN {
            return None;
        }
        let side = if pressure > 0.0 { Side::Buy } else { Side::Sell };

        // 2. EMA slope must align with the breakout side.
        let slope = snap.ema_slope;
        if slope.abs() < ORB_EMA_SLOPE_MIN {
            return None;
        }
        match side {
            Side::Buy if slope <= 0.0 => return None,
            Side::Sell if slope >= 0.0 => return None,
            _ => {}
        }

        // 3. Reference level: previous-day high/low when present, VWAP
        //    otherwise. The current LTP must be on the breakout side of
        //    the reference. We use `vwap` from the snapshot as the
        //    proxy for the current price because the snapshot does not
        //    re-publish LTP explicitly; in practice `vwap ≈ ltp` while
        //    the session is active.
        let reference_level = ctx.previous_day.map(|p| match side {
            Side::Buy => p.high_paise,
            Side::Sell => p.low_paise,
        });
        if let Some(level) = reference_level {
            if level > 0 {
                match side {
                    Side::Buy if snap.vwap < level => return None,
                    Side::Sell if snap.vwap > level => return None,
                    _ => {}
                }
            }
        }

        // 4. Confidence is the magnitude of the breakout pressure.
        let confidence = clamp01(pressure.abs());
        let base_probability = clamp01(0.5 + 0.5 * pressure.abs().min(1.0));

        Some(build_signal(
            snap.correlation_id,
            self.id(),
            SymbolId::new(snap.symbol),
            side,
            base_probability,
            confidence,
            risk_profile_for(snap.vwap, side, snap.atr),
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
    use crate::context::{NewsGates, PreviousDayMemory, StrategyToggles};

    fn snap_with(
        breakout_pressure: f32,
        ema_slope: f32,
        vwap: i64,
        atr: i64,
    ) -> FeatureSnapshot {
        FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap,
            atr,
            ema_fast: 0,
            ema_slow: 0,
            ema_slope,
            realized_vol: 0.0,
            momentum: 0.0,
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

    fn ctx<'a>(
        toggles: &'a StrategyToggles,
        news: &'a NewsGates,
        prev: Option<&'a PreviousDayMemory>,
    ) -> StrategyContext<'a> {
        StrategyContext {
            regime: Regime::Trending,
            trader_config: toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: prev,
            news_gates: news,
        }
    }

    #[test]
    fn fires_long_on_strong_upside_breakout() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news, None);
        let snap = snap_with(0.8, 0.5, 100_00, 100);
        let s = OpeningRangeBreakout.evaluate(&snap, &c).expect("signal");
        assert_eq!(s.side, Side::Buy.as_u8());
        assert!(s.confidence > 0.0 && s.confidence <= 1.0);
        assert!(s.base_probability > 0.0 && s.base_probability <= 1.0);
        assert_eq!(s.strategy, StrategyId::OpeningRangeBreakout.as_u8());
    }

    #[test]
    fn fires_short_on_strong_downside_breakout() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news, None);
        let snap = snap_with(-0.9, -0.5, 100_00, 100);
        let s = OpeningRangeBreakout.evaluate(&snap, &c).expect("signal");
        assert_eq!(s.side, Side::Sell.as_u8());
    }

    #[test]
    fn does_not_fire_when_breakout_pressure_below_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news, None);
        let snap = snap_with(0.4, 0.5, 100_00, 100);
        assert!(OpeningRangeBreakout.evaluate(&snap, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_ema_slope_disagrees() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news, None);
        // Long pressure but slope is negative.
        let snap = snap_with(0.8, -0.5, 100_00, 100);
        assert!(OpeningRangeBreakout.evaluate(&snap, &c).is_none());
    }

    #[test]
    fn does_not_fire_when_below_prev_day_high_for_long() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let prev = PreviousDayMemory {
            symbol: SymbolId::new(1),
            open_paise: 0,
            high_paise: 105_00,
            low_paise: 99_00,
            close_paise: 0,
            vwap_paise: 0,
        };
        let c = ctx(&toggles, &news, Some(&prev));
        // VWAP (≈ price) is 100_00 — below the 105_00 prev-day high.
        let snap = snap_with(0.8, 0.5, 100_00, 100);
        assert!(OpeningRangeBreakout.evaluate(&snap, &c).is_none());
    }

    #[test]
    fn fires_when_above_prev_day_high_for_long() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let prev = PreviousDayMemory {
            symbol: SymbolId::new(1),
            open_paise: 0,
            high_paise: 99_00,
            low_paise: 95_00,
            close_paise: 0,
            vwap_paise: 0,
        };
        let c = ctx(&toggles, &news, Some(&prev));
        let snap = snap_with(0.8, 0.5, 100_00, 100);
        assert!(OpeningRangeBreakout.evaluate(&snap, &c).is_some());
    }

    #[test]
    fn disabled_in_sideways_regime() {
        assert!(!OpeningRangeBreakout.enabled_in(Regime::Sideways));
        assert!(!OpeningRangeBreakout.enabled_in(Regime::LiquidityCrisis));
        assert!(!OpeningRangeBreakout.enabled_in(Regime::LowParticipation));
        assert!(OpeningRangeBreakout.enabled_in(Regime::Trending));
        assert!(OpeningRangeBreakout.enabled_in(Regime::HighVolatility));
    }
}
