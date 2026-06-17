//! Helpers shared across every concrete strategy.
//!
//! The helpers are deliberately tiny and `#[inline]` so the compiler can
//! eliminate the call overhead — strategies sit on the Hot_Path and
//! every nanosecond counts.

use hedge_core::Side;
use hedge_schemas::{RiskProfile, Signal};

use crate::context::StrategyContext;

/// Default `max_size_qty` field on a strategy-emitted [`RiskProfile`].
/// The Risk_Engine sizes the actual quantity using `Adaptive_Risk` —
/// the strategy's value is the **upper bound** the engine honours.
pub const DEFAULT_MAX_SIZE_QTY: u64 = 100;

/// Default `time_horizon_seconds` on a strategy-emitted [`RiskProfile`].
/// Five minutes matches the brief's "intraday default" guidance.
pub const DEFAULT_TIME_HORIZON_SECONDS: u32 = 300;

/// `1.5 × ATR` stop-loss multiplier from the brief.
pub const STOP_LOSS_ATR_MULT: f64 = 1.5;

/// `2.5 × ATR` take-profit multiplier from the brief.
pub const TAKE_PROFIT_ATR_MULT: f64 = 2.5;

/// Clamp a float into `[0.0, 1.0]`, treating NaN as `0.0`.
///
/// Used at the Signal-construction boundary so a malformed feature
/// (e.g. a `breakout_pressure` that briefly went outside its bounds
/// during numerical edge cases) cannot leak past the type-level
/// guarantee `base_probability ∈ [0.0, 1.0]` (R4.3).
#[inline]
pub fn clamp01(v: f32) -> f32 {
    if v.is_nan() || v < 0.0 {
        0.0
    } else if v > 1.0 {
        1.0
    } else {
        v
    }
}

/// Build a `RiskProfile` from the entry price, side, and ATR.
///
/// `stop_loss_paise = entry - 1.5 × ATR` (long) or `entry + 1.5 × ATR`
/// (short); `take_profit_paise = entry ± 2.5 × ATR`. ATR is sourced from
/// `FeatureSnapshot.atr` (paise).
///
/// `max_size_qty = DEFAULT_MAX_SIZE_QTY = 100`.
/// `time_horizon_seconds = DEFAULT_TIME_HORIZON_SECONDS = 300`.
pub fn risk_profile_for(entry_paise: i64, side: Side, atr_paise: i64) -> RiskProfile {
    // ATR is in paise; the multipliers are floats, so we widen to f64 for
    // the multiplication and round back to i64. The ATR is a price-domain
    // distance and never negative; we still saturating-add/sub so a
    // pathological zero-ATR snapshot does not wrap the i64.
    let stop_dist = (atr_paise as f64 * STOP_LOSS_ATR_MULT).round() as i64;
    let tp_dist = (atr_paise as f64 * TAKE_PROFIT_ATR_MULT).round() as i64;
    let (stop_loss, take_profit) = match side {
        Side::Buy => (
            entry_paise.saturating_sub(stop_dist),
            entry_paise.saturating_add(tp_dist),
        ),
        Side::Sell => (
            entry_paise.saturating_add(stop_dist),
            entry_paise.saturating_sub(tp_dist),
        ),
    };
    RiskProfile {
        stop_loss_paise: stop_loss,
        take_profit_paise: take_profit,
        max_size_qty: DEFAULT_MAX_SIZE_QTY,
        time_horizon_seconds: DEFAULT_TIME_HORIZON_SECONDS,
    }
}

/// Collapse a [`Side`] into the wire-form `u8` carried in
/// `Signal_v1.side`.
#[inline]
pub const fn side_byte(side: Side) -> u8 {
    side.as_u8()
}

/// Build a clamped [`Signal`] from strategy outputs.
///
/// The single canonical helper for signal construction. Defence-in-depth
/// clamping enforces R4.3 (`base_probability ∈ [0, 1]`,
/// `confidence ∈ [0, 1]`) at the type-level boundary so a strategy that
/// computes a slightly out-of-band value cannot leak it onto the wire.
#[allow(clippy::too_many_arguments)]
pub fn build_signal(
    correlation_id: [u8; 16],
    strategy_id: hedge_schemas::strategy_id::StrategyId,
    symbol: hedge_core::SymbolId,
    side: Side,
    base_probability: f32,
    confidence: f32,
    risk_profile: RiskProfile,
    ts_ns: u64,
) -> Signal {
    Signal {
        correlation_id,
        strategy: strategy_id.as_u8(),
        symbol: symbol.raw(),
        side: side_byte(side),
        base_probability: clamp01(base_probability),
        confidence: clamp01(confidence),
        risk_profile,
        ts_ns,
    }
}

/// Convenience accessor: returns `true` when the war-mode floor — when
/// active — would accept the candidate confidence. This lets a strategy
/// elide constructing a Signal it knows will be filtered out at the
/// post-evaluate war-mode gate.
#[inline]
pub fn meets_war_mode_floor(confidence: f32, ctx: &StrategyContext) -> bool {
    if !ctx.war_mode {
        return true;
    }
    confidence + f32::EPSILON >= ctx.war_mode_min_confidence
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp01_handles_nan_and_bounds() {
        assert_eq!(clamp01(0.5), 0.5);
        assert_eq!(clamp01(-1.0), 0.0);
        assert_eq!(clamp01(2.0), 1.0);
        assert_eq!(clamp01(f32::NAN), 0.0);
        assert_eq!(clamp01(0.0), 0.0);
        assert_eq!(clamp01(1.0), 1.0);
    }

    #[test]
    fn risk_profile_long_stops_below_entry_targets_above() {
        let p = risk_profile_for(10_000, Side::Buy, 100);
        // 1.5×100 = 150 stop, 2.5×100 = 250 tp.
        assert_eq!(p.stop_loss_paise, 9_850);
        assert_eq!(p.take_profit_paise, 10_250);
        assert_eq!(p.max_size_qty, 100);
        assert_eq!(p.time_horizon_seconds, 300);
    }

    #[test]
    fn risk_profile_short_stops_above_entry_targets_below() {
        let p = risk_profile_for(10_000, Side::Sell, 100);
        assert_eq!(p.stop_loss_paise, 10_150);
        assert_eq!(p.take_profit_paise, 9_750);
    }

    #[test]
    fn side_byte_matches_enum_discriminant() {
        assert_eq!(side_byte(Side::Buy), 0);
        assert_eq!(side_byte(Side::Sell), 1);
    }
}
