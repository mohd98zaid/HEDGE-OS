//! Candle-structure classifier (R3.2).
//!
//! Each tick is treated as a 1-tick candle in the live Hot_Path, so the
//! classifier maps the relationship between `open`, `high`, `low`,
//! `close` (= LTP) and the prior LTP into one of six structural shapes.
//! The `FeatureSnapshot_v1.candle_structure` field is `ubyte`, so the
//! result is encoded as a stable [`CandleStructure::as_u8`] discriminant.
//!
//! ## Convention
//!
//! Per-tick classification uses:
//!
//! * `open  = state.prev_ltp_paise` (last tick's LTP)
//! * `close = tick.ltp_paise`
//! * `high  = max(open, close)` and
//! * `low   = min(open, close)`
//!
//! On the very first tick (`prev_ltp_paise == 0`) the classifier returns
//! [`CandleStructure::Doji`] because there is no movement to describe.

use hedge_schemas::Tick;

use crate::state::FeatureState;

/// Stable single-byte discriminant for [`FeatureSnapshot_v1.candle_structure`].
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum CandleStructure {
    /// `|open - close|` is negligible relative to `high - low`.
    Doji = 0,
    /// Body fills almost the entire range — strong directional candle.
    Marubozu = 1,
    /// Long lower wick, small body, close near top.
    Hammer = 2,
    /// Long upper wick, small body, close near bottom.
    InvertedHammer = 3,
    /// Long upper shadow that does not qualify as InvertedHammer.
    LongUpperShadow = 4,
    /// Long lower shadow that does not qualify as Hammer.
    LongLowerShadow = 5,
}

impl CandleStructure {
    /// Wire-form discriminant.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Round-trip from the wire `u8` discriminant. `None` for unknown
    /// values so deserializers flag schema-evolution mismatches.
    #[inline]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Doji),
            1 => Some(Self::Marubozu),
            2 => Some(Self::Hammer),
            3 => Some(Self::InvertedHammer),
            4 => Some(Self::LongUpperShadow),
            5 => Some(Self::LongLowerShadow),
            _ => None,
        }
    }
}

/// Per-tick classifier — folds the new tick into the state and updates
/// the `compression_prices` window so the breakout / compression / sweep
/// modules can read a coherent series of LTPs.
///
/// `update` itself only stores the LTP; classification happens in
/// [`compute`] so the engine's hot loop can run all incremental
/// state-mutating steps before any classifier reads.
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    state.compression_prices.push(tick.ltp_paise);

    // Track session high/low for the breakout-pressure module.
    if state.session_high_paise == 0 || tick.ltp_paise > state.session_high_paise {
        state.session_high_paise = tick.ltp_paise;
    }
    if state.session_low_paise == 0 || tick.ltp_paise < state.session_low_paise {
        state.session_low_paise = tick.ltp_paise;
    }
}

/// Classify the most recent two-tick movement.
pub fn classify(state: &FeatureState) -> CandleStructure {
    let close = state.last_ltp_paise;
    let open = state.prev_ltp_paise;
    if close == 0 || open == 0 {
        return CandleStructure::Doji;
    }
    let high = open.max(close);
    let low = open.min(close);
    let body = (close - open).abs();
    let range = (high - low).max(1); // avoid div-by-zero
    let body_ratio = body as f64 / range as f64;

    // Doji — no body, no range.
    if body == 0 {
        return CandleStructure::Doji;
    }

    // Marubozu — body fills > 95% of range.
    if body_ratio > 0.95 {
        return CandleStructure::Marubozu;
    }

    let upper_wick = (high - close.max(open)) as f64;
    let lower_wick = (close.min(open) - low) as f64;
    let body_f = body as f64;
    // Hammer — long lower wick, small body, close near high.
    if lower_wick > 2.0 * body_f && upper_wick < body_f {
        return CandleStructure::Hammer;
    }
    if upper_wick > 2.0 * body_f && lower_wick < body_f {
        return CandleStructure::InvertedHammer;
    }
    if upper_wick > lower_wick * 1.5 {
        return CandleStructure::LongUpperShadow;
    }
    if lower_wick > upper_wick * 1.5 {
        return CandleStructure::LongLowerShadow;
    }
    CandleStructure::Doji
}

/// Schema-facing `compute` returns the wire-form discriminant.
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    classify(state).as_u8() as f32
}

/// `true` once at least two ticks have been observed.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.last_ltp_paise > 0 && state.prev_ltp_paise > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick;

    fn classify_with(prev: i64, last: i64) -> CandleStructure {
        let mut s = FeatureState::default();
        s.prev_ltp_paise = prev;
        s.last_ltp_paise = last;
        classify(&s)
    }

    #[test]
    fn doji_when_no_movement() {
        assert_eq!(classify_with(100_00, 100_00), CandleStructure::Doji);
    }

    #[test]
    fn marubozu_for_strong_directional_move() {
        // Body fills entire range when high == close, low == open (long candle).
        assert_eq!(classify_with(100_00, 105_00), CandleStructure::Marubozu);
    }

    #[test]
    fn classifier_handles_unseeded_state() {
        let s = FeatureState::default();
        assert_eq!(classify(&s), CandleStructure::Doji);
    }

    #[test]
    fn discriminant_roundtrip_through_u8() {
        for v in [
            CandleStructure::Doji,
            CandleStructure::Marubozu,
            CandleStructure::Hammer,
            CandleStructure::InvertedHammer,
            CandleStructure::LongUpperShadow,
            CandleStructure::LongLowerShadow,
        ] {
            assert_eq!(CandleStructure::from_u8(v.as_u8()), Some(v));
        }
        assert_eq!(CandleStructure::from_u8(99), None);
    }

    #[test]
    fn update_grows_compression_window_in_lockstep() {
        let mut s = FeatureState::default();
        for i in 0..5i64 {
            update(&mut s, &tick(100_00 + i * 10, 1));
        }
        assert_eq!(s.compression_prices.len(), 5);
    }
}
