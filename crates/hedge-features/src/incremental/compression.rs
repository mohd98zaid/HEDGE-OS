//! Compression-zone detector — `range / atr < 0.5` over 20 ticks.
//!
//! Source: design § Components § Feature_Extraction_Engine, R3.2.
//!
//! Returns a `f32` "tightness" score:
//!
//! ```text
//! range = max(p) - min(p) over last 20 ticks
//! ratio = range / atr
//! score = clamp(1 - ratio, 0, 1)        // 1.0 = maximally compressed
//! ```
//!
//! When the strict `ratio < 0.5` predicate is true, `score > 0.5`. The
//! Signal_Engine's Volatility_Compression_Breakout strategy can read
//! either the score directly or compare it to a threshold of 0.5.

use hedge_schemas::Tick;

use crate::state::{FeatureState, COMPRESSION_WINDOW};

use super::atr;

/// `update` is a no-op — the compression module reads from
/// `state.compression_prices`, which is filled by [`super::candle::update`].
#[inline]
pub fn update(_state: &mut FeatureState, _tick: &Tick) {}

/// Returns the compression score in `[0.0, 1.0]`.
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    if state.compression_prices.len() < COMPRESSION_WINDOW {
        return 0.0;
    }
    let mut hi = i64::MIN;
    let mut lo = i64::MAX;
    for v in state.compression_prices.iter() {
        if *v > hi {
            hi = *v;
        }
        if *v < lo {
            lo = *v;
        }
    }
    let range = (hi - lo) as f64;
    let atr_paise = atr::compute_paise(state) as f64;
    if atr_paise <= 0.0 {
        return 0.0;
    }
    let ratio = range / atr_paise;
    let score = 1.0 - ratio;
    score.clamp(0.0, 1.0) as f32
}

/// `true` once both ATR and the 20-tick price window are warm.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    atr::is_ready(state) && state.compression_prices.len() >= COMPRESSION_WINDOW
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ATR_WINDOW;

    #[test]
    fn compression_zero_before_warmup() {
        let s = FeatureState::default();
        assert_eq!(compute(&s), 0.0);
    }

    #[test]
    fn compression_high_when_range_smaller_than_atr() {
        // Drive the engine flow: 14 large-swing ticks build ATR, then
        // we directly clear the compression window and feed 20 tight
        // ticks. The compression module compares the RANGE of the
        // tight ticks against the prevailing ATR. Driving both windows
        // through the same state requires that the ATR window stays
        // populated with the earlier large swings while the compression
        // window slides over the tight ticks.
        //
        // Since `atr::update` also rolls with each tick, the two
        // windows align — meaning a true tick-by-tick "compression
        // ratio" is dominated by the recent variance. To exercise the
        // formula in isolation we seed the state directly: ATR window
        // holds 14 samples of size 200; compression prices oscillate by
        // ±1 paise.
        let mut s = FeatureState::default();
        for _ in 0..ATR_WINDOW {
            s.tr_window.push(200);
        }
        for i in 0..COMPRESSION_WINDOW as i64 {
            s.compression_prices.push(100_00 + (i % 2));
        }
        let score = compute(&s);
        // range = 1 paise, atr_paise = 200, ratio = 0.005, score = 0.995
        assert!(score > 0.5, "expected compression score > 0.5, got {}", score);
    }

    #[test]
    fn compression_low_when_range_dominates_atr() {
        let mut s = FeatureState::default();
        for _ in 0..ATR_WINDOW {
            s.tr_window.push(10);
        }
        for i in 0..COMPRESSION_WINDOW as i64 {
            s.compression_prices.push(100_00 + (i % 2) * 5_00);
        }
        // range = 500 paise, atr_paise = 10, ratio = 50 → score clamped to 0.
        let score = compute(&s);
        assert!(score < 0.5, "expected compression score < 0.5, got {}", score);
    }
}
