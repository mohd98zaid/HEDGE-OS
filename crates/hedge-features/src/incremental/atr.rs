//! Incremental ATR — Average True Range over 14 bars (R3.1).
//!
//! True range for a single bar is
//! `TR = max(high - low, |high - prev_close|, |low - prev_close|)`.
//! The classical Wilder ATR averages 14 successive `TR` values; we
//! simplify to the arithmetic mean of the rolling 14-element window
//! held inside `state.tr_window`. The Signal_Engine consumes ATR as a
//! magnitude (used by VWAP_Pullback and Volatility_Compression_Breakout),
//! so the difference between the two averaging methods is immaterial as
//! long as it is **deterministic**, **incremental**, and matches the
//! reference implementation we use in tests.
//!
//! ## Bar definition
//!
//! In the Hot_Path each tick is treated as a 1-tick bar where
//! `high == low == ltp`; the previous-close pivot then yields a per-tick
//! true range. This keeps ATR(14) responsive to the per-tick stream
//! without requiring an external bar timer.

use hedge_schemas::Tick;

use crate::state::{FeatureState, ATR_WINDOW};

/// Fold a tick into the ATR window.
///
/// On the first tick the previous close is unknown — we seed it from the
/// current LTP, which produces a `TR == 0` for the first sample. This
/// matches the reference implementation in tests.
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    let high = tick.ltp_paise;
    let low = tick.ltp_paise;
    let prev_close = if state.tick_count == 0 {
        tick.ltp_paise
    } else {
        state.prev_close_paise
    };

    let hl = (high - low).abs();
    let hpc = (high - prev_close).abs();
    let lpc = (low - prev_close).abs();
    let tr = hl.max(hpc).max(lpc);

    state.tr_window.push(tr);
    state.prev_close_paise = tick.ltp_paise;
    state.bar_high_paise = high;
    state.bar_low_paise = low;
}

/// Returns ATR in paise — arithmetic mean of the 14 most recent true
/// ranges. `0` while [`is_ready`] is false.
#[inline]
pub fn compute_paise(state: &FeatureState) -> i64 {
    if state.tr_window.is_empty() {
        return 0;
    }
    let mut sum: i128 = 0;
    let n = state.tr_window.len() as i128;
    for v in state.tr_window.iter() {
        sum += *v as i128;
    }
    (sum / n) as i64
}

/// Returns ATR as `f32` (paise units).
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    compute_paise(state) as f32
}

/// `true` once the 14-element window has been filled at least once.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.tr_window.len() >= ATR_WINDOW
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick_with_count;
    use proptest::prelude::*;

    /// Reference: arithmetic mean of true ranges over the last 14 bars.
    fn reference_atr_paise(prices: &[i64]) -> i64 {
        if prices.is_empty() {
            return 0;
        }
        let mut tr: Vec<i64> = Vec::with_capacity(prices.len());
        for (i, &p) in prices.iter().enumerate() {
            let prev_close = if i == 0 { p } else { prices[i - 1] };
            let hl = 0i64; // high == low == ltp on tick-bars
            let hpc = (p - prev_close).abs();
            let lpc = (p - prev_close).abs();
            tr.push(hl.max(hpc).max(lpc));
        }
        let take = tr.len().min(ATR_WINDOW);
        let window = &tr[tr.len() - take..];
        let sum: i128 = window.iter().map(|x| *x as i128).sum();
        (sum / window.len() as i128) as i64
    }

    #[test]
    fn atr_zero_before_window_filled() {
        let s = FeatureState::default();
        assert!(!is_ready(&s));
        assert_eq!(compute_paise(&s), 0);
    }

    #[test]
    fn atr_first_tick_seeds_zero_tr() {
        let mut s = FeatureState::default();
        update(&mut s, &tick_with_count(100_00, 0));
        // First TR is zero (high == low == prev_close == 100_00).
        assert_eq!(compute_paise(&s), 0);
        assert!(!is_ready(&s));
    }

    #[test]
    fn atr_after_14_ticks_matches_reference() {
        let mut s = FeatureState::default();
        let prices: Vec<i64> = (0..14).map(|i| 100_00 + i as i64 * 50).collect();
        for (i, p) in prices.iter().enumerate() {
            s.tick_count = i as u64;
            update(&mut s, &tick_with_count(*p, i as u64));
        }
        assert!(is_ready(&s));
        assert_eq!(compute_paise(&s), reference_atr_paise(&prices));
    }

    #[test]
    fn atr_matches_reference_under_random_inputs_property() {
        let runner = &mut proptest::test_runner::TestRunner::default();
        let strategy = proptest::collection::vec(1_00i64..=1_000_000i64, 1..200);
        runner
            .run(&strategy, |prices| {
                let mut state = FeatureState::default();
                for (i, p) in prices.iter().enumerate() {
                    state.tick_count = i as u64;
                    update(&mut state, &tick_with_count(*p, i as u64));
                }
                let got = compute_paise(&state);
                let expected = reference_atr_paise(&prices);
                prop_assert_eq!(got, expected);
                Ok(())
            })
            .unwrap();
    }
}
