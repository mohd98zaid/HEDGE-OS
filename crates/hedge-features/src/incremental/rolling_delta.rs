//! Rolling signed-volume delta over a 30-second window.
//!
//! Source: design § Components § Feature_Extraction_Engine, R3.1
//! ("rolling delta"). The signed-volume convention follows the
//! Orderflow_Engine: positive for buyer-initiated, negative for seller-
//! initiated. Until the orderflow signature is wired into the
//! per-tick feed, we infer the signed quantity from the `total_buy_qty -
//! total_sell_qty` differential present on every `Tick_v1`.

use hedge_schemas::Tick;

use crate::state::{DeltaSample, FeatureState, ROLLING_DELTA_CAPACITY, ROLLING_DELTA_WINDOW_NS};

/// Push the new tick's signed volume into the 30 s window and update the
/// cached running sum.
///
/// Signed volume rule (placeholder until the Orderflow_Engine signs each
/// tick directly): if the `total_buy_qty` differential against the
/// previous tick is positive, classify as buyer-initiated; otherwise
/// seller-initiated. We use **only** the LTQ for the magnitude so the
/// metric stays interpretable as "trades / 30 s".
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    let signed_qty = sign_volume(state, tick);

    // Append the new sample.
    let new_sample = DeltaSample {
        ts_ns: tick.ts_recv_ns,
        signed_qty,
    };

    // Eviction: drop entries whose timestamp is older than the window.
    // Because `RingWindow` is FIFO with overwrite-on-full, we re-build
    // the cached sum from a fresh scan after pushing the new sample.
    state.delta_samples.push(new_sample);

    // Recompute the cached sum, skipping samples older than the window.
    let cutoff = tick.ts_recv_ns.saturating_sub(ROLLING_DELTA_WINDOW_NS);
    let mut sum: i64 = 0;
    for s in state.delta_samples.iter() {
        if s.ts_ns >= cutoff {
            sum = sum.saturating_add(s.signed_qty);
        }
    }
    state.rolling_delta_cached = sum;
}

/// Returns the rolling delta as `i64` (signed volume).
#[inline]
pub fn compute_paise(state: &FeatureState) -> i64 {
    state.rolling_delta_cached
}

/// Returns the rolling delta as `f32` (FlatBuffers schema declares
/// `rolling_delta: long`, so this is an information-preserving cast for
/// the downstream `compute` consumer; callers that need the integer
/// value should prefer [`compute_paise`]).
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    state.rolling_delta_cached as f32
}

/// `true` once at least one sample has been folded in.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    !state.delta_samples.is_empty()
}

/// Capacity reporter used by the `assert_no_alloc` harness to bound the
/// number of samples required for a 30 s window.
#[inline]
pub fn capacity() -> usize {
    ROLLING_DELTA_CAPACITY
}

fn sign_volume(state: &FeatureState, tick: &Tick) -> i64 {
    // Magnitude: ltq.
    let mag = tick.ltq.min(i64::MAX as u64) as i64;
    if mag == 0 {
        return 0;
    }
    // Sign: prefer the (buy_qty - sell_qty) differential against the
    // previous book; fall back to LTP up-tick / down-tick relative to
    // the *previous* tick's LTP. The engine flow rolls
    // `last_ltp_paise → prev_ltp_paise` and `tick.ltp_paise →
    // last_ltp_paise` AFTER every indicator runs, so during update
    // `state.last_ltp_paise` is the previous tick's LTP.
    let prev_buy = state.last_book.total_buy_qty as i128;
    let prev_sell = state.last_book.total_sell_qty as i128;
    let cur_buy = tick.total_buy_qty as i128;
    let cur_sell = tick.total_sell_qty as i128;
    let book_imbalance = (cur_buy - prev_buy) - (cur_sell - prev_sell);
    if book_imbalance > 0 {
        mag
    } else if book_imbalance < 0 {
        -mag
    } else if state.last_ltp_paise != 0 && tick.ltp_paise > state.last_ltp_paise {
        mag
    } else if state.last_ltp_paise != 0 && tick.ltp_paise < state.last_ltp_paise {
        -mag
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick_full;

    #[test]
    fn rolling_delta_zero_before_any_tick() {
        let s = FeatureState::default();
        assert!(!is_ready(&s));
        assert_eq!(compute_paise(&s), 0);
    }

    #[test]
    fn rolling_delta_positive_on_uptick() {
        let mut s = FeatureState::default();
        // Engine bookkeeping mirror — `last_ltp_paise` is the previous
        // tick's LTP from the rolling-delta module's perspective.
        s.last_ltp_paise = 100_00;
        let t = tick_full(101_00, 5, 100, 0, 0, 1_000_000_000);
        update(&mut s, &t);
        assert_eq!(compute_paise(&s), 5);
    }

    #[test]
    fn rolling_delta_negative_on_downtick() {
        let mut s = FeatureState::default();
        s.last_ltp_paise = 100_00;
        let t = tick_full(99_00, 5, 100, 0, 0, 1_000_000_000);
        update(&mut s, &t);
        assert_eq!(compute_paise(&s), -5);
    }

    #[test]
    fn rolling_delta_zero_when_ltq_is_zero() {
        let mut s = FeatureState::default();
        s.last_ltp_paise = 100_00;
        let t = tick_full(101_00, 0, 100, 0, 0, 1_000_000_000);
        update(&mut s, &t);
        assert_eq!(compute_paise(&s), 0);
    }

    #[test]
    fn rolling_delta_evicts_samples_older_than_window() {
        let mut s = FeatureState::default();
        s.last_ltp_paise = 100_00;
        // First sample at t=0.
        let t1 = tick_full(101_00, 10, 100, 0, 0, 0);
        update(&mut s, &t1);
        assert_eq!(compute_paise(&s), 10);

        // Second sample at t=31s — outside the 30s window.
        s.last_ltp_paise = 101_00;
        let t2 = tick_full(102_00, 5, 100, 0, 0, 31_000_000_000);
        update(&mut s, &t2);
        // The first sample is now older than the cutoff (31s - 30s = 1s).
        assert_eq!(compute_paise(&s), 5);
    }

    #[test]
    fn rolling_delta_keeps_samples_inside_window() {
        let mut s = FeatureState::default();
        s.last_ltp_paise = 100_00;
        let t1 = tick_full(101_00, 10, 100, 0, 0, 0);
        update(&mut s, &t1);
        s.last_ltp_paise = 101_00;
        let t2 = tick_full(102_00, 5, 100, 0, 0, 15_000_000_000);
        update(&mut s, &t2);
        // Both inside 30s — running sum should add.
        assert_eq!(compute_paise(&s), 15);
    }
}
