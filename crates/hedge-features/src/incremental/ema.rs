//! Incremental EMA — exponential moving averages and EMA slope.
//!
//! Two windows are maintained per symbol (R3.1):
//!
//! * **EMA fast (9)** — `α = 2 / (9 + 1) = 0.2`.
//! * **EMA slow (21)** — `α = 2 / (21 + 1) ≈ 0.0909`.
//!
//! Recurrence: `EMA_t = EMA_{t-1} + α · (price - EMA_{t-1})`.
//!
//! On the first observation the EMA is seeded with the LTP itself, so
//! the recurrence stays exactly equal to a python `pandas.ewm(span=N,
//! adjust=False)` reference (which is the reference implementation we
//! use in tests via the closed-form recurrence).
//!
//! ## EMA slope
//!
//! Defined as `(ema_now - ema_{n_ago}) / n_ago` where `n_ago` is the
//! [`EMA_SLOPE_LOOKBACK`] (5) most recent EMA fast samples. Returned as
//! a paise-per-tick `f32` rate.

use hedge_schemas::Tick;

use crate::state::{FeatureState, EMA_FAST_PERIOD, EMA_SLOPE_LOOKBACK, EMA_SLOW_PERIOD};

/// `α` for the EMA(9) recurrence.
pub const ALPHA_FAST: f64 = 2.0 / (EMA_FAST_PERIOD as f64 + 1.0);

/// `α` for the EMA(21) recurrence.
pub const ALPHA_SLOW: f64 = 2.0 / (EMA_SLOW_PERIOD as f64 + 1.0);

/// Fold a tick into both EMA accumulators and the EMA-fast history
/// window used by [`compute_slope`].
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    let price = tick.ltp_paise as f64;

    if state.ema_fast_seeded {
        let prev = state.ema_fast_paise as f64;
        let next = prev + ALPHA_FAST * (price - prev);
        state.ema_fast_paise = next.round() as i64;
    } else {
        state.ema_fast_paise = tick.ltp_paise;
        state.ema_fast_seeded = true;
    }

    if state.ema_slow_seeded {
        let prev = state.ema_slow_paise as f64;
        let next = prev + ALPHA_SLOW * (price - prev);
        state.ema_slow_paise = next.round() as i64;
    } else {
        state.ema_slow_paise = tick.ltp_paise;
        state.ema_slow_seeded = true;
    }

    state.ema_fast_history.push(state.ema_fast_paise);
}

/// Returns EMA fast in paise.
#[inline]
pub fn compute_fast_paise(state: &FeatureState) -> i64 {
    if state.ema_fast_seeded {
        state.ema_fast_paise
    } else {
        0
    }
}

/// Returns EMA slow in paise.
#[inline]
pub fn compute_slow_paise(state: &FeatureState) -> i64 {
    if state.ema_slow_seeded {
        state.ema_slow_paise
    } else {
        0
    }
}

/// EMA slope as `(ema_now - ema_{lookback}) / lookback` in paise / tick.
///
/// Returns `0.0` until [`is_ready`] is true. The schema field is `f32`
/// so we cast at the boundary.
#[inline]
pub fn compute_slope(state: &FeatureState) -> f32 {
    if state.ema_fast_history.len() < EMA_SLOPE_LOOKBACK {
        return 0.0;
    }
    // Iterator skips through to the oldest of the last 5 samples.
    let oldest = match state
        .ema_fast_history
        .iter_recent(EMA_SLOPE_LOOKBACK)
        .next()
    {
        Some(v) => *v,
        None => return 0.0,
    };
    let now = state.ema_fast_paise;
    ((now - oldest) as f64 / (EMA_SLOPE_LOOKBACK - 1).max(1) as f64) as f32
}

/// Schema-facing alias: returns the EMA fast value as `f32` (paise).
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    compute_fast_paise(state) as f32
}

/// `true` once both EMAs have been seeded and the history window is full
/// enough to compute the slope.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.ema_fast_seeded
        && state.ema_slow_seeded
        && state.ema_fast_history.len() >= EMA_SLOPE_LOOKBACK
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick_with_count;
    use proptest::prelude::*;

    /// Reference EMA recurrence in paise via f64 round-to-nearest.
    fn reference_ema_paise(prices: &[i64], alpha: f64) -> i64 {
        if prices.is_empty() {
            return 0;
        }
        let mut ema = prices[0] as f64;
        for &p in &prices[1..] {
            ema = ema + alpha * (p as f64 - ema);
        }
        ema.round() as i64
    }

    #[test]
    fn ema_returns_zero_before_seeded() {
        let s = FeatureState::default();
        assert_eq!(compute_fast_paise(&s), 0);
        assert_eq!(compute_slow_paise(&s), 0);
        assert!(!is_ready(&s));
    }

    #[test]
    fn ema_seeded_to_first_price() {
        let mut s = FeatureState::default();
        update(&mut s, &tick_with_count(123_45, 0));
        assert_eq!(compute_fast_paise(&s), 123_45);
        assert_eq!(compute_slow_paise(&s), 123_45);
    }

    #[test]
    fn ema_recurrence_matches_reference_on_constant_input() {
        // Property: feeding the same price N times keeps both EMAs at
        // that exact value.
        let mut s = FeatureState::default();
        for i in 0..50 {
            update(&mut s, &tick_with_count(100_00, i));
        }
        assert_eq!(compute_fast_paise(&s), 100_00);
        assert_eq!(compute_slow_paise(&s), 100_00);
    }

    #[test]
    fn ema_recurrence_matches_reference_under_random_inputs_property() {
        let runner = &mut proptest::test_runner::TestRunner::default();
        let strategy = proptest::collection::vec(1_00i64..=1_000_000i64, 1..200);
        runner
            .run(&strategy, |prices| {
                let mut state = FeatureState::default();
                for (i, p) in prices.iter().enumerate() {
                    update(&mut state, &tick_with_count(*p, i as u64));
                }
                let fast_got = compute_fast_paise(&state);
                let slow_got = compute_slow_paise(&state);
                let fast_expected = reference_ema_paise(&prices, ALPHA_FAST);
                let slow_expected = reference_ema_paise(&prices, ALPHA_SLOW);
                // Allow 1 paise tolerance for round-to-nearest drift on
                // accumulated f64 -> i64 round-trips.
                prop_assert!(
                    (fast_got - fast_expected).abs() <= 1,
                    "fast: got {} expected {}",
                    fast_got,
                    fast_expected
                );
                prop_assert!(
                    (slow_got - slow_expected).abs() <= 1,
                    "slow: got {} expected {}",
                    slow_got,
                    slow_expected
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn ema_slope_zero_before_history_full() {
        let mut s = FeatureState::default();
        update(&mut s, &tick_with_count(100_00, 0));
        assert_eq!(compute_slope(&s), 0.0);
    }

    #[test]
    fn ema_slope_positive_under_rising_prices() {
        let mut s = FeatureState::default();
        for i in 0..20i64 {
            update(&mut s, &tick_with_count(100_00 + i * 100, i as u64));
        }
        assert!(compute_slope(&s) > 0.0);
    }

    #[test]
    fn ema_slope_negative_under_falling_prices() {
        let mut s = FeatureState::default();
        for i in 0..20i64 {
            update(&mut s, &tick_with_count(200_00 - i * 100, i as u64));
        }
        assert!(compute_slope(&s) < 0.0);
    }
}
