//! Momentum — `(price_now - price_n_ago) / price_n_ago` over 10 ticks.
//!
//! Source: design § Components § Feature_Extraction_Engine, R3.1.

use hedge_schemas::Tick;

use crate::state::{FeatureState, MOMENTUM_WINDOW};

/// Append the new LTP into the momentum buffer.
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    state.momentum_prices.push(tick.ltp_paise);
}

/// Returns the relative change `(p_now - p_old) / p_old` as `f32`.
/// `0.0` until the buffer has ≥ 2 samples.
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    if state.momentum_prices.len() < 2 {
        return 0.0;
    }
    let oldest = match state.momentum_prices.oldest() {
        Some(v) => *v,
        None => return 0.0,
    };
    if oldest == 0 {
        return 0.0;
    }
    let newest = match state.momentum_prices.latest() {
        Some(v) => *v,
        None => return 0.0,
    };
    ((newest as f64 - oldest as f64) / oldest as f64) as f32
}

/// `true` once the window has filled.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.momentum_prices.len() >= MOMENTUM_WINDOW
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick_with_count;
    use proptest::prelude::*;

    fn reference_momentum(prices: &[i64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }
        let take = prices.len().min(MOMENTUM_WINDOW);
        let window = &prices[prices.len() - take..];
        let oldest = window[0] as f64;
        if oldest == 0.0 {
            return 0.0;
        }
        let newest = *window.last().unwrap() as f64;
        (newest - oldest) / oldest
    }

    #[test]
    fn momentum_zero_before_two_ticks() {
        let s = FeatureState::default();
        assert_eq!(compute(&s), 0.0);
    }

    #[test]
    fn momentum_after_full_window_uses_oldest_in_window() {
        let mut s = FeatureState::default();
        for i in 0..15u64 {
            update(&mut s, &tick_with_count(100_00 + i as i64 * 100, i));
        }
        // After 15 ticks, oldest in window of size 10 is at index 5 (price 100_00 + 500 = 100_500),
        // newest is index 14 (price 100_00 + 1400 = 101_400). Wait — ltp_paise is in paise:
        // p_old = 100_00 + 5*100 = 100_500 paise; p_new = 100_00 + 14*100 = 101_400 paise.
        let p_old = 100_00 + 5 * 100;
        let p_new = 100_00 + 14 * 100;
        let expected = ((p_new as f64) - p_old as f64) / p_old as f64;
        let got = compute(&s) as f64;
        assert!((got - expected).abs() < 1e-6, "got {} expected {}", got, expected);
    }

    #[test]
    fn momentum_matches_reference_under_random_inputs_property() {
        let runner = &mut proptest::test_runner::TestRunner::default();
        let strategy = proptest::collection::vec(1_00i64..=1_000_000i64, 1..200);
        runner
            .run(&strategy, |prices| {
                let mut state = FeatureState::default();
                for (i, p) in prices.iter().enumerate() {
                    update(&mut state, &tick_with_count(*p, i as u64));
                }
                let got = compute(&state) as f64;
                let expected = reference_momentum(&prices);
                let tol = (expected.abs() * 1e-5).max(1e-7);
                prop_assert!(
                    (got - expected).abs() <= tol,
                    "got {} expected {} tol {}",
                    got,
                    expected,
                    tol
                );
                Ok(())
            })
            .unwrap();
    }
}
