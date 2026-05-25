//! Realized volatility — standard deviation of log returns over 30 ticks.
//!
//! Definition (R3.1, design § Components § Feature_Extraction_Engine):
//!
//! ```text
//! r_t = ln(price_t) - ln(price_{t-1})
//! σ   = sqrt( Σ (r_t - r̄)^2 / (N - 1) )    over the last N=30 returns
//! ```
//!
//! ## Numerical considerations
//!
//! Prices are paise (`i64`); a log-domain conversion is the cheapest way
//! to retain precision across the full NSE price range without resorting
//! to fixed-point logs. We compute in `f64` inside the recurrence, then
//! cast to `f32` at the schema boundary because the FlatBuffers field is
//! `realized_vol: float`.

use hedge_schemas::Tick;

use crate::state::{FeatureState, VOLATILITY_WINDOW};

/// Fold a tick into the log-return window.
///
/// `state.last_ltp_paise` holds the LTP of the **previous** tick (the
/// engine bookkeeping rolls `last_ltp_paise → prev_ltp_paise` and
/// `tick.ltp_paise → last_ltp_paise` AFTER every indicator runs), so
/// the log-return for the new tick is `ln(tick.ltp) - ln(last_ltp)`.
/// On the very first tick `state.last_ltp_paise == 0`, so we skip the
/// push — the next tick produces the first valid return.
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    if state.last_ltp_paise > 0 && tick.ltp_paise > 0 {
        let cur = (tick.ltp_paise as f64).ln();
        let prev = (state.last_ltp_paise as f64).ln();
        state.log_returns.push(cur - prev);
    }
}

/// Returns realized volatility in the same units as `ln(price)` (i.e.
/// dimensionless). `0.0` until [`is_ready`] is true.
pub fn compute(state: &FeatureState) -> f32 {
    let n = state.log_returns.len();
    if n < 2 {
        return 0.0;
    }
    let mut sum = 0.0f64;
    for v in state.log_returns.iter() {
        sum += *v;
    }
    let mean = sum / n as f64;
    let mut var = 0.0f64;
    for v in state.log_returns.iter() {
        let d = *v - mean;
        var += d * d;
    }
    let variance = var / (n as f64 - 1.0);
    variance.sqrt() as f32
}

/// `true` once the window holds the full 30 returns.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.log_returns.len() >= VOLATILITY_WINDOW
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick_with_count;
    use proptest::prelude::*;

    /// Reference: pure f64 stdev of log returns, sample (N-1) form.
    fn reference_vol(prices: &[i64]) -> f64 {
        if prices.len() < 2 {
            return 0.0;
        }
        let returns: Vec<f64> = prices
            .windows(2)
            .map(|w| (w[1] as f64).ln() - (w[0] as f64).ln())
            .collect();
        let take = returns.len().min(VOLATILITY_WINDOW);
        let window = &returns[returns.len() - take..];
        let n = window.len() as f64;
        if n < 2.0 {
            return 0.0;
        }
        let mean: f64 = window.iter().sum::<f64>() / n;
        let var: f64 = window.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
        var.sqrt()
    }

    #[test]
    fn volatility_zero_before_warmup() {
        let s = FeatureState::default();
        assert!(!is_ready(&s));
        assert_eq!(compute(&s), 0.0);
    }

    #[test]
    fn volatility_constant_prices_yield_zero() {
        // Property: ln(p) - ln(p) == 0 for every tick → stdev 0.
        let mut s = FeatureState::default();
        for i in 0..50u64 {
            let t = tick_with_count(100_00, i);
            update(&mut s, &t);
            // Engine bookkeeping mirror — `last_ltp_paise` is what the
            // next call sees as the "previous" price.
            s.last_ltp_paise = 100_00;
        }
        assert!(is_ready(&s));
        assert!(compute(&s).abs() < 1e-9_f32);
    }

    #[test]
    fn volatility_matches_reference_under_random_inputs_property() {
        let runner = &mut proptest::test_runner::TestRunner::default();
        let strategy = proptest::collection::vec(1_00i64..=1_000_000i64, 2..200);
        runner
            .run(&strategy, |prices| {
                let mut state = FeatureState::default();
                let mut prev = 0i64;
                for (i, p) in prices.iter().enumerate() {
                    let t = tick_with_count(*p, i as u64);
                    state.last_ltp_paise = prev;
                    update(&mut state, &t);
                    prev = *p;
                }
                let got = compute(&state) as f64;
                let expected = reference_vol(&prices);
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
