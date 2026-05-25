//! Incremental VWAP — Volume-Weighted Average Price.
//!
//! Definition: `VWAP = Σ(price · qty) / Σ(qty)` accumulated since session
//! start (design § Components § Feature_Extraction_Engine; R3.1).
//!
//! ## Update / compute / is_ready
//!
//! * [`update`] folds `(ltp_paise · ltq)` into the state's `i128`
//!   numerator and `u128` denominator. O(1), allocation-free.
//! * [`compute_paise`] returns the integer-domain VWAP in paise; this is
//!   the canonical output for downstream consumers that already speak
//!   `Px` (e.g. VWAP_Pullback). Returns `0` until the first tick with
//!   `ltq > 0` is observed.
//! * [`compute`] returns the same value cast to `f32` for the
//!   `feat.update.<sym>` payload, where the FlatBuffers schema declares
//!   `vwap: long`.
//! * [`is_ready`] returns `true` once `Σ qty > 0`.
//!
//! ## Reset on session boundary
//!
//! VWAP is the canonical example of a session-scoped accumulator — the
//! design names a session-start reset explicitly. The reset is performed
//! by [`crate::state::FeatureState::clear_session`], not here, so that
//! every session-scoped indicator clears in one call. The dedicated test
//! `vwap_reset_on_session_boundary_clears_cumulative` lives below.

use hedge_schemas::Tick;

use crate::state::FeatureState;

/// Fold a single tick into the VWAP accumulators.
///
/// Ticks with `ltq == 0` (e.g. quote updates that did not trade) are
/// folded as no-ops on the cumulative numerator and denominator — they
/// must not bias the average.
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    let qty = tick.ltq;
    if qty == 0 {
        return;
    }
    // i128 numerator absorbs `i64::MAX × u64::MAX` worst case.
    let contribution = tick.ltp_paise as i128 * qty as i128;
    state.vwap_num = state.vwap_num.saturating_add(contribution);
    state.vwap_den = state.vwap_den.saturating_add(qty as u128);
}

/// Returns VWAP in paise. `0` while `is_ready` is false.
#[inline]
pub fn compute_paise(state: &FeatureState) -> i64 {
    if state.vwap_den == 0 {
        0
    } else {
        // Integer division. The result fits in `i64` because the
        // numerator divided by a positive denominator is bounded by the
        // largest single price contribution, which is itself an `i64`.
        let v = state.vwap_num / state.vwap_den as i128;
        // Clamp defensively. In practice this branch is dead.
        if v > i64::MAX as i128 {
            i64::MAX
        } else if v < i64::MIN as i128 {
            i64::MIN
        } else {
            v as i64
        }
    }
}

/// Returns VWAP as `f32` (paise units). Callers that round-trip to `f64`
/// for accuracy should prefer [`compute_paise`].
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    compute_paise(state) as f32
}

/// `true` once at least one tick with `ltq > 0` has been folded in.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.vwap_den > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick;
    use proptest::prelude::*;

    #[test]
    fn vwap_returns_zero_before_warmup() {
        let s = FeatureState::default();
        assert!(!is_ready(&s));
        assert_eq!(compute_paise(&s), 0);
        assert_eq!(compute(&s), 0.0);
    }

    #[test]
    fn vwap_zero_qty_ticks_are_no_ops() {
        let mut s = FeatureState::default();
        // Quote-only tick: ltq = 0. Must not move VWAP.
        update(&mut s, &tick(100_00, 0));
        assert!(!is_ready(&s));
        assert_eq!(compute_paise(&s), 0);
    }

    #[test]
    fn vwap_single_trade_equals_price() {
        let mut s = FeatureState::default();
        update(&mut s, &tick(100_00, 1));
        assert!(is_ready(&s));
        assert_eq!(compute_paise(&s), 100_00);
    }

    #[test]
    fn vwap_two_trades_volume_weighted_average() {
        let mut s = FeatureState::default();
        update(&mut s, &tick(100_00, 10)); // 100.00 × 10
        update(&mut s, &tick(110_00, 30)); // 110.00 × 30
        // (100.00 * 10 + 110.00 * 30) / (10 + 30) = (1000 + 3300) / 40
        // = 4300/40 = 107.50  — but in paise: (10000*10 + 11000*30)/40
        // = (100000 + 330000)/40 = 430000/40 = 10750
        assert_eq!(compute_paise(&s), 10_750);
    }

    /// Property: VWAP equals the brute-force formula for any sequence
    /// of ticks within FP tolerance / paise tolerance.
    #[test]
    fn vwap_matches_reference_under_random_inputs_property() {
        // Manual proptest harness because we want explicit shrink reports.
        let runner = &mut proptest::test_runner::TestRunner::default();
        let strategy = proptest::collection::vec(
            (1i64..=10_000_000i64, 1u64..=10_000u64),
            1..200,
        );
        runner
            .run(&strategy, |trades| {
                let mut state = FeatureState::default();
                let mut num: i128 = 0;
                let mut den: u128 = 0;
                for (price, qty) in &trades {
                    update(&mut state, &tick(*price, *qty));
                    num += (*price as i128) * (*qty as i128);
                    den += *qty as u128;
                }
                let expected = if den == 0 { 0 } else { (num / den as i128) as i64 };
                let got = compute_paise(&state);
                prop_assert_eq!(got, expected);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn vwap_reset_on_session_boundary_clears_cumulative() {
        let mut s = FeatureState::default();
        update(&mut s, &tick(100_00, 100));
        update(&mut s, &tick(101_00, 50));
        assert!(is_ready(&s));
        assert!(compute_paise(&s) > 0);

        s.clear_session();

        assert!(!is_ready(&s));
        assert_eq!(compute_paise(&s), 0);

        // After the reset, a fresh trade at a new price re-anchors VWAP at
        // that price — proving the cumulative was wiped.
        update(&mut s, &tick(200_00, 1));
        assert_eq!(compute_paise(&s), 200_00);
    }
}
