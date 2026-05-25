//! Property-based tests for the Position_Engine arithmetic.
//!
//! These tests live in the integration `tests/` directory rather than in
//! `#[cfg(test)] mod` blocks so they are visible to `cargo test --tests`
//! at the workspace level. They exercise the public API only.
//!
//! Validates: Requirement 8.1, 8.2 (relevant arithmetic). The full latency
//! property — `position update p99 < 5 ms per fill` — lives in the
//! dedicated proptest task **16.2** and the end-to-end Property 9 suite
//! (task 55.1).

use hedge_core::{Px, Side, SymbolId};
use hedge_position::{Position, PositionEngine};
use proptest::prelude::*;

/// Generator: a price between ₹1.00 and ₹100,000.00 in paise.
fn price_strategy() -> impl Strategy<Value = Px> {
    (1_00i64..100_000_00).prop_map(Px::from_paise)
}

/// Generator: a non-zero buy quantity bounded so VWAP intermediates stay
/// within `i64`. `1..=1_000_000` is well below the overflow threshold even
/// when squared into paise products.
fn qty_strategy() -> impl Strategy<Value = u64> {
    1u64..=1_000_000
}

/// Generator: `(total_qty, partition)` where `partition.iter().sum() == total_qty`
/// and every element is strictly positive. Always produces between 1 and 8
/// partial fills so test runtime stays bounded.
///
/// We materialise this as a single flat map over `(qty, weights)` so the
/// returned closure captures only owned, `Copy` values.
fn total_with_partition() -> impl Strategy<Value = (u64, Vec<u64>)> {
    (qty_strategy(), prop::collection::vec(1u64..=10_000, 1..=8)).prop_map(|(total, weights)| {
        let k = weights.len().min(total as usize).max(1);
        let weights: Vec<u64> = weights.into_iter().take(k).collect();

        // Normalise weights into integer slices that sum exactly to `total`,
        // every slice ≥ 1.
        let sum_w: u128 = weights.iter().map(|w| *w as u128).sum::<u128>().max(1);
        let mut acc: u128 = 0;
        let mut out: Vec<u64> = Vec::with_capacity(k);
        for (i, w) in weights.iter().enumerate() {
            let target_acc = if i + 1 == k {
                total as u128
            } else {
                acc + ((*w as u128) * total as u128 / sum_w)
            };
            let slice = target_acc.saturating_sub(acc);
            acc += slice;
            out.push(slice as u64);
        }

        // Promote any zero slice to 1, donating from neighbours that have
        // headroom.
        let mut deficit: u64 = 0;
        for v in out.iter_mut() {
            if *v == 0 {
                *v = 1;
                deficit += 1;
            }
        }
        while deficit > 0 {
            let mut donated = false;
            for v in out.iter_mut() {
                if *v > 1 && deficit > 0 {
                    *v -= 1;
                    deficit -= 1;
                    donated = true;
                }
            }
            if !donated {
                break;
            }
        }

        // Patch any drift from rounding so the partition sums to `total`.
        let so_far: u64 = out.iter().sum();
        if so_far < total {
            *out.last_mut().unwrap() += total - so_far;
        } else if so_far > total {
            let excess = so_far - total;
            let last = out.last_mut().unwrap();
            *last = last.saturating_sub(excess);
        }

        (total, out)
    })
}

proptest! {
    /// **Validates: Requirements 8.1, 8.2** —
    /// for any sequence of partial buys summing to `total_qty`, the
    /// resulting position equals a single full-fill outcome (within paise
    /// rounding from integer division).
    #[test]
    fn vwap_partials_equal_single_fill(
        partition in total_with_partition(),
        fill_px in price_strategy(),
    ) {
        let (total_qty, parts) = partition;
        // All slices fill at the same price → VWAP must be exactly fill_px.
        let mut multi = Position::flat(SymbolId::new(1));
        for p in &parts {
            multi.apply_fill(Side::Buy, *p, fill_px);
        }

        let mut single = Position::flat(SymbolId::new(1));
        single.apply_fill(Side::Buy, total_qty, fill_px);

        prop_assert_eq!(multi.quantity, single.quantity);
        prop_assert_eq!(multi.avg_entry_px, single.avg_entry_px);
        prop_assert_eq!(multi.realized_pnl_paise, 0);
        prop_assert_eq!(multi.realized_pnl_paise, single.realized_pnl_paise);
    }

    /// **Validates: Requirements 8.1** —
    /// VWAP across two distinct partial fills at different prices equals
    /// the textbook formula:
    ///
    /// ```text
    /// avg = (q1*p1 + q2*p2) / (q1+q2)
    /// ```
    ///
    /// implemented exactly in integer paise (truncation toward zero — i.e.
    /// the documented integer-division semantics).
    #[test]
    fn vwap_two_legs_matches_formula(
        q1 in qty_strategy(),
        q2 in qty_strategy(),
        p1 in price_strategy(),
        p2 in price_strategy(),
    ) {
        let mut p = Position::flat(SymbolId::new(1));
        p.apply_fill(Side::Buy, q1, p1);
        p.apply_fill(Side::Buy, q2, p2);

        let expected = ((q1 as i128) * p1.to_paise() as i128
            + (q2 as i128) * p2.to_paise() as i128)
            / (q1 as i128 + q2 as i128);

        prop_assert_eq!(p.avg_entry_px.to_paise() as i128, expected);
        prop_assert_eq!(p.quantity as u64, q1 + q2);
    }

    /// **Validates: Requirements 8.3** —
    /// `apply_mark` only changes `unrealized_pnl_paise`; `realized_pnl_paise`
    /// is invariant across any number of mark updates.
    #[test]
    fn marks_never_modify_realized(
        open_qty in qty_strategy(),
        open_px in price_strategy(),
        marks in prop::collection::vec(price_strategy(), 1..16),
    ) {
        let mut p = Position::flat(SymbolId::new(1));
        p.apply_fill(Side::Buy, open_qty, open_px);
        let realized_before = p.realized_pnl_paise;
        for m in marks {
            p.apply_mark(m);
            prop_assert_eq!(p.realized_pnl_paise, realized_before);
        }
    }

    /// **Validates: Requirements 8.5** —
    /// `peak_equity_paise` is monotonically non-decreasing across a
    /// sequence of price moves on a long position.
    #[test]
    fn peak_equity_is_monotonic_under_marks(
        open_qty in 1u64..=1_000,
        open_px_paise in 50_00i64..=200_00,
        marks in prop::collection::vec(50_00i64..=200_00, 1..16),
    ) {
        let engine = PositionEngine::with_throttle(20_000_00, 0);
        engine.on_fill(SymbolId::new(1), Side::Buy, open_qty, Px::from_paise(open_px_paise), 0);
        let mut prev_peak = engine.snapshot_risk_state().peak_equity_paise;
        for (i, m) in marks.iter().enumerate() {
            let now = (i as u64 + 1) * 1_000_000;
            engine.on_tick(SymbolId::new(1), Px::from_paise(*m), now);
            let cur = engine.snapshot_risk_state().peak_equity_paise;
            prop_assert!(cur >= prev_peak, "peak equity went backward");
            prev_peak = cur;
        }
    }
}
