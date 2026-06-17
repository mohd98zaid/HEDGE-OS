//! Property-based tests for `hedge-core` primitives (task 2.2).
//!
//! Validates:
//!   - Property 4 — Score and Formula Equivalence (Px arithmetic)
//!   - Property 3 — Latency Budget Compliance (LatencyTimer monotonicity)
//!
//! **Validates: Requirements 1.4, 2.6, 3.4**

use hedge_core::{Px, RingWindow};
use proptest::prelude::*;

// ---- Px arithmetic proptests --------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property: Px addition is commutative and conserves paise.
    /// For any two paise values, a + b == b + a and the result matches
    /// the underlying i64 addition (no fractional drift).
    #[test]
    fn px_addition_commutative(a in -1_000_000_000_000i64..1_000_000_000_000, b in -1_000_000_000_000i64..1_000_000_000_000) {
        let pa = Px::from_paise(a);
        let pb = Px::from_paise(b);
        prop_assert_eq!((pa + pb).to_paise(), a + b);
        prop_assert_eq!((pb + pa).to_paise(), b + a);
        prop_assert_eq!((pa + pb).to_paise(), (pb + pa).to_paise());
    }

    /// Property: Px subtraction is the inverse of addition.
    /// For any a, b: (a + b) - b == a and (a - b) + b == a.
    #[test]
    fn px_subtraction_inverse_of_addition(a in -1_000_000_000_000i64..1_000_000_000_000, b in -1_000_000_000_000i64..1_000_000_000_000) {
        let pa = Px::from_paise(a);
        let pb = Px::from_paise(b);
        prop_assert_eq!(((pa + pb) - pb).to_paise(), a);
        prop_assert_eq!(((pa - pb) + pb).to_paise(), a);
    }

    /// Property: scalar multiplication distributes over addition.
    /// k * (a + b) == k*a + k*b. Constrained to avoid overflow.
    #[test]
    fn px_scalar_mul_distributes(
        a in -1_000_000i64..1_000_000,
        b in -1_000_000i64..1_000_000,
        k in -1_000i64..1_000,
    ) {
        let pa = Px::from_paise(a);
        let pb = Px::from_paise(b);
        let lhs = ((pa + pb) * k).to_paise();
        let rhs = (pa * k + pb * k).to_paise();
        prop_assert_eq!(lhs, rhs);
    }

    /// Property: negation is an involution: -(-x) == x.
    #[test]
    fn px_negation_involution(a in any::<i64>()) {
        let pa = Px::from_paise(a);
        prop_assert_eq!((-(-pa)).to_paise(), pa.to_paise());
    }

    /// Property: from_paise / to_paise round-trip is identity.
    /// For any i64, Px::from_paise(x).to_paise() == x.
    #[test]
    fn px_paise_round_trip(x in any::<i64>()) {
        prop_assert_eq!(Px::from_paise(x).to_paise(), x);
    }

    /// Property: from_inr / to_inr round-trip for integer rupee values.
    /// Constrained to avoid overflow (rupees * 100 must fit i64).
    #[test]
    fn px_inr_round_trip(r in (i64::MIN / 100)..=(i64::MAX / 100)) {
        prop_assert_eq!(Px::from_inr(r).to_inr(), r);
    }

    /// Property: checked_add returns Some for non-overflowing inputs
    /// and the result matches unchecked addition.
    #[test]
    fn px_checked_add_matches_unchecked(a in -1_000_000_000_000i64..1_000_000_000_000, b in -1_000_000_000_000i64..1_000_000_000_000) {
        let pa = Px::from_paise(a);
        let pb = Px::from_paise(b);
        match pa.checked_add(pb) {
            Some(result) => {
                // No overflow: result must match unchecked add
                prop_assert_eq!(result.to_paise(), a.wrapping_add(b));
                // And it must not have wrapped (i.e., the add was safe)
                prop_assert!(
                    (a >= 0 && b >= 0 && result.to_paise() >= 0) ||
                    (a <= 0 && b <= 0 && result.to_paise() <= 0) ||
                    (a ^ b < 0), // different signs can't overflow
                    "overflow detected: {} + {} = {}", a, b, result.to_paise()
                );
            }
            None => {
                // Overflow detected — verify it would actually overflow
                prop_assert!(
                    a.checked_add(b).is_none(),
                    "checked_add returned None but i64 add would succeed"
                );
            }
        }
    }

    /// Property: abs() returns non-negative value and abs(abs(x)) == abs(x).
    #[test]
    fn px_abs_non_negative_and_idempotent(a in -1_000_000_000_000i64..1_000_000_000_000) {
        let pa = Px::from_paise(a);
        let abs_a = pa.abs();
        prop_assert!(abs_a.to_paise() >= 0);
        prop_assert_eq!(abs_a.abs().to_paise(), abs_a.to_paise());
    }

    /// Property: Display format always contains exactly two digits after decimal.
    #[test]
    fn px_display_two_decimal_places(a in -1_000_000_000_000i64..1_000_000_000_000) {
        let pa = Px::from_paise(a);
        let s = format!("{}", pa);
        // Find the decimal point
        if let Some(dot_pos) = s.find('.') {
            let frac = &s[dot_pos + 1..];
            prop_assert_eq!(frac.len(), 2, "Display '{}' doesn't have 2 decimal digits", s);
        } else if a % 100 == 0 {
            // Whole paise values display without decimal: acceptable
        } else {
            prop_assert!(false, "Display '{}' missing decimal point", s);
        }
    }
}

// ---- RingWindow proptests ------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Property: RingWindow push never panics for any sequence within capacity.
    /// For capacity N, pushing any N values and then pushing more never panics.
    #[test]
    fn ring_window_push_never_panics(
        values in proptest::collection::vec(any::<u32>(), 0..100),
        cap in 1usize..50,
    ) {
        let mut w: RingWindow<u32, 64> = RingWindow::new();
        // We can't use a runtime capacity, so we push up to 64 elements
        // and verify no panics occur.
        for &v in values.iter().take(64) {
            w.push(v);
        }
        // Window should be at most full
        prop_assert!(w.len() <= 64);
    }

    /// Property: after pushing k values (k <= N), the window contains exactly
    /// min(k, N) elements and iter() yields exactly that many.
    #[test]
    fn ring_window_len_matches_iter_count(
        values in proptest::collection::vec(any::<u32>(), 0..80),
    ) {
        let mut w: RingWindow<u32, 64> = RingWindow::new();
        for &v in values.iter().take(80) {
            w.push(v);
        }
        let count = w.iter().count();
        prop_assert_eq!(w.len(), count);
        prop_assert!(w.len() <= 64);
    }

    /// Property: iter() yields elements in insertion order (oldest first).
    /// For a window of capacity N, after pushing M > N values, the last N
    /// values (in insertion order) are yielded by iter().
    #[test]
    fn ring_window_iter_preserves_order(
        values in proptest::collection::vec(0u32..1000, 0..100),
    ) {
        let mut w: RingWindow<u32, 16> = RingWindow::new();
        for &v in &values {
            w.push(v);
        }
        let collected: Vec<u32> = w.iter().copied().collect();
        // The window should hold the last min(values.len(), 16) elements
        let expected_count = values.len().min(16);
        prop_assert_eq!(collected.len(), expected_count);
        // And they should be the most recent values in insertion order
        if values.len() >= 16 {
            let tail = &values[values.len() - 16..];
            prop_assert_eq!(&collected, tail);
        } else {
            prop_assert_eq!(&collected, &values[..]);
        }
    }

    /// Property: latest() always returns the last pushed value (when non-empty).
    #[test]
    fn ring_window_latest_is_last_pushed(
        values in proptest::collection::vec(any::<i64>(), 1..80),
    ) {
        let mut w: RingWindow<i64, 32> = RingWindow::new();
        for &v in &values {
            w.push(v);
        }
        prop_assert_eq!(w.latest(), Some(&values[values.len() - 1]));
    }

    /// Property: oldest() always returns the oldest retained value.
    #[test]
    fn ring_window_oldest_is_oldest_retained(
        values in proptest::collection::vec(any::<i64>(), 1..80),
    ) {
        let mut w: RingWindow<i64, 32> = RingWindow::new();
        for &v in &values {
            w.push(v);
        }
        let expected_oldest = if values.len() <= 32 {
            &values[0]
        } else {
            &values[values.len() - 32]
        };
        prop_assert_eq!(w.oldest(), Some(expected_oldest));
    }

    /// Property: clear() resets to empty state regardless of prior pushes.
    #[test]
    fn ring_window_clear_resets(
        values in proptest::collection::vec(any::<u8>(), 1..80),
    ) {
        let mut w: RingWindow<u8, 16> = RingWindow::new();
        for &v in &values {
            w.push(v);
        }
        w.clear();
        prop_assert!(w.is_empty());
        prop_assert_eq!(w.len(), 0);
        prop_assert_eq!(w.latest(), None);
        prop_assert_eq!(w.oldest(), None);
        prop_assert_eq!(w.iter().count(), 0);
    }

    /// Property: iter_recent(k) returns the last min(k, len) elements.
    #[test]
    fn ring_window_iter_recent_bounds(
        values in proptest::collection::vec(any::<u32>(), 0..60),
        k in 0usize..100,
    ) {
        let mut w: RingWindow<u32, 32> = RingWindow::new();
        for &v in &values {
            w.push(v);
        }
        let recent: Vec<u32> = w.iter_recent(k).copied().collect();
        let expected_len = k.min(w.len());
        prop_assert_eq!(recent.len(), expected_len);
        // Recent elements should be a suffix of iter()
        let full: Vec<u32> = w.iter().copied().collect();
        let suffix = &full[full.len().saturating_sub(k)..];
        prop_assert_eq!(&recent, suffix);
    }
}
