//! Fixed-decimal price newtype.
//!
//! [`Px`] represents a price in **paise** (one-hundredth of one Indian Rupee)
//! using a 64-bit signed integer. All arithmetic is done in the integer
//! domain; **no floating point is used or accepted**, satisfying the design's
//! "Common Types" (`pub type Px = i64; // paise, fixed-point`) and the
//! Property 4 requirement that score and risk formulas are exact.
//!
//! `i64` paise gives a representable range of approximately ±9.22 × 10^16 INR
//! — far exceeding any conceivable single-trade price. NSE tick prices are
//! quoted in paise, so this is also the natural on-wire representation
//! (`Tick_v1.ltp_paise`, `OrderIntent_v1.limit_paise`).
//!
//! ## Operators
//!
//! * `Px + Px → Px` and `Px - Px → Px` are saturating-free integer
//!   operations (overflow is treated as a programmer error and panics in
//!   debug, wraps in release like every other `i64` operation).
//! * `Px * i64 → Px` and `i64 * Px → Px` scale by an integer multiplier
//!   (e.g. quantity).
//! * `Px / i64 → Px` divides by a non-zero integer scalar.
//!
//! Dividing one `Px` by another (yielding a dimensionless ratio) is
//! intentionally **not** provided — the result would lose decimal precision
//! and is rarely what the caller wants. Use `to_paise()` and divide manually.

use std::fmt;
use std::ops::{Add, AddAssign, Div, Mul, Neg, Sub, SubAssign};

use serde::{Deserialize, Serialize};

/// Number of paise per rupee (10^2).
pub const PAISE_PER_INR: i64 = 100;

/// Fixed-decimal price expressed in paise.
///
/// All Hot_Path price math goes through this type. The internal `i64` is
/// public to make on-wire codecs (FlatBuffers) zero-copy, but constructing
/// a `Px` from a raw paise count is preferred via [`Px::from_paise`].
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Px(pub i64);

impl Px {
    /// Zero-paise price.
    pub const ZERO: Self = Self(0);

    /// Construct from a raw paise count.
    #[inline]
    pub const fn from_paise(paise: i64) -> Self {
        Self(paise)
    }

    /// Construct from a whole-rupee count. Equivalent to
    /// `Px::from_paise(rupees * 100)`.
    ///
    /// # Panics (debug only)
    ///
    /// Overflows when `rupees * 100` exceeds `i64::MAX`. In release builds,
    /// overflow wraps per Rust's standard integer semantics — but any
    /// realistic NSE/BSE price is far below the overflow threshold.
    #[inline]
    pub const fn from_inr(rupees: i64) -> Self {
        Self(rupees * PAISE_PER_INR)
    }

    /// Returns the underlying paise count. Lossless.
    #[inline]
    pub const fn to_paise(self) -> i64 {
        self.0
    }

    /// Returns the integer rupee component (truncated toward zero).
    /// `200_50` paise → 200 rupees, `-1_50` paise → -1 rupee.
    ///
    /// **This is lossy** — fractional paise are dropped. Use [`Px::to_paise`]
    /// when precision matters.
    #[inline]
    pub const fn to_inr(self) -> i64 {
        self.0 / PAISE_PER_INR
    }

    /// Saturating addition. Returns `i64::MAX` on overflow rather than
    /// wrapping. Hot_Path code paths prefer the regular `+` operator and
    /// rely on the workspace `panic = "abort"` profile to surface bugs;
    /// `checked_*` and `saturating_*` are exposed for risk-engine paths
    /// that must be infallible.
    #[inline]
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating subtraction. Returns `i64::MIN` on underflow.
    #[inline]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    /// Checked addition; returns `None` on overflow.
    #[inline]
    pub const fn checked_add(self, rhs: Self) -> Option<Self> {
        match self.0.checked_add(rhs.0) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked subtraction; returns `None` on underflow.
    #[inline]
    pub const fn checked_sub(self, rhs: Self) -> Option<Self> {
        match self.0.checked_sub(rhs.0) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked scalar multiplication.
    #[inline]
    pub const fn checked_mul_scalar(self, scalar: i64) -> Option<Self> {
        match self.0.checked_mul(scalar) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Returns `true` if the price is non-negative.
    #[inline]
    pub const fn is_non_negative(self) -> bool {
        self.0 >= 0
    }

    /// Absolute value as a fresh `Px`. Saturates at `i64::MAX` to avoid the
    /// `i64::MIN` overflow trap (since `-i64::MIN` is unrepresentable in
    /// `i64`).
    #[inline]
    pub const fn abs(self) -> Self {
        Self(self.0.saturating_abs())
    }
}

impl Default for Px {
    #[inline]
    fn default() -> Self {
        Self::ZERO
    }
}

// --- Arithmetic operators -------------------------------------------------

impl Add for Px {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

impl Sub for Px {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

impl AddAssign for Px {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl SubAssign for Px {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

impl Mul<i64> for Px {
    type Output = Self;
    #[inline]
    fn mul(self, rhs: i64) -> Self {
        Self(self.0 * rhs)
    }
}

impl Mul<Px> for i64 {
    type Output = Px;
    #[inline]
    fn mul(self, rhs: Px) -> Px {
        Px(self * rhs.0)
    }
}

impl Div<i64> for Px {
    type Output = Self;
    /// # Panics
    ///
    /// Panics if `rhs == 0`, mirroring `i64` division semantics.
    #[inline]
    fn div(self, rhs: i64) -> Self {
        Self(self.0 / rhs)
    }
}

impl Neg for Px {
    type Output = Self;
    #[inline]
    fn neg(self) -> Self {
        Self(-self.0)
    }
}

// --- Display --------------------------------------------------------------

impl fmt::Display for Px {
    /// Renders as `<rupees>.<paise:02>`, e.g. `Px(200_50)` → `"₹200.50"`.
    /// The rupee glyph is used to disambiguate the unit at log inspection
    /// time.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let paise = self.0;
        let sign = if paise < 0 { "-" } else { "" };
        // `unsigned_abs` avoids the `i64::MIN` wrap pitfall.
        let abs = paise.unsigned_abs();
        let rupees = abs / PAISE_PER_INR as u64;
        let frac = abs % PAISE_PER_INR as u64;
        write!(f, "{}\u{20B9}{}.{:02}", sign, rupees, frac)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_inr_round_trips_through_to_inr() {
        for rupees in [0i64, 1, 99, 100, 12345, -7, -10000] {
            let px = Px::from_inr(rupees);
            assert_eq!(px.to_inr(), rupees, "round trip failed for {} INR", rupees);
            assert_eq!(px.to_paise(), rupees * PAISE_PER_INR);
        }
    }

    #[test]
    fn from_paise_preserves_value_exactly() {
        // Property: paise representation is lossless (fixed-decimal).
        let cases = [0i64, 1, 7, 50, 100, 10_001, -1, -50, -10_001];
        for paise in cases {
            let px = Px::from_paise(paise);
            assert_eq!(px.to_paise(), paise);
        }
    }

    #[test]
    fn add_and_sub_match_underlying_i64() {
        let a = Px::from_paise(150_00); // ₹150.00
        let b = Px::from_paise(25_50); //  ₹25.50

        assert_eq!((a + b).to_paise(), 175_50);
        assert_eq!((a - b).to_paise(), 124_50);
        assert_eq!((b - a).to_paise(), -124_50);
    }

    #[test]
    fn add_assign_and_sub_assign_mutate_in_place() {
        let mut p = Px::from_paise(100);
        p += Px::from_paise(50);
        assert_eq!(p.to_paise(), 150);
        p -= Px::from_paise(200);
        assert_eq!(p.to_paise(), -50);
    }

    #[test]
    fn scalar_mul_in_both_directions() {
        let p = Px::from_paise(7_25); // ₹7.25
        assert_eq!((p * 4).to_paise(), 29_00);
        assert_eq!((4i64 * p).to_paise(), 29_00);
        assert_eq!((p * 0).to_paise(), 0);
        assert_eq!((p * -3).to_paise(), -21_75);
    }

    #[test]
    fn scalar_div_truncates_toward_zero() {
        // Integer division semantics — paise are exact, no fractional drift.
        assert_eq!((Px::from_paise(10) / 3).to_paise(), 3);
        assert_eq!((Px::from_paise(-10) / 3).to_paise(), -3);
        assert_eq!((Px::from_paise(99_99) / 100).to_paise(), 99);
    }

    #[test]
    #[should_panic]
    fn scalar_div_by_zero_panics() {
        // Mirrors `i64::div` so the bug does not silently corrupt prices.
        let _ = Px::from_paise(1) / 0;
    }

    #[test]
    fn neg_inverts_sign() {
        assert_eq!((-Px::from_paise(42)).to_paise(), -42);
        assert_eq!((-Px::from_paise(-42)).to_paise(), 42);
        assert_eq!((-Px::ZERO).to_paise(), 0);
    }

    #[test]
    fn checked_add_detects_overflow() {
        assert!(Px::from_paise(i64::MAX).checked_add(Px::from_paise(1)).is_none());
        assert_eq!(
            Px::from_paise(10).checked_add(Px::from_paise(5)).unwrap().to_paise(),
            15
        );
    }

    #[test]
    fn saturating_arithmetic_clamps() {
        assert_eq!(
            Px::from_paise(i64::MAX).saturating_add(Px::from_paise(10)).to_paise(),
            i64::MAX
        );
        assert_eq!(
            Px::from_paise(i64::MIN).saturating_sub(Px::from_paise(10)).to_paise(),
            i64::MIN
        );
    }

    #[test]
    fn ord_eq_hash_match_paise_value() {
        let a = Px::from_paise(100);
        let b = Px::from_paise(200);
        assert!(a < b);
        assert_eq!(a, Px::from_paise(100));

        // Hash equal values to equal hashes.
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        Px::from_paise(100).hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn display_renders_two_paise_digits() {
        assert_eq!(format!("{}", Px::from_paise(200_50)), "\u{20B9}200.50");
        assert_eq!(format!("{}", Px::from_paise(7)), "\u{20B9}0.07");
        assert_eq!(format!("{}", Px::from_paise(-1_05)), "-\u{20B9}1.05");
        assert_eq!(format!("{}", Px::ZERO), "\u{20B9}0.00");
    }

    #[test]
    fn px_is_copy_and_eight_bytes() {
        // FlatBuffers `long` compatibility: must be exactly 8 bytes.
        assert_eq!(std::mem::size_of::<Px>(), 8);
        fn assert_copy<T: Copy>() {}
        assert_copy::<Px>();
    }

    #[test]
    fn arithmetic_round_trip_property_demo() {
        // Demo of Property 4 — `Px` arithmetic conserves paise exactly. The
        // dedicated `proptest` suite (task 2.2) generalises this.
        let cases = [(0i64, 0i64), (100, 50), (-7, 13), (1_234_567, 89_01)];
        for (a, b) in cases {
            let pa = Px::from_paise(a);
            let pb = Px::from_paise(b);
            assert_eq!((pa + pb).to_paise(), a + b);
            assert_eq!((pa - pb).to_paise(), a - b);
            assert_eq!(((pa + pb) - pb).to_paise(), a);
        }
    }
}
