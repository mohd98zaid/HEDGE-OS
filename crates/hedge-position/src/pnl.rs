//! Volume-weighted PnL arithmetic in pure integer paise.
//!
//! Every Hot_Path price quantity is expressed in **paise** (one-hundredth of
//! one rupee). This module performs the volume-weighted-average and
//! realised-PnL math without ever touching `f32` / `f64`, satisfying the
//! "no floating point in the Hot_Path" constraint that flows from R3.6,
//! R30.4, and Property 4 (Score and Formula Equivalence).
//!
//! ## Sign convention
//!
//! [`Position::quantity`](super::position::Position::quantity) is **signed**:
//!
//! * positive → net long (buys exceed sells),
//! * negative → net short (sells exceed buys),
//! * zero    → flat.
//!
//! `avg_entry_px` is always tracked as a non-negative `Px` — sign is carried
//! by `quantity` alone. When the position is flat, `avg_entry_px` is
//! `Px::ZERO`.
//!
//! ## VWAP recurrence
//!
//! For an opening or scaling buy on a long (or opening / scaling sell on a
//! short), the new average entry price is the volume-weighted average of
//! the previous and incoming legs:
//!
//! ```text
//! new_avg = (|old_qty| * old_avg + fill_qty * fill_px) / (|old_qty| + fill_qty)
//! ```
//!
//! For a closing fill (sell against a long, or buy against a short), the
//! realised PnL contribution is:
//!
//! ```text
//! realised += (fill_px - old_avg) * close_qty       // long-closing sell
//! realised += (old_avg - fill_px) * close_qty       // short-closing buy
//! ```
//!
//! where `close_qty = min(fill_qty, |old_qty|)`. Any residual fill quantity
//! beyond `|old_qty|` flips the sign of `quantity` and starts a new leg at
//! `fill_px` — i.e. a buy that exceeds an open short closes the short and
//! opens a long at `fill_px`.
//!
//! All intermediate products use `i128` to avoid overflow on
//! pathological inputs (e.g. `qty = 10_000_000` × `px = 1_000_000_00 paise`
//! still fits well below `i128::MAX`). The final stored values are `i64`.

use hedge_core::{Px, Side};

/// Outcome of folding a single fill into a [`Position`](super::position::Position).
///
/// Returned by [`apply_fill_inner`] so the higher-level
/// [`Position::apply_fill`](super::position::Position::apply_fill) can update
/// itself and surface `delta_realized_paise` to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FillOutcome {
    /// Position quantity *after* this fill is folded in. Signed.
    pub new_quantity: i64,
    /// Volume-weighted average entry price *after* this fill. Always
    /// non-negative; `Px::ZERO` when the position is flat.
    pub new_avg_entry_px: Px,
    /// Realised PnL contributed by *this fill alone* (paise). Sign convention:
    ///
    /// * positive → profit,
    /// * negative → loss,
    /// * zero → opening / scaling fill that did not close any position.
    pub delta_realized_paise: i64,
}

/// Apply one fill to a position represented by `(old_qty, old_avg)` and
/// return the resulting state plus the realised-PnL delta.
///
/// This is the single source of truth for VWAP and realised-PnL arithmetic;
/// [`Position::apply_fill`](super::position::Position::apply_fill) is a thin
/// wrapper that mutates state on top of this function.
///
/// `fill_qty` is the unsigned magnitude of the fill (R6 OrderState_v1 carries
/// `filled_qty: ulong`). `side` is the *direction of the fill* — buy adds to
/// long / closes short, sell adds to short / closes long.
///
/// # Behavioural guarantees
///
/// 1. **Opening / scaling**: when the fill direction matches the existing
///    position direction (or the position is flat), `delta_realized_paise == 0`
///    and `new_avg_entry_px` is the volume-weighted average over `|old_qty|`
///    and `fill_qty`.
/// 2. **Partial close**: when the fill direction opposes the existing
///    position and `fill_qty <= |old_qty|`, only `quantity` shrinks (toward
///    zero); `avg_entry_px` is preserved; `delta_realized_paise` reflects
///    the closed leg.
/// 3. **Flip / over-close**: when `fill_qty > |old_qty|`, the residual
///    `fill_qty - |old_qty|` opens a fresh leg in the opposite direction at
///    `fill_px`; the closed portion still contributes to
///    `delta_realized_paise`.
/// 4. **Idempotent flat-flat**: when both `old_qty` and `fill_qty` are zero
///    the function returns the input unchanged.
///
/// # Panics (debug only)
///
/// Panics in debug builds if `(|old_qty| + fill_qty)` overflows `i128`. In
/// practice this can never trigger for any realistic NSE / BSE order book
/// because Indian equities are quoted in paise with daily volumes well below
/// `i64::MAX`.
#[inline]
pub fn apply_fill_inner(
    old_qty: i64,
    old_avg: Px,
    side: Side,
    fill_qty: u64,
    fill_px: Px,
) -> FillOutcome {
    // Treat a zero-quantity fill as a no-op; this lets callers feed
    // OrderState updates blindly without first filtering.
    if fill_qty == 0 {
        return FillOutcome {
            new_quantity: old_qty,
            new_avg_entry_px: old_avg,
            delta_realized_paise: 0,
        };
    }

    let signed_fill: i128 = match side {
        Side::Buy => fill_qty as i128,
        Side::Sell => -(fill_qty as i128),
    };
    let new_qty_signed: i128 = old_qty as i128 + signed_fill;

    // ---- Case A: position was flat. Open a fresh leg at fill_px. -------
    if old_qty == 0 {
        return FillOutcome {
            new_quantity: new_qty_signed as i64,
            new_avg_entry_px: fill_px,
            delta_realized_paise: 0,
        };
    }

    let same_direction = (old_qty > 0 && matches!(side, Side::Buy))
        || (old_qty < 0 && matches!(side, Side::Sell));

    if same_direction {
        // ---- Case B: scaling existing leg. New avg = VWAP. ------------
        let abs_old = old_qty.unsigned_abs() as i128;
        let abs_fill = fill_qty as i128;
        let denom = abs_old + abs_fill;
        // `old_avg.to_paise()` is always non-negative for a tracked
        // position; we still cast to i128 to avoid overflow on multiply.
        let num = abs_old * old_avg.to_paise() as i128 + abs_fill * fill_px.to_paise() as i128;
        let new_avg = num / denom;
        FillOutcome {
            new_quantity: new_qty_signed as i64,
            new_avg_entry_px: Px::from_paise(new_avg as i64),
            delta_realized_paise: 0,
        }
    } else {
        // ---- Case C: opposing leg. Close, possibly flip. --------------
        let abs_old = old_qty.unsigned_abs() as i128;
        let close_qty: i128 = abs_old.min(fill_qty as i128);

        // Realised PnL on the closed slice. Long-closing sell:
        //   pnl = (fill_px - old_avg) * close_qty
        // Short-closing buy:
        //   pnl = (old_avg - fill_px) * close_qty
        let realized: i128 = if old_qty > 0 {
            (fill_px.to_paise() as i128 - old_avg.to_paise() as i128) * close_qty
        } else {
            (old_avg.to_paise() as i128 - fill_px.to_paise() as i128) * close_qty
        };

        let residual: i128 = fill_qty as i128 - close_qty; // ≥ 0
        if residual == 0 {
            // Pure partial-or-full close: keep avg unless we hit zero.
            let new_qty = new_qty_signed as i64;
            let new_avg = if new_qty == 0 { Px::ZERO } else { old_avg };
            FillOutcome {
                new_quantity: new_qty,
                new_avg_entry_px: new_avg,
                delta_realized_paise: realized as i64,
            }
        } else {
            // Flipped: residual opens a new leg at fill_px in the opposite
            // direction.
            let new_qty = new_qty_signed as i64;
            FillOutcome {
                new_quantity: new_qty,
                new_avg_entry_px: fill_px,
                delta_realized_paise: realized as i64,
            }
        }
    }
}

/// Recompute unrealised PnL for a position at the supplied mark price.
///
/// Sign convention matches realised PnL:
/// `unrealized = (mark - avg) * qty` where `qty` is signed. For a long this
/// is positive when `mark > avg`; for a short it flips sign automatically
/// because `qty < 0`.
///
/// Returns paise. Uses `i128` for the multiply to avoid overflow on extreme
/// inputs.
#[inline]
pub fn unrealized_pnl_paise(qty: i64, avg_entry_px: Px, mark_px: Px) -> i64 {
    if qty == 0 {
        return 0;
    }
    let diff = mark_px.to_paise() as i128 - avg_entry_px.to_paise() as i128;
    let prod = diff * qty as i128;
    prod as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn px(paise: i64) -> Px {
        Px::from_paise(paise)
    }

    // --- apply_fill_inner ---------------------------------------------------

    #[test]
    fn opens_long_from_flat() {
        let r = apply_fill_inner(0, Px::ZERO, Side::Buy, 100, px(150_00));
        assert_eq!(r.new_quantity, 100);
        assert_eq!(r.new_avg_entry_px, px(150_00));
        assert_eq!(r.delta_realized_paise, 0);
    }

    #[test]
    fn opens_short_from_flat() {
        let r = apply_fill_inner(0, Px::ZERO, Side::Sell, 80, px(99_50));
        assert_eq!(r.new_quantity, -80);
        assert_eq!(r.new_avg_entry_px, px(99_50));
        assert_eq!(r.delta_realized_paise, 0);
    }

    #[test]
    fn scales_long_uses_vwap() {
        // Open 100 @ 100, then add 100 @ 200. Avg should be 150.
        let r = apply_fill_inner(100, px(100_00), Side::Buy, 100, px(200_00));
        assert_eq!(r.new_quantity, 200);
        assert_eq!(r.new_avg_entry_px, px(150_00));
        assert_eq!(r.delta_realized_paise, 0);
    }

    #[test]
    fn scales_short_uses_vwap() {
        // Short 100 @ 100, add 100 @ 80. Avg = 90.
        let r = apply_fill_inner(-100, px(100_00), Side::Sell, 100, px(80_00));
        assert_eq!(r.new_quantity, -200);
        assert_eq!(r.new_avg_entry_px, px(90_00));
        assert_eq!(r.delta_realized_paise, 0);
    }

    #[test]
    fn partial_close_long_realizes_pnl() {
        // Long 100 @ 100. Sell 40 @ 110 → close 40 @ +1000 paise/unit = 40_000 paise.
        let r = apply_fill_inner(100, px(100_00), Side::Sell, 40, px(110_00));
        assert_eq!(r.new_quantity, 60);
        assert_eq!(r.new_avg_entry_px, px(100_00));
        assert_eq!(r.delta_realized_paise, 10_00 * 40);
    }

    #[test]
    fn full_close_long_resets_avg_to_zero() {
        // Long 100 @ 100. Sell 100 @ 105.
        let r = apply_fill_inner(100, px(100_00), Side::Sell, 100, px(105_00));
        assert_eq!(r.new_quantity, 0);
        assert_eq!(r.new_avg_entry_px, Px::ZERO);
        assert_eq!(r.delta_realized_paise, 5_00 * 100);
    }

    #[test]
    fn partial_close_short_realizes_pnl() {
        // Short 100 @ 100. Buy 30 @ 95 → realised = (100-95)*30 = 15_000 paise.
        let r = apply_fill_inner(-100, px(100_00), Side::Buy, 30, px(95_00));
        assert_eq!(r.new_quantity, -70);
        assert_eq!(r.new_avg_entry_px, px(100_00));
        assert_eq!(r.delta_realized_paise, 5_00 * 30);
    }

    #[test]
    fn losing_partial_close_long_yields_negative_pnl() {
        // Long 100 @ 100. Sell 50 @ 90 → realised = (90-100)*50 = -50_000 paise.
        let r = apply_fill_inner(100, px(100_00), Side::Sell, 50, px(90_00));
        assert_eq!(r.new_quantity, 50);
        assert_eq!(r.new_avg_entry_px, px(100_00));
        assert_eq!(r.delta_realized_paise, -10_00 * 50);
    }

    #[test]
    fn long_to_short_flip_resets_avg_to_fill_price() {
        // Long 60 @ 100. Sell 100 @ 110.
        // Closes 60 @ +10/unit = 60_000 paise.
        // Residual 40 sells at 110 → opens short -40 @ 110.
        let r = apply_fill_inner(60, px(100_00), Side::Sell, 100, px(110_00));
        assert_eq!(r.new_quantity, -40);
        assert_eq!(r.new_avg_entry_px, px(110_00));
        assert_eq!(r.delta_realized_paise, 10_00 * 60);
    }

    #[test]
    fn short_to_long_flip_resets_avg_to_fill_price() {
        // Short 50 @ 100. Buy 80 @ 95.
        // Closes 50 @ +5/unit = 25_000 paise.
        // Residual 30 buys at 95 → opens long 30 @ 95.
        let r = apply_fill_inner(-50, px(100_00), Side::Buy, 80, px(95_00));
        assert_eq!(r.new_quantity, 30);
        assert_eq!(r.new_avg_entry_px, px(95_00));
        assert_eq!(r.delta_realized_paise, 5_00 * 50);
    }

    #[test]
    fn zero_qty_fill_is_noop() {
        let r = apply_fill_inner(100, px(100_00), Side::Buy, 0, px(200_00));
        assert_eq!(r.new_quantity, 100);
        assert_eq!(r.new_avg_entry_px, px(100_00));
        assert_eq!(r.delta_realized_paise, 0);
    }

    // --- unrealized_pnl_paise -----------------------------------------------

    #[test]
    fn unrealized_zero_when_flat() {
        assert_eq!(unrealized_pnl_paise(0, Px::ZERO, px(100_00)), 0);
    }

    #[test]
    fn unrealized_long_in_profit() {
        // Long 100 @ 100. Mark @ 105 → +500 paise/unit × 100 = 50_000 paise.
        assert_eq!(unrealized_pnl_paise(100, px(100_00), px(105_00)), 5_00 * 100);
    }

    #[test]
    fn unrealized_long_in_loss() {
        // Long 100 @ 100. Mark @ 90 → -1000 paise/unit × 100 = -100_000 paise.
        assert_eq!(unrealized_pnl_paise(100, px(100_00), px(90_00)), -10_00 * 100);
    }

    #[test]
    fn unrealized_short_in_profit() {
        // Short 100 @ 100. Mark @ 95 → diff=-500, qty=-100 → +500*100 = +50_000 paise.
        assert_eq!(unrealized_pnl_paise(-100, px(100_00), px(95_00)), 5_00 * 100);
    }

    #[test]
    fn unrealized_short_in_loss() {
        // Short 100 @ 100. Mark @ 110 → diff=+1000, qty=-100 → -1000*100 = -100_000 paise.
        assert_eq!(unrealized_pnl_paise(-100, px(100_00), px(110_00)), -10_00 * 100);
    }
}
