//! Live per-symbol position tracker (R8.1).
//!
//! [`Position`] stores the canonical state for a single symbol: signed
//! quantity, volume-weighted average entry price, and both realised /
//! unrealised PnL in paise. The struct is updated by exactly two methods:
//!
//! * [`Position::apply_fill`] — folds a single broker fill into the
//!   position using the [`pnl`](crate::pnl) arithmetic and returns the
//!   realised-PnL delta (R8.2).
//! * [`Position::apply_mark`] — updates `last_mark_px` and recomputes
//!   `unrealized_pnl_paise` from the new mark (R8.3).
//!
//! Per-strategy capital accounting (R8.4) lives in
//! [`StrategyAllocation`]; the engine owns a small inline vector of these
//! per [`Position`].

use hedge_core::{Px, Side, SmallVec, SymbolId};

use crate::pnl::{apply_fill_inner, unrealized_pnl_paise};

/// Per-strategy capital allocation against a single symbol (R8.4).
///
/// Stored inline in [`Position::strategy_allocations`] because we expect at
/// most a handful of strategies to share a symbol simultaneously. The
/// `quantity` field is signed so a strategy can hold the long side of a
/// hedged spread on the same symbol another strategy is shorting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct StrategyAllocation {
    /// Stable per-strategy id (matches `hedge_schemas::strategy_id::StrategyId::as_u8`).
    pub strategy_id: u8,
    /// Strategy's signed quantity contribution to this symbol.
    pub quantity: i64,
    /// Capital (in paise — internally we always work in paise) allocated by
    /// this strategy to this symbol. Stored as `i64` because the design
    /// surfaces it as INR but paise gives us four-decimal headroom for
    /// intra-day variation.
    pub allocated_capital_inr: i64,
}

/// Live state for a single symbol.
///
/// All monetary fields are paise (`i64`). The struct is `Default` so callers
/// can spawn a fresh flat position with `Position::default_for(symbol)`.
#[derive(Debug, Clone)]
pub struct Position {
    /// Symbol this position tracks.
    pub symbol: SymbolId,
    /// Signed net quantity (positive = long, negative = short, zero = flat).
    pub quantity: i64,
    /// Volume-weighted average entry price; non-negative; `Px::ZERO` when flat.
    pub avg_entry_px: Px,
    /// Realised PnL accumulated across all closing fills (paise).
    pub realized_pnl_paise: i64,
    /// Last computed unrealised PnL at `last_mark_px` (paise). Recomputed by
    /// [`Position::apply_mark`] and again at the end of [`Position::apply_fill`]
    /// so the field is always consistent with the current state.
    pub unrealized_pnl_paise: i64,
    /// Last mark price seen. Defaults to `Px::ZERO` for a fresh position so
    /// `apply_fill` produces sensible (zero) unrealised values until a tick
    /// arrives.
    pub last_mark_px: Px,
    /// Per-strategy attribution. SmallVec of capacity 4 because a symbol is
    /// rarely shared by more than four concurrent strategies.
    pub strategy_allocations: SmallVec<[StrategyAllocation; 4]>,
}

impl Position {
    /// Construct a flat position for `symbol`.
    #[inline]
    pub fn flat(symbol: SymbolId) -> Self {
        Self {
            symbol,
            quantity: 0,
            avg_entry_px: Px::ZERO,
            realized_pnl_paise: 0,
            unrealized_pnl_paise: 0,
            last_mark_px: Px::ZERO,
            strategy_allocations: SmallVec::new(),
        }
    }

    /// Returns `true` when the position has zero net quantity.
    #[inline]
    pub fn is_flat(&self) -> bool {
        self.quantity == 0
    }

    /// Absolute exposure in paise (|qty| × avg_entry_px).
    ///
    /// For a flat position returns 0. The result is `i64`; pathological
    /// inputs that would overflow saturate to `i64::MAX`.
    #[inline]
    pub fn exposure_paise(&self) -> i64 {
        if self.quantity == 0 {
            return 0;
        }
        let abs_qty = self.quantity.unsigned_abs() as i128;
        let prod = abs_qty * self.avg_entry_px.to_paise() as i128;
        if prod > i64::MAX as i128 {
            i64::MAX
        } else {
            prod as i64
        }
    }

    /// Total PnL (realised + unrealised) in paise.
    #[inline]
    pub fn total_pnl_paise(&self) -> i64 {
        self.realized_pnl_paise
            .saturating_add(self.unrealized_pnl_paise)
    }

    /// Fold a single fill into this position.
    ///
    /// Returns the realised-PnL **delta contributed by this fill alone**
    /// (paise). The cumulative `realized_pnl_paise` and `unrealized_pnl_paise`
    /// fields are also updated; the latter is recomputed from the new
    /// quantity / avg / `last_mark_px` so the position remains internally
    /// consistent.
    ///
    /// `fill_qty` is the unsigned magnitude of the fill (it matches
    /// `OrderState_v1.filled_qty: ulong`). `side` is the direction the fill
    /// was executed in.
    #[inline]
    pub fn apply_fill(&mut self, side: Side, fill_qty: u64, fill_px: Px) -> i64 {
        let outcome = apply_fill_inner(self.quantity, self.avg_entry_px, side, fill_qty, fill_px);
        self.quantity = outcome.new_quantity;
        self.avg_entry_px = outcome.new_avg_entry_px;
        self.realized_pnl_paise = self
            .realized_pnl_paise
            .saturating_add(outcome.delta_realized_paise);
        // Recompute unrealised at the prevailing last mark so the cached
        // value never drifts after a fill at a different price than the
        // last tick.
        self.unrealized_pnl_paise =
            unrealized_pnl_paise(self.quantity, self.avg_entry_px, self.last_mark_px);
        outcome.delta_realized_paise
    }

    /// Update `last_mark_px` and recompute `unrealized_pnl_paise`.
    ///
    /// Realised PnL is **not** touched (R8.3 says only unrealised PnL is
    /// updated on a tick). Returns the new `unrealized_pnl_paise` value.
    #[inline]
    pub fn apply_mark(&mut self, mark_px: Px) -> i64 {
        self.last_mark_px = mark_px;
        self.unrealized_pnl_paise =
            unrealized_pnl_paise(self.quantity, self.avg_entry_px, mark_px);
        self.unrealized_pnl_paise
    }

    /// Replace this position's strategy allocations.
    ///
    /// The caller is responsible for ensuring `quantity` matches the sum of
    /// the per-strategy `quantity` fields (the engine enforces this when
    /// strategies report their fills).
    #[inline]
    pub fn set_strategy_allocations(&mut self, allocs: SmallVec<[StrategyAllocation; 4]>) {
        self.strategy_allocations = allocs;
    }

    /// Borrow current strategy allocations (R8.4).
    #[inline]
    pub fn strategy_allocations(&self) -> &[StrategyAllocation] {
        &self.strategy_allocations
    }
}

impl Default for Position {
    /// `Default::default()` returns a flat position on `SymbolId(0)`. Use
    /// [`Position::flat`] when constructing for a specific symbol.
    fn default() -> Self {
        Self::flat(SymbolId::new(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn px(paise: i64) -> Px {
        Px::from_paise(paise)
    }

    fn fresh() -> Position {
        Position::flat(SymbolId::new(42))
    }

    // R8.1: maintain quantity, avg_entry, realized + unrealized.

    #[test]
    fn long_open_then_add_then_partial_close_then_full_close() {
        let mut p = fresh();

        // Open 100 @ 100.
        let d = p.apply_fill(Side::Buy, 100, px(100_00));
        assert_eq!(d, 0);
        assert_eq!(p.quantity, 100);
        assert_eq!(p.avg_entry_px, px(100_00));
        assert_eq!(p.realized_pnl_paise, 0);

        // Add 100 @ 200 → avg = 150, qty = 200.
        let d = p.apply_fill(Side::Buy, 100, px(200_00));
        assert_eq!(d, 0);
        assert_eq!(p.quantity, 200);
        assert_eq!(p.avg_entry_px, px(150_00));
        assert_eq!(p.realized_pnl_paise, 0);

        // Partial close 80 @ 160 → realised = (160-150)*80 = 800_paise * 80 = 80_000.
        let d = p.apply_fill(Side::Sell, 80, px(160_00));
        assert_eq!(d, 10_00 * 80);
        assert_eq!(p.quantity, 120);
        assert_eq!(p.avg_entry_px, px(150_00));
        assert_eq!(p.realized_pnl_paise, 10_00 * 80);

        // Full close 120 @ 140 → realised = (140-150)*120 = -120_000 paise.
        let d = p.apply_fill(Side::Sell, 120, px(140_00));
        assert_eq!(d, -10_00 * 120);
        assert_eq!(p.quantity, 0);
        assert_eq!(p.avg_entry_px, Px::ZERO);
        assert_eq!(p.realized_pnl_paise, 10_00 * 80 + (-10_00 * 120));
        assert!(p.is_flat());
    }

    #[test]
    fn long_reversal_to_short_flips_sign_and_resets_avg() {
        let mut p = fresh();
        p.apply_fill(Side::Buy, 60, px(100_00));
        // Sell 100 @ 110: closes 60 @ +10/unit = 60_000 paise; opens short 40 @ 110.
        let d = p.apply_fill(Side::Sell, 100, px(110_00));
        assert_eq!(d, 10_00 * 60);
        assert_eq!(p.quantity, -40);
        assert_eq!(p.avg_entry_px, px(110_00));
        assert_eq!(p.realized_pnl_paise, 10_00 * 60);
    }

    #[test]
    fn short_open_then_add_then_partial_close_then_full_close() {
        let mut p = fresh();

        // Short 100 @ 100.
        p.apply_fill(Side::Sell, 100, px(100_00));
        assert_eq!(p.quantity, -100);
        assert_eq!(p.avg_entry_px, px(100_00));

        // Add 100 @ 80 → avg = 90, qty = -200.
        p.apply_fill(Side::Sell, 100, px(80_00));
        assert_eq!(p.quantity, -200);
        assert_eq!(p.avg_entry_px, px(90_00));

        // Partial close 80 @ 70 → realised = (90-70)*80 = 20_00*80 = 160_000 paise.
        let d = p.apply_fill(Side::Buy, 80, px(70_00));
        assert_eq!(d, 20_00 * 80);
        assert_eq!(p.quantity, -120);
        assert_eq!(p.avg_entry_px, px(90_00));

        // Full close 120 @ 100 → realised = (90-100)*120 = -120_000.
        let d = p.apply_fill(Side::Buy, 120, px(100_00));
        assert_eq!(d, -10_00 * 120);
        assert_eq!(p.quantity, 0);
        assert_eq!(p.avg_entry_px, Px::ZERO);
        assert_eq!(p.realized_pnl_paise, 20_00 * 80 + (-10_00 * 120));
    }

    #[test]
    fn short_reversal_to_long_flips_sign_and_resets_avg() {
        let mut p = fresh();
        p.apply_fill(Side::Sell, 50, px(100_00));
        // Buy 80 @ 95: closes 50 @ +5/unit = 25_000 paise; opens long 30 @ 95.
        let d = p.apply_fill(Side::Buy, 80, px(95_00));
        assert_eq!(d, 5_00 * 50);
        assert_eq!(p.quantity, 30);
        assert_eq!(p.avg_entry_px, px(95_00));
        assert_eq!(p.realized_pnl_paise, 5_00 * 50);
    }

    // R8.3: tick mark only updates unrealized; realized is untouched.

    #[test]
    fn apply_mark_only_changes_unrealized_pnl() {
        let mut p = fresh();
        p.apply_fill(Side::Buy, 100, px(100_00));
        let realized_before = p.realized_pnl_paise;

        let unreal = p.apply_mark(px(105_00));
        assert_eq!(unreal, 5_00 * 100);
        assert_eq!(p.unrealized_pnl_paise, 5_00 * 100);
        assert_eq!(p.realized_pnl_paise, realized_before);
        assert_eq!(p.last_mark_px, px(105_00));

        // Drop mark — realised still unchanged.
        let unreal = p.apply_mark(px(95_00));
        assert_eq!(unreal, -5_00 * 100);
        assert_eq!(p.realized_pnl_paise, realized_before);
    }

    #[test]
    fn flat_position_has_zero_unrealized_at_any_mark() {
        let mut p = fresh();
        assert_eq!(p.apply_mark(px(100_00)), 0);
        assert_eq!(p.apply_mark(px(50_00)), 0);
    }

    #[test]
    fn fill_recomputes_unrealized_at_last_mark() {
        let mut p = fresh();
        p.apply_mark(px(105_00));
        // Open 100 @ 100; with last mark 105 unrealised should be +5*100 = 500_paise * 100.
        p.apply_fill(Side::Buy, 100, px(100_00));
        assert_eq!(p.unrealized_pnl_paise, 5_00 * 100);
    }

    // Helpers / accessors.

    #[test]
    fn exposure_and_total_pnl_helpers() {
        let mut p = fresh();
        p.apply_fill(Side::Buy, 100, px(150_00));
        assert_eq!(p.exposure_paise(), 100 * 150_00);
        p.apply_mark(px(160_00));
        assert_eq!(p.total_pnl_paise(), p.realized_pnl_paise + p.unrealized_pnl_paise);
    }

    #[test]
    fn strategy_allocations_round_trip() {
        let mut p = fresh();
        let mut allocs: SmallVec<[StrategyAllocation; 4]> = SmallVec::new();
        allocs.push(StrategyAllocation {
            strategy_id: 1,
            quantity: 60,
            allocated_capital_inr: 6_000,
        });
        allocs.push(StrategyAllocation {
            strategy_id: 3,
            quantity: 40,
            allocated_capital_inr: 4_000,
        });
        p.set_strategy_allocations(allocs);
        assert_eq!(p.strategy_allocations().len(), 2);
        assert_eq!(p.strategy_allocations()[0].strategy_id, 1);
        assert_eq!(p.strategy_allocations()[1].quantity, 40);
    }
}
