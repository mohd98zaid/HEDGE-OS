//! C.12 — Conservation-of-cash property test (full-cockpit-data spec).
//!
//! **Validates: Requirement 8.6** — for any random sequence of fills and
//! mark updates on a single symbol, the Position_Engine's accounting must
//! conserve cash exactly. Concretely, three invariants hold for every
//! reachable position state:
//!
//! 1. **Realised-PnL conservation.** Cumulative `realized_pnl_paise` always
//!    equals the closed-leg cash flow computed independently from the fill
//!    tape: every closing slice contributes `(exit − entry) × qty` for a
//!    long (and the mirror for a short). We recompute this with an
//!    independent reference ledger and assert byte-equality with the
//!    engine's running total.
//!
//! 2. **Flat ⇒ unrealised is zero.** Whenever the net quantity returns to
//!    zero, `unrealized_pnl_paise` is exactly zero — a flat book has no
//!    open mark-to-market exposure.
//!
//! 3. **Total-P&L identity.** At any mark, `total_pnl = realized +
//!    unrealized`, and `unrealized = (mark − avg) × qty` exactly (integer
//!    paise, no float drift).
//!
//! The "conservation" framing: cash can only be created or destroyed by a
//! *closing* fill (realised) or by a *price move on an open position*
//! (unrealised). An opening/scaling fill moves cash between "cash" and
//! "position cost basis" without creating P&L. The reference ledger below
//! encodes exactly that bookkeeping and must agree with the engine for
//! every random tape.
//!
//! These tests exercise the public `Position` API only and live in the
//! integration `tests/` directory so `cargo test -p hedge-position` runs
//! them at the workspace level.

use hedge_core::{Px, Side, SymbolId};
use hedge_position::Position;
use proptest::prelude::*;

/// One event on the random tape fed to the engine: a fill or a mark.
#[derive(Clone, Debug)]
enum TapeEvent {
    Fill { side: Side, qty: u64, px_paise: i64 },
    Mark { px_paise: i64 },
}

/// Generator: a price between ₹1.00 and ₹10,000.00 in paise. Bounded so
/// `qty × px` products stay comfortably inside `i128` even when summed over
/// a long tape.
fn px_paise_strategy() -> impl Strategy<Value = i64> {
    1_00i64..10_000_00
}

/// Generator: a fill quantity in `[1, 10_000]`.
fn qty_strategy() -> impl Strategy<Value = u64> {
    1u64..=10_000
}

/// Generator: one tape event. ~70% fills, ~30% marks so closes happen
/// often enough to exercise realised-PnL paths.
fn event_strategy() -> impl Strategy<Value = TapeEvent> {
    prop_oneof![
        7 => (any::<bool>(), qty_strategy(), px_paise_strategy()).prop_map(
            |(is_buy, qty, px_paise)| TapeEvent::Fill {
                side: if is_buy { Side::Buy } else { Side::Sell },
                qty,
                px_paise,
            }
        ),
        3 => px_paise_strategy().prop_map(|px_paise| TapeEvent::Mark { px_paise }),
    ]
}

/// Generator: a tape of 1..=40 events.
fn tape_strategy() -> impl Strategy<Value = Vec<TapeEvent>> {
    prop::collection::vec(event_strategy(), 1..=40)
}

/// Independent reference ledger mirroring the documented VWAP +
/// realised-PnL bookkeeping in `hedge_position::pnl`. This is a *second
/// implementation* of the accounting so the property test cross-checks the
/// engine against an independent computation rather than against itself.
#[derive(Default)]
struct RefLedger {
    qty: i64,
    avg_paise: i64,
    realized_paise: i128,
}

impl RefLedger {
    fn apply_fill(&mut self, side: Side, fill_qty: u64, fill_px: i64) {
        if fill_qty == 0 {
            return;
        }
        let signed: i128 = match side {
            Side::Buy => fill_qty as i128,
            Side::Sell => -(fill_qty as i128),
        };
        let new_qty = self.qty as i128 + signed;

        if self.qty == 0 {
            // Open fresh leg.
            self.qty = new_qty as i64;
            self.avg_paise = fill_px;
            return;
        }

        let same_dir = (self.qty > 0 && matches!(side, Side::Buy))
            || (self.qty < 0 && matches!(side, Side::Sell));

        if same_dir {
            // Scale: VWAP over |old| and fill.
            let abs_old = self.qty.unsigned_abs() as i128;
            let abs_fill = fill_qty as i128;
            let num = abs_old * self.avg_paise as i128 + abs_fill * fill_px as i128;
            self.avg_paise = (num / (abs_old + abs_fill)) as i64;
            self.qty = new_qty as i64;
        } else {
            // Opposing: close (possibly flip).
            let abs_old = self.qty.unsigned_abs() as i128;
            let close_qty = abs_old.min(fill_qty as i128);
            let realized = if self.qty > 0 {
                (fill_px as i128 - self.avg_paise as i128) * close_qty
            } else {
                (self.avg_paise as i128 - fill_px as i128) * close_qty
            };
            self.realized_paise += realized;

            let residual = fill_qty as i128 - close_qty;
            self.qty = new_qty as i64;
            if residual == 0 {
                if self.qty == 0 {
                    self.avg_paise = 0;
                }
                // else: partial close, avg preserved.
            } else {
                // Flipped: residual opens a new leg at fill_px.
                self.avg_paise = fill_px;
            }
        }
    }

    fn unrealized_paise(&self, mark_paise: i64) -> i128 {
        if self.qty == 0 || mark_paise == 0 {
            return 0;
        }
        (mark_paise as i128 - self.avg_paise as i128) * self.qty as i128
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// **Validates: Requirement 8.6** — the engine's cumulative realised
    /// PnL equals the independent reference ledger after every event on a
    /// random tape, and the total-P&L identity + flat-book invariant hold
    /// throughout.
    #[test]
    fn conservation_of_cash_holds_over_random_tape(tape in tape_strategy()) {
        let mut pos = Position::flat(SymbolId::new(1));
        let mut ledger = RefLedger::default();
        // Track the most recent mark so we can assert the unrealised
        // identity after fills too (the engine recomputes unrealised at
        // the prevailing last mark on every fill).
        let mut last_mark_paise: i64 = 0;

        for ev in &tape {
            match *ev {
                TapeEvent::Fill { side, qty, px_paise } => {
                    pos.apply_fill(side, qty, Px::from_paise(px_paise));
                    ledger.apply_fill(side, qty, px_paise);
                }
                TapeEvent::Mark { px_paise } => {
                    pos.apply_mark(Px::from_paise(px_paise));
                    last_mark_paise = px_paise;
                }
            }

            // Invariant 1: realised PnL matches the independent ledger.
            prop_assert_eq!(
                pos.realized_pnl_paise as i128,
                ledger.realized_paise,
                "realised PnL diverged from reference ledger"
            );

            // Invariant: quantity and avg-cost agree with the ledger.
            prop_assert_eq!(pos.quantity, ledger.qty, "quantity diverged");
            if pos.quantity != 0 {
                prop_assert_eq!(
                    pos.avg_entry_px.to_paise(),
                    ledger.avg_paise,
                    "avg entry price diverged"
                );
            }

            // Invariant 2: flat book ⇒ zero unrealised.
            if pos.quantity == 0 {
                prop_assert_eq!(
                    pos.unrealized_pnl_paise, 0,
                    "flat position must have zero unrealised PnL"
                );
            }

            // Invariant 3: total-P&L identity + unrealised closed form at
            // the prevailing mark.
            prop_assert_eq!(
                pos.total_pnl_paise(),
                pos.realized_pnl_paise.saturating_add(pos.unrealized_pnl_paise),
                "total_pnl != realized + unrealized"
            );
            prop_assert_eq!(
                pos.unrealized_pnl_paise as i128,
                ledger.unrealized_paise(last_mark_paise),
                "unrealised PnL != (mark - avg) * qty"
            );
        }
    }

    /// **Validates: Requirement 8.6** — when a book is opened entirely at a
    /// **single** entry price and then fully closed, *all* P&L is realised
    /// and equals the exact net cash flow (Σ sell proceeds − Σ buy cost),
    /// with zero residual unrealised.
    ///
    /// A single entry price is used deliberately: with multiple entry prices
    /// the volume-weighted average is computed by integer division, which
    /// truncates sub-paise fractions — so realised P&L would differ from the
    /// "exact" cash flow by a bounded rounding term. That rounding behaviour
    /// is the engine's documented integer-paise semantics and is covered by
    /// the random-tape test above (which cross-checks against an independent
    /// ledger using the *same* truncation). Here we pin the entry price so
    /// the VWAP is exact and the conservation identity is exact.
    #[test]
    fn fully_closed_book_realises_exact_net_cashflow(
        entry_px in px_paise_strategy(),
        buy_qtys in prop::collection::vec(qty_strategy(), 1..=10),
        sell_pxs in prop::collection::vec(px_paise_strategy(), 1..=10),
    ) {
        let total_qty: u64 = buy_qtys.iter().sum();

        let mut pos = Position::flat(SymbolId::new(7));

        // Open the entire book at one price → avg == entry_px exactly.
        let mut buy_cost: i128 = 0;
        for q in &buy_qtys {
            pos.apply_fill(Side::Buy, *q, Px::from_paise(entry_px));
            buy_cost += (*q as i128) * (entry_px as i128);
        }
        prop_assert_eq!(
            pos.avg_entry_px.to_paise(), entry_px,
            "single-price book must have an exact (untruncated) avg"
        );

        // Close the entire book across a few sell slices at varying prices.
        let n = sell_pxs.len() as u64;
        let base = total_qty / n;
        let mut remaining = total_qty;
        let mut sell_proceeds: i128 = 0;
        for (i, px) in sell_pxs.iter().enumerate() {
            let slice = if i as u64 + 1 == n { remaining } else { base };
            if slice == 0 {
                continue;
            }
            pos.apply_fill(Side::Sell, slice, Px::from_paise(*px));
            sell_proceeds += (slice as i128) * (*px as i128);
            remaining -= slice;
        }

        // Book must be flat.
        prop_assert_eq!(pos.quantity, 0, "book should be flat after matched sells");
        prop_assert_eq!(pos.unrealized_pnl_paise, 0, "flat ⇒ zero unrealised");

        // Realised PnL == sell proceeds − buy cost (exact integer paise).
        prop_assert_eq!(
            pos.realized_pnl_paise as i128,
            sell_proceeds - buy_cost,
            "realised PnL must equal net cash flow of the fully-closed book"
        );
    }

    /// **Validates: Requirement 8.6** — even with *multiple* entry prices
    /// (VWAP truncation in play), realised P&L on a fully-closed book stays
    /// within the bounded rounding envelope of the exact net cash flow:
    /// `0 <= realised − exact < num_buy_legs × total_qty` paise. This proves
    /// the engine never *creates or destroys* cash beyond integer-division
    /// rounding — the precise statement of conservation under integer paise.
    #[test]
    fn fully_closed_book_conserves_cash_within_rounding(
        legs in prop::collection::vec((qty_strategy(), px_paise_strategy()), 1..=10),
        sell_pxs in prop::collection::vec(px_paise_strategy(), 1..=10),
    ) {
        let total_qty: u64 = legs.iter().map(|(q, _)| *q).sum();
        let num_legs = legs.len() as i128;

        let mut pos = Position::flat(SymbolId::new(11));
        let mut buy_cost: i128 = 0;
        for (q, px) in &legs {
            pos.apply_fill(Side::Buy, *q, Px::from_paise(*px));
            buy_cost += (*q as i128) * (*px as i128);
        }

        let n = sell_pxs.len() as u64;
        let base = total_qty / n;
        let mut remaining = total_qty;
        let mut sell_proceeds: i128 = 0;
        for (i, px) in sell_pxs.iter().enumerate() {
            let slice = if i as u64 + 1 == n { remaining } else { base };
            if slice == 0 {
                continue;
            }
            pos.apply_fill(Side::Sell, slice, Px::from_paise(*px));
            sell_proceeds += (slice as i128) * (*px as i128);
            remaining -= slice;
        }

        prop_assert_eq!(pos.quantity, 0, "book should be flat");
        prop_assert_eq!(pos.unrealized_pnl_paise, 0, "flat ⇒ zero unrealised");

        let exact = sell_proceeds - buy_cost;
        let realised = pos.realized_pnl_paise as i128;
        let drift = realised - exact;
        // Truncating the VWAP down makes the entry cost basis no larger than
        // the true basis, so realised is >= exact, and the gap is bounded by
        // the per-leg truncation (< 1 paise on the avg) times the closed qty.
        prop_assert!(
            drift >= 0,
            "realised ({}) must not be below exact net cash flow ({})", realised, exact
        );
        prop_assert!(
            drift < num_legs * (total_qty as i128) + 1,
            "cash drift {} exceeded the integer-rounding envelope (legs={}, qty={})",
            drift, num_legs, total_qty
        );
    }
}
