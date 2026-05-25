//! In-memory L2 orderbook representation.
//!
//! [`LiveBook`] mirrors the relevant subset of `OrderBook_v1`
//! (`hedge_schemas::OrderBook`) into a no-allocation inline structure that
//! the Orderflow_Engine can update on every `md.book.<sym>` payload without
//! touching the heap (R2.6).
//!
//! The design caps level-2 depth at 20 (`MAX_BOOK_LEVELS = 20`); both bid
//! and ask sides are stored as
//! [`ArrayVec<BookLevel, MAX_BOOK_LEVELS>`](arrayvec::ArrayVec) so the
//! capacity is fixed at compile time and overflow is observable via
//! `try_push`.
//!
//! ### Update strategy
//!
//! NSE/BSE level-2 updates carry the **full** snapshot of the top-N levels
//! per side per book event. The book's update path therefore replaces both
//! sides in place rather than diffing — this is what the wire payload
//! supports and is also the cheapest path (one `clear` + `extend_from_slice`
//! per side, neither of which allocates because the storage is inline).
//!
//! `last_seq` is the per-symbol sequence number we infer from the
//! exchange-stamped `ts_ns` field in `OrderBook_v1`. The schema does not
//! carry an explicit sequence number, so we use `ts_ns` as a monotonic
//! ordering key — out-of-order book updates are dropped silently rather
//! than rolling state backwards (Property 9 in spirit).

use hedge_core::{ArrayVec, Px};
use hedge_schemas::{BookLevel, OrderBook};

/// Maximum number of bid or ask levels held per side. Mirrors
/// [`hedge_schemas::generated::orderbook_generated::MAX_BOOK_LEVELS`] so the
/// type does not depend on internal generated paths.
pub const MAX_BOOK_LEVELS: usize = 20;

/// In-memory live orderbook for one symbol.
///
/// Both sides use [`ArrayVec`] with inline storage of `MAX_BOOK_LEVELS`
/// elements so steady-state updates allocate nothing on the heap (R2.6).
///
/// Bids are stored from best (highest price) to worst; asks are stored from
/// best (lowest price) to worst. Callers SHOULD NOT mutate the vectors
/// directly — always go through [`LiveBook::apply`] so `last_seq` and the
/// ordering invariants are maintained together.
#[derive(Debug, Clone, Default)]
pub struct LiveBook {
    /// Bids sorted from best (top of book) to worst.
    pub bids: ArrayVec<BookLevel, MAX_BOOK_LEVELS>,
    /// Asks sorted from best (top of book) to worst.
    pub asks: ArrayVec<BookLevel, MAX_BOOK_LEVELS>,
    /// Monotonic ordering key of the most recent update applied. Subsequent
    /// updates with `ts_ns <= last_seq` are dropped.
    pub last_seq: u64,
}

impl LiveBook {
    /// Construct an empty live book.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` when neither side carries any levels.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Best bid (top-of-book buy quote).
    #[inline]
    pub fn top_bid(&self) -> Option<&BookLevel> {
        self.bids.first()
    }

    /// Best ask (top-of-book sell quote).
    #[inline]
    pub fn top_ask(&self) -> Option<&BookLevel> {
        self.asks.first()
    }

    /// Top-of-book bid price expressed as a [`Px`], or `None` if the book is
    /// empty on the bid side.
    #[inline]
    pub fn top_bid_px(&self) -> Option<Px> {
        self.top_bid().map(|lvl| Px::from_paise(lvl.price_paise))
    }

    /// Top-of-book ask price expressed as a [`Px`], or `None` if the book is
    /// empty on the ask side.
    #[inline]
    pub fn top_ask_px(&self) -> Option<Px> {
        self.top_ask().map(|lvl| Px::from_paise(lvl.price_paise))
    }

    /// Borrow the bid levels as a slice.
    #[inline]
    pub fn bid_levels(&self) -> &[BookLevel] {
        self.bids.as_slice()
    }

    /// Borrow the ask levels as a slice.
    #[inline]
    pub fn ask_levels(&self) -> &[BookLevel] {
        self.asks.as_slice()
    }

    /// Apply a wire `OrderBook_v1` payload to the live book.
    ///
    /// Returns `true` when the update was applied, `false` when it was
    /// dropped because the carried `ts_ns` is `<= last_seq` (out-of-order).
    /// Updates with strictly newer `ts_ns` replace both sides in place; we
    /// truncate to `MAX_BOOK_LEVELS` rather than failing because the wire
    /// schema's `Vec<BookLevel>` has no upstream cap.
    ///
    /// **Allocation discipline**: the `clear()` calls drop nothing on the
    /// heap (the inline storage is `Copy + Default`); `try_push` is bounded
    /// at the inline capacity so it cannot spill. Steady-state book updates
    /// therefore touch the allocator zero times — verified by the
    /// `assert_no_alloc` test harness in `engine.rs`.
    pub fn apply(&mut self, book: &OrderBook) -> bool {
        if book.ts_ns <= self.last_seq && self.last_seq != 0 {
            // Out-of-order or duplicate. Initial application (last_seq == 0)
            // is always accepted regardless of the carried ts_ns, so the
            // first book event after process startup is never spuriously
            // rejected.
            return false;
        }
        self.bids.clear();
        self.asks.clear();
        for lvl in book.bid_levels.iter().take(MAX_BOOK_LEVELS) {
            // try_push cannot fail here because we capped the iter at
            // MAX_BOOK_LEVELS, but we use the explicit form so a future
            // change to the cap surfaces as a Result rather than a panic.
            let _ = self.bids.try_push(*lvl);
        }
        for lvl in book.ask_levels.iter().take(MAX_BOOK_LEVELS) {
            let _ = self.asks.try_push(*lvl);
        }
        self.last_seq = book.ts_ns;
        true
    }

    /// Force-reset the book to the empty state. Used on session start /
    /// supervisor recovery. Does not allocate.
    #[inline]
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_seq = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(price_paise: i64, qty: u64) -> BookLevel {
        BookLevel {
            price_paise,
            qty,
            orders: 1,
        }
    }

    fn book_with(ts_ns: u64, bids: &[BookLevel], asks: &[BookLevel]) -> OrderBook {
        OrderBook {
            correlation_id: [0u8; 16],
            symbol: 1,
            exchange: 0,
            bid_levels: bids.to_vec(),
            ask_levels: asks.to_vec(),
            ts_ns,
        }
    }

    #[test]
    fn new_book_is_empty() {
        let b = LiveBook::new();
        assert!(b.is_empty());
        assert!(b.top_bid().is_none());
        assert!(b.top_ask().is_none());
        assert_eq!(b.last_seq, 0);
    }

    #[test]
    fn apply_replaces_both_sides_and_advances_last_seq() {
        let mut b = LiveBook::new();
        let book = book_with(
            100,
            &[lvl(10000, 50), lvl(9900, 100)],
            &[lvl(10100, 40), lvl(10200, 80)],
        );
        assert!(b.apply(&book));
        assert_eq!(b.last_seq, 100);
        assert_eq!(b.top_bid().unwrap().price_paise, 10000);
        assert_eq!(b.top_ask().unwrap().price_paise, 10100);
        assert_eq!(b.bid_levels().len(), 2);
        assert_eq!(b.ask_levels().len(), 2);
    }

    #[test]
    fn out_of_order_update_is_rejected() {
        let mut b = LiveBook::new();
        let first = book_with(200, &[lvl(10000, 50)], &[lvl(10100, 40)]);
        let stale = book_with(100, &[lvl(11111, 999)], &[lvl(22222, 888)]);
        assert!(b.apply(&first));
        assert!(!b.apply(&stale), "stale update must be rejected");
        // Original state is preserved.
        assert_eq!(b.top_bid().unwrap().price_paise, 10000);
        assert_eq!(b.top_ask().unwrap().price_paise, 10100);
        assert_eq!(b.last_seq, 200);
    }

    #[test]
    fn duplicate_seq_is_rejected() {
        let mut b = LiveBook::new();
        let first = book_with(50, &[lvl(10000, 1)], &[lvl(10100, 1)]);
        let dup = book_with(50, &[lvl(10500, 1)], &[lvl(10600, 1)]);
        assert!(b.apply(&first));
        assert!(!b.apply(&dup));
        assert_eq!(b.top_bid().unwrap().price_paise, 10000);
    }

    #[test]
    fn first_apply_accepts_any_ts() {
        // Even a ts of 0 is fine on the very first apply because last_seq
        // is also 0 and we treat the initial state as "no prior update".
        let mut b = LiveBook::new();
        let book = book_with(0, &[lvl(1, 1)], &[lvl(2, 1)]);
        assert!(b.apply(&book));
        assert_eq!(b.last_seq, 0);
    }

    #[test]
    fn apply_truncates_oversized_payload_to_max_levels() {
        let mut b = LiveBook::new();
        // 25 levels per side — over the 20 cap.
        let bids: Vec<BookLevel> = (0..25).map(|i| lvl(10000 - i, 1)).collect();
        let asks: Vec<BookLevel> = (0..25).map(|i| lvl(10100 + i, 1)).collect();
        let book = book_with(1, &bids, &asks);
        assert!(b.apply(&book));
        assert_eq!(b.bid_levels().len(), MAX_BOOK_LEVELS);
        assert_eq!(b.ask_levels().len(), MAX_BOOK_LEVELS);
    }

    #[test]
    fn clear_resets_state_and_last_seq() {
        let mut b = LiveBook::new();
        let book = book_with(99, &[lvl(10000, 50)], &[lvl(10100, 40)]);
        b.apply(&book);
        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.last_seq, 0);
    }

    #[test]
    fn top_bid_px_and_top_ask_px_round_trip_through_px() {
        let mut b = LiveBook::new();
        let book = book_with(1, &[lvl(15050, 10)], &[lvl(15075, 12)]);
        b.apply(&book);
        assert_eq!(b.top_bid_px(), Some(Px::from_paise(15050)));
        assert_eq!(b.top_ask_px(), Some(Px::from_paise(15075)));
    }

    #[test]
    fn second_apply_replaces_first_in_place() {
        // Property: applying a new book wipes the previous side contents.
        let mut b = LiveBook::new();
        let first = book_with(1, &[lvl(100, 1), lvl(99, 1)], &[lvl(101, 1)]);
        let second = book_with(2, &[lvl(110, 5)], &[lvl(120, 5), lvl(121, 5), lvl(122, 5)]);
        b.apply(&first);
        b.apply(&second);
        assert_eq!(b.bid_levels().len(), 1);
        assert_eq!(b.ask_levels().len(), 3);
        assert_eq!(b.top_bid().unwrap().price_paise, 110);
        assert_eq!(b.top_ask().unwrap().price_paise, 120);
    }
}
