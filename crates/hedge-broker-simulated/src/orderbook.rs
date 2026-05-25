//! In-memory orderbook used by [`SimulatedBroker`](crate::SimulatedBroker)
//! to derive synthetic fills.
//!
//! The orderbook is a static snapshot of two sorted sides:
//!
//! * `bids` — buyers, sorted **descending** by price (best price first).
//! * `asks` — sellers, sorted **ascending** by price (best price first).
//!
//! Each level pairs a price (paise) with a quantity. The book is populated
//! by callers either programmatically (tests) or from a recorded ticker
//! file (replay). `SimulatedBroker::submit` consumes liquidity off the
//! relevant side until the order intent's quantity is satisfied or the
//! book runs out.
//!
//! ### Determinism
//!
//! All operations are deterministic in the order they are issued. The
//! implementation uses no clocks, no randomness, and no global state, so a
//! given sequence of `submit / modify / cancel / status` calls against a
//! given starting book always produces the same sequence of fills and FSM
//! transitions. This is the contract Property **12 (Replay Determinism)**
//! relies on.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One level of the limit-order book.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookLevel {
    /// Price in paise.
    pub price_paise: i64,
    /// Quantity available at this price.
    pub qty: u64,
}

impl BookLevel {
    /// Construct a level.
    #[inline]
    pub const fn new(price_paise: i64, qty: u64) -> Self {
        Self { price_paise, qty }
    }
}

/// In-memory orderbook for a single symbol. Independent per-symbol books
/// are held by `SimulatedBroker` in a `DashMap`.
#[derive(Clone, Debug, Default)]
pub struct OrderBook {
    /// Bids keyed by negated price for descending iteration. Using a
    /// `BTreeMap` keeps insertion / lookup at O(log n) and produces a
    /// deterministic iteration order.
    bids: BTreeMap<i64, u64>,
    /// Asks keyed by price (ascending).
    asks: BTreeMap<i64, u64>,
}

impl OrderBook {
    /// New empty book.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a book from two slices. Levels are merged: duplicate
    /// prices have their quantities summed. This is convenient for tests
    /// and for ingest paths that may emit the same level twice.
    pub fn from_levels(bids: &[BookLevel], asks: &[BookLevel]) -> Self {
        let mut book = Self::new();
        for lvl in bids {
            book.add_bid(lvl.price_paise, lvl.qty);
        }
        for lvl in asks {
            book.add_ask(lvl.price_paise, lvl.qty);
        }
        book
    }

    /// Add (or top up) a bid level.
    pub fn add_bid(&mut self, price_paise: i64, qty: u64) {
        if qty == 0 {
            return;
        }
        // Negate the key so iteration in natural ascending key order
        // produces descending price order.
        let entry = self.bids.entry(-price_paise).or_insert(0);
        *entry = entry.saturating_add(qty);
    }

    /// Add (or top up) an ask level.
    pub fn add_ask(&mut self, price_paise: i64, qty: u64) {
        if qty == 0 {
            return;
        }
        let entry = self.asks.entry(price_paise).or_insert(0);
        *entry = entry.saturating_add(qty);
    }

    /// Best (highest) bid price; `None` if the book is empty on the bid
    /// side.
    pub fn best_bid(&self) -> Option<BookLevel> {
        self.bids
            .iter()
            .next()
            .map(|(neg_price, qty)| BookLevel::new(-*neg_price, *qty))
    }

    /// Best (lowest) ask price; `None` if the book is empty on the ask
    /// side.
    pub fn best_ask(&self) -> Option<BookLevel> {
        self.asks
            .iter()
            .next()
            .map(|(price, qty)| BookLevel::new(*price, *qty))
    }

    /// Total quantity available across all bid levels.
    pub fn total_bid_qty(&self) -> u64 {
        self.bids.values().copied().fold(0u64, u64::saturating_add)
    }

    /// Total quantity available across all ask levels.
    pub fn total_ask_qty(&self) -> u64 {
        self.asks.values().copied().fold(0u64, u64::saturating_add)
    }

    /// Consume up to `requested_qty` units from the **ask** side at or
    /// below `limit_paise`. Returns the synthetic fills produced and
    /// mutates the book in place.
    ///
    /// `limit_paise` of `None` matches a market order: the entire book
    /// is eligible.
    ///
    /// Fills are emitted level by level in **price-time order** (best
    /// price first, deterministic) so the average fill price is
    /// reproducible.
    pub fn consume_asks(
        &mut self,
        requested_qty: u64,
        limit_paise: Option<i64>,
    ) -> Vec<BookLevel> {
        let mut remaining = requested_qty;
        let mut fills: Vec<BookLevel> = Vec::new();
        // Collect prices in ascending order; we cannot mutate while iterating.
        let prices: Vec<i64> = self.asks.keys().copied().collect();
        for price in prices {
            if remaining == 0 {
                break;
            }
            if let Some(limit) = limit_paise {
                if price > limit {
                    break;
                }
            }
            let avail = self.asks.get(&price).copied().unwrap_or(0);
            if avail == 0 {
                continue;
            }
            let take = avail.min(remaining);
            fills.push(BookLevel::new(price, take));
            remaining -= take;
            let new_avail = avail - take;
            if new_avail == 0 {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, new_avail);
            }
        }
        fills
    }

    /// Consume up to `requested_qty` units from the **bid** side at or
    /// above `limit_paise`. Mirrors [`consume_asks`](Self::consume_asks)
    /// for sell orders.
    pub fn consume_bids(
        &mut self,
        requested_qty: u64,
        limit_paise: Option<i64>,
    ) -> Vec<BookLevel> {
        let mut remaining = requested_qty;
        let mut fills: Vec<BookLevel> = Vec::new();
        // Collect bid prices in descending order (best first).
        let neg_prices: Vec<i64> = self.bids.keys().copied().collect();
        for neg_price in neg_prices {
            if remaining == 0 {
                break;
            }
            let price = -neg_price;
            if let Some(limit) = limit_paise {
                if price < limit {
                    break;
                }
            }
            let avail = self.bids.get(&neg_price).copied().unwrap_or(0);
            if avail == 0 {
                continue;
            }
            let take = avail.min(remaining);
            fills.push(BookLevel::new(price, take));
            remaining -= take;
            let new_avail = avail - take;
            if new_avail == 0 {
                self.bids.remove(&neg_price);
            } else {
                self.bids.insert(neg_price, new_avail);
            }
        }
        fills
    }

    /// Reset the book to empty.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

/// Convenience: compute the volume-weighted average price of a slice of
/// fills. Returns `(total_qty, vwap_paise)`. `vwap_paise` is `0` when
/// `total_qty` is `0`.
pub fn vwap_paise(fills: &[BookLevel]) -> (u64, i64) {
    let mut total_qty: u128 = 0;
    let mut total_notional: i128 = 0;
    for f in fills {
        total_qty = total_qty.saturating_add(f.qty as u128);
        total_notional =
            total_notional.saturating_add((f.price_paise as i128).saturating_mul(f.qty as i128));
    }
    if total_qty == 0 {
        return (0, 0);
    }
    let vwap = total_notional / total_qty as i128;
    let vwap_clamped = if vwap > i64::MAX as i128 {
        i64::MAX
    } else if vwap < i64::MIN as i128 {
        i64::MIN
    } else {
        vwap as i64
    };
    let qty_clamped = if total_qty > u64::MAX as u128 {
        u64::MAX
    } else {
        total_qty as u64
    };
    (qty_clamped, vwap_clamped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bids_iterate_descending_by_price() {
        let book = OrderBook::from_levels(
            &[
                BookLevel::new(100_00, 5),
                BookLevel::new(102_00, 7),
                BookLevel::new(101_00, 3),
            ],
            &[],
        );
        assert_eq!(book.best_bid(), Some(BookLevel::new(102_00, 7)));
    }

    #[test]
    fn asks_iterate_ascending_by_price() {
        let book = OrderBook::from_levels(
            &[],
            &[
                BookLevel::new(100_00, 5),
                BookLevel::new(98_00, 4),
                BookLevel::new(99_00, 3),
            ],
        );
        assert_eq!(book.best_ask(), Some(BookLevel::new(98_00, 4)));
    }

    #[test]
    fn consume_asks_market_order_walks_book() {
        let mut book = OrderBook::from_levels(
            &[],
            &[
                BookLevel::new(100_00, 5),
                BookLevel::new(101_00, 4),
                BookLevel::new(102_00, 3),
            ],
        );
        let fills = book.consume_asks(8, None);
        assert_eq!(
            fills,
            vec![BookLevel::new(100_00, 5), BookLevel::new(101_00, 3)]
        );
        // 1 unit remaining at 101_00, plus 3 at 102_00.
        assert_eq!(book.best_ask(), Some(BookLevel::new(101_00, 1)));
        assert_eq!(book.total_ask_qty(), 4);
    }

    #[test]
    fn consume_asks_limit_order_respects_limit() {
        let mut book = OrderBook::from_levels(
            &[],
            &[
                BookLevel::new(100_00, 5),
                BookLevel::new(101_00, 4),
                BookLevel::new(102_00, 3),
            ],
        );
        let fills = book.consume_asks(20, Some(101_00));
        // 5 at 100_00 and 4 at 101_00; 102_00 is above limit.
        assert_eq!(
            fills,
            vec![BookLevel::new(100_00, 5), BookLevel::new(101_00, 4)]
        );
        assert_eq!(book.total_ask_qty(), 3);
        // Remaining qty 11 unfilled — caller decides FSM state.
    }

    #[test]
    fn consume_asks_partial_fill_when_book_runs_out() {
        let mut book = OrderBook::from_levels(&[], &[BookLevel::new(100_00, 3)]);
        let fills = book.consume_asks(10, None);
        assert_eq!(fills, vec![BookLevel::new(100_00, 3)]);
        assert_eq!(book.total_ask_qty(), 0);
    }

    #[test]
    fn consume_asks_empty_book_returns_no_fills() {
        let mut book = OrderBook::new();
        let fills = book.consume_asks(10, None);
        assert!(fills.is_empty());
    }

    #[test]
    fn consume_bids_walks_book_descending() {
        let mut book = OrderBook::from_levels(
            &[
                BookLevel::new(100_00, 5),
                BookLevel::new(99_00, 4),
                BookLevel::new(98_00, 3),
            ],
            &[],
        );
        let fills = book.consume_bids(7, None);
        assert_eq!(
            fills,
            vec![BookLevel::new(100_00, 5), BookLevel::new(99_00, 2)]
        );
    }

    #[test]
    fn consume_bids_limit_order_respects_limit() {
        let mut book = OrderBook::from_levels(
            &[
                BookLevel::new(100_00, 5),
                BookLevel::new(99_00, 4),
                BookLevel::new(98_00, 3),
            ],
            &[],
        );
        let fills = book.consume_bids(20, Some(99_00));
        // 5 at 100_00 + 4 at 99_00; 98_00 below limit.
        assert_eq!(
            fills,
            vec![BookLevel::new(100_00, 5), BookLevel::new(99_00, 4)]
        );
        assert_eq!(book.total_bid_qty(), 3);
    }

    #[test]
    fn vwap_paise_computes_correctly() {
        let fills = vec![BookLevel::new(100_00, 3), BookLevel::new(102_00, 2)];
        let (qty, vwap) = vwap_paise(&fills);
        assert_eq!(qty, 5);
        // (100_00*3 + 102_00*2) / 5 = (30000 + 20400)/5 = 50400/5 = 10080
        assert_eq!(vwap, 10080);
    }

    #[test]
    fn vwap_paise_handles_empty() {
        let (qty, vwap) = vwap_paise(&[]);
        assert_eq!(qty, 0);
        assert_eq!(vwap, 0);
    }

    #[test]
    fn add_zero_qty_is_noop() {
        let mut book = OrderBook::new();
        book.add_bid(100_00, 0);
        book.add_ask(100_00, 0);
        assert!(book.best_bid().is_none());
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn duplicate_levels_sum_quantities() {
        let mut book = OrderBook::new();
        book.add_bid(100_00, 5);
        book.add_bid(100_00, 7);
        assert_eq!(book.best_bid(), Some(BookLevel::new(100_00, 12)));
    }

    #[test]
    fn deterministic_replay_same_sequence_same_outcome() {
        // Property 12: replay determinism — identical input sequences produce
        // identical fill outputs and identical end-state books.
        let make_book = || {
            OrderBook::from_levels(
                &[BookLevel::new(99_00, 4), BookLevel::new(98_00, 6)],
                &[BookLevel::new(101_00, 3), BookLevel::new(102_00, 5)],
            )
        };
        let mut a = make_book();
        let mut b = make_book();

        let fa1 = a.consume_asks(2, None);
        let fa2 = a.consume_asks(4, Some(102_00));
        let fb1 = b.consume_asks(2, None);
        let fb2 = b.consume_asks(4, Some(102_00));

        assert_eq!(fa1, fb1);
        assert_eq!(fa2, fb2);
        assert_eq!(a.total_ask_qty(), b.total_ask_qty());
        assert_eq!(a.best_ask(), b.best_ask());
    }
}
