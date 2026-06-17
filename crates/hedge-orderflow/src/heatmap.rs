//! Live orderflow heatmap.
//!
//! [`HeatmapSnapshot`] is a 20-row × 2-column matrix (bid qty, ask qty)
//! mirrored from the live book. The Orderflow_Engine produces a fresh
//! snapshot on every accepted `md.book.<sym>` payload and writes it into a
//! `tokio::sync::watch::Sender<HeatmapSnapshot>`; the UI gateway holds the
//! matching [`watch::Receiver`] and forwards changes to the React cockpit
//! over a WebSocket (R2.4).
//!
//! ### Why `tokio::sync::watch`
//!
//! The heatmap is a *low-frequency*, single-publisher / multi-subscriber
//! signal: we want every subscriber to see the *latest* snapshot, never a
//! backlog. `watch` channels coalesce updates to the most recent value,
//! which is exactly the desired semantics.
//!
//! ### Allocation discipline
//!
//! `HeatmapSnapshot` stores its rows as fixed-size arrays so the steady
//! state path performs no heap allocation (R2.6). The `tokio::sync::watch`
//! channel internally allocates once at construction; subsequent
//! `send_replace` calls reuse the same slot.

use hedge_core::SymbolId;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::book::{LiveBook, MAX_BOOK_LEVELS};

/// One heatmap row corresponding to a depth slot in the orderbook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HeatmapRow {
    /// Bid price expressed in paise. `0` when the slot is empty.
    pub bid_price_paise: i64,
    /// Bid quantity at the slot.
    pub bid_qty: u64,
    /// Ask price expressed in paise. `0` when the slot is empty.
    pub ask_price_paise: i64,
    /// Ask quantity at the slot.
    pub ask_qty: u64,
}

/// Snapshot of the orderbook heatmap for a single symbol.
///
/// Rows are stored in fixed-size depth-N arrays. Slot index `i` always
/// represents "the `i`-th best level on each side" — empty slots are
/// zeroed rather than absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeatmapSnapshot {
    /// Symbol the snapshot describes.
    pub symbol: SymbolId,
    /// Monotonic timestamp (ns since process epoch) at which the snapshot
    /// was produced. Permits the consumer to detect staleness.
    pub ts_ns: u64,
    /// 20 rows of `(bid, ask)` pairs.
    pub rows: [HeatmapRow; MAX_BOOK_LEVELS],
}

impl HeatmapSnapshot {
    /// Construct an empty snapshot (every row zeroed).
    pub fn empty(symbol: SymbolId) -> Self {
        Self {
            symbol,
            ts_ns: 0,
            rows: [HeatmapRow::default(); MAX_BOOK_LEVELS],
        }
    }

    /// Refresh the snapshot from a [`LiveBook`] at `ts_ns`.
    ///
    /// Allocation-free: writes happen into the inline arrays in place.
    pub fn fill_from(&mut self, book: &LiveBook, ts_ns: u64) {
        self.ts_ns = ts_ns;
        for i in 0..MAX_BOOK_LEVELS {
            let bid = book.bid_levels().get(i);
            let ask = book.ask_levels().get(i);
            self.rows[i] = HeatmapRow {
                bid_price_paise: bid.map(|l| l.price_paise).unwrap_or(0),
                bid_qty: bid.map(|l| l.qty).unwrap_or(0),
                ask_price_paise: ask.map(|l| l.price_paise).unwrap_or(0),
                ask_qty: ask.map(|l| l.qty).unwrap_or(0),
            };
        }
    }

    /// Construct a fresh snapshot from a [`LiveBook`] at `ts_ns`. Convenience
    /// wrapper around [`HeatmapSnapshot::empty`] + [`fill_from`].
    pub fn from_book(symbol: SymbolId, book: &LiveBook, ts_ns: u64) -> Self {
        let mut snap = Self::empty(symbol);
        snap.fill_from(book, ts_ns);
        snap
    }

    /// `true` when every row is the default zero row.
    pub fn is_empty(&self) -> bool {
        self.rows.iter().all(|r| *r == HeatmapRow::default())
    }
}

/// Live orderflow heatmap for one symbol, exposed via a
/// `tokio::sync::watch` channel.
///
/// Construction allocates the channel once. Subsequent updates use
/// `send_replace`, which writes the new value into the existing slot
/// without allocating.
pub struct OrderflowHeatmap {
    symbol: SymbolId,
    sender: watch::Sender<HeatmapSnapshot>,
}

impl OrderflowHeatmap {
    /// Create a new heatmap initialised to an empty snapshot for `symbol`.
    pub fn new(symbol: SymbolId) -> Self {
        let (tx, _rx) = watch::channel(HeatmapSnapshot::empty(symbol));
        Self { symbol, sender: tx }
    }

    /// Symbol this heatmap belongs to.
    #[inline]
    pub fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// Subscribe a new receiver. Any number of receivers can be in flight
    /// simultaneously; each independently observes the latest snapshot.
    #[inline]
    pub fn subscribe(&self) -> watch::Receiver<HeatmapSnapshot> {
        self.sender.subscribe()
    }

    /// Retrieve the latest snapshot without allocating a new channel receiver.
    #[inline]
    pub fn get_snapshot(&self) -> HeatmapSnapshot {
        self.sender.borrow().clone()
    }

    /// Number of currently-active subscribers. Useful for UI-gateway
    /// liveness checks; not load-bearing for correctness.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Replace the published snapshot with a fresh one derived from `book`
    /// at `ts_ns`. Performs no heap allocation.
    pub fn publish_from_book(&self, book: &LiveBook, ts_ns: u64) {
        // Build the new snapshot on the stack and hand ownership to the
        // channel. `send_replace` always succeeds — receivers cannot
        // back-pressure a watch channel.
        let snap = HeatmapSnapshot::from_book(self.symbol, book, ts_ns);
        let _ = self.sender.send_replace(snap);
    }

    /// Replace the published snapshot with `snap`. Caller-built variant of
    /// [`publish_from_book`].
    pub fn publish(&self, snap: HeatmapSnapshot) {
        let _ = self.sender.send_replace(snap);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_schemas::{BookLevel, OrderBook};

    fn lvl(p: i64, q: u64) -> BookLevel {
        BookLevel {
            price_paise: p,
            qty: q,
            orders: 1,
        }
    }

    fn book(bids: &[BookLevel], asks: &[BookLevel]) -> LiveBook {
        let mut b = LiveBook::new();
        b.apply(&OrderBook {
            correlation_id: [0u8; 16],
            symbol: 1,
            exchange: 0,
            bid_levels: bids.to_vec(),
            ask_levels: asks.to_vec(),
            ts_ns: 1,
        });
        b
    }

    #[test]
    fn empty_snapshot_has_zero_rows() {
        let s = HeatmapSnapshot::empty(SymbolId::new(1));
        assert!(s.is_empty());
        assert_eq!(s.symbol, SymbolId::new(1));
        assert_eq!(s.ts_ns, 0);
        assert_eq!(s.rows.len(), MAX_BOOK_LEVELS);
    }

    #[test]
    fn fill_from_populates_rows_in_order() {
        let b = book(
            &[lvl(100, 5), lvl(99, 4)],
            &[lvl(101, 3), lvl(102, 2), lvl(103, 1)],
        );
        let s = HeatmapSnapshot::from_book(SymbolId::new(1), &b, 7);
        assert_eq!(s.ts_ns, 7);
        assert_eq!(s.rows[0].bid_price_paise, 100);
        assert_eq!(s.rows[0].bid_qty, 5);
        assert_eq!(s.rows[1].bid_price_paise, 99);
        assert_eq!(s.rows[2].bid_price_paise, 0);
        assert_eq!(s.rows[2].ask_price_paise, 103);
        assert_eq!(s.rows[2].ask_qty, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn watch_subscribers_observe_replaced_snapshot() {
        let h = OrderflowHeatmap::new(SymbolId::new(42));
        let mut rx = h.subscribe();
        let b = book(&[lvl(100, 5)], &[lvl(101, 4)]);
        h.publish_from_book(&b, 99);
        rx.changed().await.expect("channel alive");
        let s = rx.borrow_and_update().clone();
        assert_eq!(s.symbol, SymbolId::new(42));
        assert_eq!(s.ts_ns, 99);
        assert_eq!(s.rows[0].bid_qty, 5);
        assert_eq!(s.rows[0].ask_qty, 4);
    }

    #[test]
    fn receiver_count_tracks_subscribers() {
        let h = OrderflowHeatmap::new(SymbolId::new(1));
        // The internal _rx is dropped at construction so a fresh receiver
        // count starts at zero.
        assert_eq!(h.receiver_count(), 0);
        let _r1 = h.subscribe();
        assert_eq!(h.receiver_count(), 1);
        let _r2 = h.subscribe();
        assert_eq!(h.receiver_count(), 2);
    }
}
