//! Orderflow_Engine orchestrator.
//!
//! Wires the live book, detector, metrics computer, and heatmap publisher
//! into one per-symbol state machine. The engine subscribes to
//! `md.book.<sym>` and `md.tick.<sym>`, computes [`OrderflowSnapshot`]s
//! incrementally, and publishes `of.event.<sym>` (one JSON message per
//! emitted [`OrderflowEvent`]) and `of.heatmap.<sym>` (one JSON message per
//! refreshed snapshot).
//!
//! ### State storage
//!
//! Per-symbol state lives in a `parking_lot::Mutex<HashMap<SymbolId,
//! OrderflowState>>`. The hashmap is populated lazily on first observation
//! of each symbol; once populated, in-place updates do not allocate (the
//! map's bucket already exists). Lock contention is minimal because each
//! symbol's task path holds the mutex only long enough to swap state in
//! and out.
//!
//! ### No-allocation discipline (R2.6)
//!
//! [`OrderflowEngine::ingest_book`] is the steady-state Hot_Path entry
//! point. After the per-symbol [`OrderflowState`] has been instantiated
//! once, subsequent calls allocate **zero** bytes — verified by the
//! `assert_no_alloc` test below (gated behind the `alloc-tracking`
//! feature so release binaries pay nothing).

use std::collections::HashMap;
use std::sync::Arc;

use hedge_core::{BoundedEvents, SymbolId};
use hedge_schemas::{OrderBook, Tick};
use parking_lot::Mutex;

use crate::book::LiveBook;
use crate::events::{Detector, OrderflowEvent};
use crate::heatmap::{HeatmapSnapshot, OrderflowHeatmap};
use crate::metrics::{
    bid_ask_imbalance, liquidity_pressure, OrderflowSnapshot, RollingDelta,
    DEFAULT_ROLLING_DELTA_WINDOW_NS,
};
use crate::war_mode::WarModeProfile;

/// Per-symbol state owned by the engine.
pub struct OrderflowState {
    /// Symbol this state belongs to.
    pub symbol: SymbolId,
    /// Live book mirror.
    pub book: LiveBook,
    /// Stateful detector.
    pub detector: Detector,
    /// Rolling delta window.
    pub delta: RollingDelta,
    /// Heatmap publisher (`tokio::sync::watch::Sender`).
    pub heatmap: OrderflowHeatmap,
}

impl OrderflowState {
    /// Construct a fresh state for `symbol`.
    pub fn new(symbol: SymbolId) -> Self {
        Self::with_window(symbol, DEFAULT_ROLLING_DELTA_WINDOW_NS)
    }

    /// Construct a state with an explicit rolling-delta window.
    pub fn with_window(symbol: SymbolId, rolling_delta_window_ns: u64) -> Self {
        Self {
            symbol,
            book: LiveBook::new(),
            detector: Detector::new(),
            delta: RollingDelta::with_window(rolling_delta_window_ns),
            heatmap: OrderflowHeatmap::new(symbol),
        }
    }

    /// Subscribe to heatmap updates. The returned receiver always observes
    /// the latest snapshot.
    pub fn subscribe_heatmap(&self) -> tokio::sync::watch::Receiver<HeatmapSnapshot> {
        self.heatmap.subscribe()
    }

    /// Apply a fresh `OrderBook_v1` payload, refreshing book state, running
    /// detectors, refreshing the heatmap, and producing a snapshot.
    ///
    /// Returns `Some(snapshot)` when the update was applied and `None` when
    /// the book was rejected (out-of-order ts_ns, see [`LiveBook::apply`]).
    ///
    /// **Allocation discipline**: in steady state every mutation lands in
    /// pre-existing inline storage; the only allocation that ever occurs in
    /// this function is the optional `BoundedEvents` clone happening implicitly
    /// when the snapshot's events vec is constructed. We hand the snapshot's
    /// events buffer in by reference into the detectors so the caller
    /// composes events into the same buffer the snapshot returns — no copy.
    pub fn ingest_book(&mut self, book: &OrderBook, now_ns: u64) -> Option<OrderflowSnapshot> {
        if !self.book.apply(book) {
            return None;
        }

        // Refresh heatmap.
        self.heatmap.publish_from_book(&self.book, now_ns);

        let mut events: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        let _emitted = self.detector.observe_book(&self.book, now_ns, &mut events);

        let snap = OrderflowSnapshot {
            symbol: self.symbol,
            bid_ask_imbalance: bid_ask_imbalance(&self.book),
            aggressive_buyer_volume: self.delta.buyer_volume(now_ns),
            aggressive_seller_volume: self.delta.seller_volume(now_ns),
            rolling_delta: self.delta.signed_delta(now_ns),
            liquidity_pressure: liquidity_pressure(&self.book),
            events,
        };
        Some(snap)
    }

    /// Apply a fresh `Tick_v1` payload, classifying the trade against the
    /// current top of book and updating the rolling delta. Returns the
    /// detector-emitted absorption / hidden-liquidity events, if any.
    ///
    /// `now_ns` is the engine's monotonic timestamp. We use the tick's own
    /// `ts_recv_ns` field would be acceptable too; we accept an explicit
    /// argument so the caller can stamp the orderflow-stage timestamp
    /// without depending on the upstream Market_Data_Engine clock.
    pub fn ingest_tick(&mut self, tick: &Tick, now_ns: u64) -> OrderflowSnapshot {
        // Aggressor classification on the rolling delta side. The detector
        // performs the absorption / hidden-liquidity pattern match and we
        // reuse its result for the snapshot's events.
        let mut buyer_v = 0u64;
        let mut seller_v = 0u64;
        if let Some(ask) = self.book.top_ask() {
            if tick.ltp_paise >= ask.price_paise {
                buyer_v = tick.ltq;
            }
        }
        if let Some(bid) = self.book.top_bid() {
            if tick.ltp_paise <= bid.price_paise {
                seller_v = tick.ltq;
            }
        }
        if buyer_v != 0 || seller_v != 0 {
            self.delta.record(now_ns, buyer_v, seller_v);
        }

        let mut events: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        let _emitted = self.detector.observe_trade(tick, &self.book, &mut events);

        OrderflowSnapshot {
            symbol: self.symbol,
            bid_ask_imbalance: bid_ask_imbalance(&self.book),
            aggressive_buyer_volume: self.delta.buyer_volume(now_ns),
            aggressive_seller_volume: self.delta.seller_volume(now_ns),
            rolling_delta: self.delta.signed_delta(now_ns),
            liquidity_pressure: liquidity_pressure(&self.book),
            events,
        }
    }
}

/// Engine handle. Cloneable — the inner state is shared via `Arc<Mutex<…>>`.
#[derive(Clone)]
pub struct OrderflowEngine {
    inner: Arc<Mutex<HashMap<SymbolId, OrderflowState>>>,
    rolling_delta_window_ns: u64,
    /// Runtime War_Mode profile. Updated by the engine binary's
    /// `ops.warmode.*` subscriber and read on the per-event hot loop
    /// to scale detector sensitivity (R26.2).
    war_mode: Arc<WarModeProfile>,
}

impl Default for OrderflowEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderflowEngine {
    /// Construct a fresh engine using the default rolling-delta window.
    pub fn new() -> Self {
        Self::with_window(DEFAULT_ROLLING_DELTA_WINDOW_NS)
    }

    /// Construct a fresh engine with an explicit rolling-delta window.
    pub fn with_window(rolling_delta_window_ns: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            rolling_delta_window_ns,
            war_mode: Arc::new(WarModeProfile::inactive()),
        }
    }

    /// Pre-populate state for a symbol so the first ingest call does not
    /// allocate a hashmap bucket. Hot_Path binaries call this for every
    /// tracked symbol at startup.
    pub fn ensure_symbol(&self, symbol: SymbolId) {
        let mut guard = self.inner.lock();
        guard
            .entry(symbol)
            .or_insert_with(|| OrderflowState::with_window(symbol, self.rolling_delta_window_ns));
    }

    /// Subscribe to heatmap updates for `symbol`. Returns `None` if the
    /// symbol has never been observed.
    pub fn subscribe_heatmap(
        &self,
        symbol: SymbolId,
    ) -> Option<tokio::sync::watch::Receiver<HeatmapSnapshot>> {
        let guard = self.inner.lock();
        guard.get(&symbol).map(|s| s.subscribe_heatmap())
    }

    /// Apply a book update and return the resulting snapshot.
    ///
    /// On the first call for a symbol the hashmap allocates a bucket; on
    /// subsequent calls every mutation is in-place and no heap allocation
    /// occurs.
    pub fn ingest_book(&self, book: &OrderBook, now_ns: u64) -> Option<OrderflowSnapshot> {
        let symbol = SymbolId::new(book.symbol);
        let mut guard = self.inner.lock();
        let state = guard
            .entry(symbol)
            .or_insert_with(|| OrderflowState::with_window(symbol, self.rolling_delta_window_ns));
        state.ingest_book(book, now_ns)
    }

    /// Apply a tick update and return the resulting snapshot.
    pub fn ingest_tick(&self, tick: &Tick, now_ns: u64) -> OrderflowSnapshot {
        let symbol = SymbolId::new(tick.symbol);
        let mut guard = self.inner.lock();
        let state = guard
            .entry(symbol)
            .or_insert_with(|| OrderflowState::with_window(symbol, self.rolling_delta_window_ns));
        state.ingest_tick(tick, now_ns)
    }

    /// Number of symbols currently tracked. Useful for tests and metrics.
    pub fn tracked_symbol_count(&self) -> usize {
        self.inner.lock().len()
    }

    /// Shared handle to the runtime War_Mode profile. The engine binary
    /// drives the [`WarModeProfile`] from its `ops.warmode.*` subscriber
    /// (`hedge-session::WarModeController` is the producer); detectors
    /// and the public liquidity-event filter read the profile on each
    /// event to scale sensitivity per R26.2.
    #[inline]
    pub fn war_mode(&self) -> &Arc<WarModeProfile> {
        &self.war_mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_core::Side;
    use hedge_schemas::BookLevel;

    fn lvl(price_paise: i64, qty: u64) -> BookLevel {
        BookLevel {
            price_paise,
            qty,
            orders: 1,
        }
    }

    fn make_book(symbol: u32, ts_ns: u64, bids: &[BookLevel], asks: &[BookLevel]) -> OrderBook {
        OrderBook {
            correlation_id: [0u8; 16],
            symbol,
            exchange: 0,
            bid_levels: bids.to_vec(),
            ask_levels: asks.to_vec(),
            ts_ns,
        }
    }

    fn make_tick(symbol: u32, price_paise: i64, qty: u64) -> Tick {
        Tick {
            correlation_id: [0u8; 16],
            symbol,
            exchange: 0,
            ltp_paise: price_paise,
            bid_paise: 0,
            ask_paise: 0,
            ltq: qty,
            total_buy_qty: 0,
            total_sell_qty: 0,
            ts_exchange_ns: 0,
            ts_recv_ns: 0,
        }
    }

    #[test]
    fn snapshot_imbalance_tracks_top_of_book() {
        let eng = OrderflowEngine::new();
        let book = make_book(1, 1, &[lvl(100, 30)], &[lvl(101, 10)]);
        let snap = eng.ingest_book(&book, 1).unwrap();
        // (30 - 10)/40 = 0.5
        assert!((snap.bid_ask_imbalance - 0.5).abs() < 1e-6);
        assert!(snap.liquidity_pressure >= -1.0 && snap.liquidity_pressure <= 1.0);
        assert_eq!(snap.aggressive_buyer_volume, 0);
        assert_eq!(snap.aggressive_seller_volume, 0);
        assert_eq!(snap.rolling_delta, 0);
    }

    #[test]
    fn rolling_delta_accumulates_through_aggressive_trades() {
        let eng = OrderflowEngine::new();
        let book = make_book(1, 1, &[lvl(100, 5)], &[lvl(101, 5)]);
        eng.ingest_book(&book, 1);

        // Aggressive buyer (price >= ask) trades 10.
        let tick_buy = make_tick(1, 101, 10);
        let s1 = eng.ingest_tick(&tick_buy, 100);
        assert_eq!(s1.aggressive_buyer_volume, 10);
        assert_eq!(s1.aggressive_seller_volume, 0);
        assert_eq!(s1.rolling_delta, 10);

        // Aggressive seller (price <= bid) trades 4.
        let tick_sell = make_tick(1, 100, 4);
        let s2 = eng.ingest_tick(&tick_sell, 200);
        assert_eq!(s2.aggressive_buyer_volume, 10);
        assert_eq!(s2.aggressive_seller_volume, 4);
        assert_eq!(s2.rolling_delta, 6);
    }

    #[test]
    fn snapshot_includes_liquidity_gap_event() {
        // Tick size 1 paise; bid side 5-paise gap -> event.
        let eng = OrderflowEngine::new();
        let book = make_book(
            1,
            1,
            &[lvl(10000, 10), lvl(9994, 5)],
            &[lvl(10100, 10), lvl(10101, 5)],
        );
        let snap = eng.ingest_book(&book, 1).unwrap();
        assert_eq!(snap.events.len(), 1);
        match snap.events.as_slice()[0] {
            OrderflowEvent::LiquidityGap { side, .. } => assert_eq!(side, Side::Buy),
            ref other => panic!("expected LiquidityGap, got {:?}", other),
        }
    }

    #[test]
    fn out_of_order_book_returns_none() {
        let eng = OrderflowEngine::new();
        let first = make_book(1, 100, &[lvl(100, 5)], &[lvl(101, 5)]);
        let stale = make_book(1, 50, &[lvl(99, 5)], &[lvl(102, 5)]);
        assert!(eng.ingest_book(&first, 100).is_some());
        assert!(eng.ingest_book(&stale, 50).is_none());
    }

    #[test]
    fn tracked_symbol_count_grows_with_distinct_symbols() {
        let eng = OrderflowEngine::new();
        eng.ensure_symbol(SymbolId::new(1));
        eng.ensure_symbol(SymbolId::new(2));
        // Re-ensuring is idempotent.
        eng.ensure_symbol(SymbolId::new(1));
        assert_eq!(eng.tracked_symbol_count(), 2);
    }

    #[test]
    fn engine_war_mode_handle_starts_inactive_and_can_be_activated() {
        // Verifies the engine surfaces a shared `WarModeProfile` handle
        // that the binary's `ops.warmode.*` subscriber drives. R26.2.
        use crate::war_mode::NORMAL_SCAN_MULTIPLIER as NSM;
        let eng = OrderflowEngine::new();
        let wm = Arc::clone(eng.war_mode());
        assert!(!wm.is_active());
        wm.activate(2.0, 0.6);
        assert!(eng.war_mode().is_active());
        assert_eq!(eng.war_mode().scan_multiplier(), 2.0);
        assert_eq!(eng.war_mode().sensitivity_factor(), 2.0);
        wm.deactivate();
        assert!(!eng.war_mode().is_active());
        assert_eq!(eng.war_mode().sensitivity_factor(), NSM);
    }

    #[test]
    fn liquidity_pressure_in_unit_range_for_arbitrary_book() {
        let eng = OrderflowEngine::new();
        let book = make_book(
            1,
            1,
            &[lvl(100, 7), lvl(99, 3), lvl(98, 5)],
            &[lvl(101, 11), lvl(102, 1), lvl(103, 9)],
        );
        let snap = eng.ingest_book(&book, 1).unwrap();
        assert!(snap.liquidity_pressure >= -1.0 && snap.liquidity_pressure <= 1.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heatmap_subscriber_observes_book_updates() {
        let eng = OrderflowEngine::new();
        eng.ensure_symbol(SymbolId::new(7));
        let mut rx = eng.subscribe_heatmap(SymbolId::new(7)).expect("subscribed");
        let book = make_book(7, 1, &[lvl(100, 5)], &[lvl(101, 4)]);
        eng.ingest_book(&book, 99);
        rx.changed().await.expect("watch alive");
        let snap = rx.borrow_and_update().clone();
        assert_eq!(snap.symbol, SymbolId::new(7));
        assert_eq!(snap.ts_ns, 99);
        assert_eq!(snap.rows[0].bid_qty, 5);
        assert_eq!(snap.rows[0].ask_qty, 4);
    }

    /// Liquidity pressure is in `[-1, 1]` for any random book — proptest
    /// generalisation of the unit case above.
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn liquidity_pressure_within_unit_range(
            bid_qtys in proptest::collection::vec(1u64..1_000_000u64, 0..10),
            ask_qtys in proptest::collection::vec(1u64..1_000_000u64, 0..10),
        ) {
            let bids: Vec<_> = bid_qtys
                .iter()
                .enumerate()
                .map(|(i, q)| BookLevel { price_paise: 10000 - i as i64, qty: *q, orders: 1 })
                .collect();
            let asks: Vec<_> = ask_qtys
                .iter()
                .enumerate()
                .map(|(i, q)| BookLevel { price_paise: 10100 + i as i64, qty: *q, orders: 1 })
                .collect();
            let eng = OrderflowEngine::new();
            let book = make_book(1, 1, &bids, &asks);
            let snap = eng.ingest_book(&book, 1).unwrap();
            prop_assert!(snap.liquidity_pressure >= -1.0);
            prop_assert!(snap.liquidity_pressure <= 1.0);
            prop_assert!(snap.bid_ask_imbalance >= -1.0);
            prop_assert!(snap.bid_ask_imbalance <= 1.0);
        }
    }

    /// Steady-state book ingest is allocation-free once the per-symbol
    /// state has been pre-populated. Run with
    /// `cargo test -p hedge-orderflow --features alloc-tracking`.
    #[cfg(feature = "alloc-tracking")]
    #[test]
    fn steady_state_book_ingest_does_not_allocate() {
        let eng = OrderflowEngine::new();
        eng.ensure_symbol(SymbolId::new(1));
        // First call inserts into the hashmap — that is allowed to allocate.
        let warmup = make_book(1, 1, &[lvl(100, 5)], &[lvl(101, 5)]);
        eng.ingest_book(&warmup, 1);

        // Second call must be allocation-free.
        let book = make_book(1, 2, &[lvl(101, 6)], &[lvl(102, 4)]);
        hedge_core::alloc_harness::assert_no_alloc("orderflow steady-state book", || {
            let _ = eng.ingest_book(&book, 2);
        });
    }
}
