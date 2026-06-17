//! Property-based tests for `hedge-orderflow` metrics (task 11.2).
//!
//! Validates:
//!   - liquidity_pressure ∈ [-1, 1] for any book configuration
//!   - bid_ask_imbalance ∈ [-1, 1] for any book configuration
//!   - Both return 0.0 for empty book
//!   - RollingDelta: signed_delta = buyer_volume - seller_volume
//!   - RollingDelta: old buckets are dropped after window expires
//!   - OrderflowSnapshot::empty has zero defaults

use hedge_core::SymbolId;
use hedge_orderflow::{
    bid_ask_imbalance, liquidity_pressure, LiveBook, RollingDelta, OrderflowSnapshot,
};
use hedge_schemas::{BookLevel, OrderBook};
use proptest::prelude::*;

fn arb_book_level() -> impl Strategy<Value = BookLevel> {
    (0i64..1_000_000, 0u64..10_000).prop_map(|(price_paise, qty)| BookLevel {
        price_paise,
        qty,
        orders: 1,
    })
}

fn build_book(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> LiveBook {
    let mut book = LiveBook::new();
    book.apply(&OrderBook {
        correlation_id: [0u8; 16],
        symbol: 1,
        exchange: 0,
        bid_levels: bids,
        ask_levels: asks,
        ts_ns: 1,
    });
    book
}

proptest! {
    #[test]
    fn bid_ask_imbalance_bounded(
        bids in prop::collection::vec(arb_book_level(), 0..20),
        asks in prop::collection::vec(arb_book_level(), 0..20),
    ) {
        let book = build_book(bids, asks);
        let v = bid_ask_imbalance(&book);
        prop_assert!(v >= -1.0 && v <= 1.0, "bid_ask_imbalance={} out of range", v);
    }

    #[test]
    fn liquidity_pressure_bounded(
        bids in prop::collection::vec(arb_book_level(), 0..20),
        asks in prop::collection::vec(arb_book_level(), 0..20),
    ) {
        let book = build_book(bids, asks);
        let v = liquidity_pressure(&book);
        prop_assert!(v >= -1.0 && v <= 1.0, "liquidity_pressure={} out of range", v);
    }

    #[test]
    fn only_bids_gives_plus_one(qty in 1u64..10_000) {
        let book = build_book(
            vec![BookLevel { price_paise: 100, qty, orders: 1 }],
            vec![],
        );
        prop_assert_eq!(bid_ask_imbalance(&book), 1.0);
    }

    #[test]
    fn only_asks_gives_minus_one(qty in 1u64..10_000) {
        let book = build_book(
            vec![],
            vec![BookLevel { price_paise: 101, qty, orders: 1 }],
        );
        prop_assert_eq!(bid_ask_imbalance(&book), -1.0);
    }

    #[test]
    fn rolling_delta_signed_equals_buyer_minus_seller(
        records in prop::collection::vec((0u64..10_000, 0u64..10_000u64), 0..50),
    ) {
        let mut rd = RollingDelta::with_window(30_000_000_000);
        for (i, (buy, sell)) in records.iter().enumerate() {
            rd.record((i as u64) * 1_000_000_000, *buy, *sell);
        }
        let now = (records.len() as u64).saturating_sub(1) * 1_000_000_000;
        let buyer = rd.buyer_volume(now);
        let seller = rd.seller_volume(now);
        let delta = rd.signed_delta(now);
        let expected = (buyer as i128) - (seller as i128);
        let clamped = expected.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        prop_assert_eq!(delta, clamped);
    }

    #[test]
    fn rolling_delta_drops_old_buckets(
        old_vol in 1u64..100_000,
        new_vol in 1u64..100_000,
    ) {
        let window_ns = 1_000_000_000u64;
        let mut rd = RollingDelta::with_window(window_ns);
        rd.record(0, old_vol, 0);
        let later = window_ns * 3;
        rd.record(later, new_vol, 0);
        let buyer = rd.buyer_volume(later);
        prop_assert_eq!(buyer, new_vol, "old volume should be dropped");
    }
}

// ---- Deterministic tests -------------------------------------------------

#[test]
fn empty_book_returns_zero() {
    let book = build_book(vec![], vec![]);
    assert_eq!(bid_ask_imbalance(&book), 0.0);
    assert_eq!(liquidity_pressure(&book), 0.0);
}

#[test]
fn empty_snapshot_defaults() {
    let s = OrderflowSnapshot::empty(SymbolId::new(42));
    assert_eq!(s.symbol, SymbolId::new(42));
    assert_eq!(s.bid_ask_imbalance, 0.0);
    assert_eq!(s.liquidity_pressure, 0.0);
    assert_eq!(s.aggressive_buyer_volume, 0);
    assert_eq!(s.aggressive_seller_volume, 0);
    assert_eq!(s.rolling_delta, 0);
    assert!(s.events.is_empty());
}
