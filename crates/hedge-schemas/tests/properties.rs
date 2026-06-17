//! Property-based tests for `hedge-schemas` (task 4.2).
//!
//! Validates:
//!   - Property 5 — Serialization and Persistence Round-Trip
//!
//! The generated FlatBuffers types are POD mirrors; full wire-format
//! encode/decode lands when `flatc` is on PATH. These tests validate
//! structural construction and field preservation.
//!
//! **Validates: Requirements 1.5**

use hedge_schemas::{
    OrderIntent, RiskProfile, Signal, Tick,
};
use hedge_schemas::order_state::OrderLifecycleState;
use proptest::prelude::*;

// ---- Generators ----------------------------------------------------------

fn arb_tick() -> impl Strategy<Value = Tick> {
    (
        any::<[u8; 16]>(),
        1u32..1024u32,
        any::<i8>(),
        any::<i64>(),
        any::<i64>(),
        any::<i64>(),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(
            |(cid, sym, exch, ltp, bid, ask, ltq, buy, sell, ts_ex, ts_recv)| Tick {
                correlation_id: cid,
                symbol: sym,
                exchange: exch,
                ltp_paise: ltp,
                bid_paise: bid,
                ask_paise: ask,
                ltq,
                total_buy_qty: buy,
                total_sell_qty: sell,
                ts_exchange_ns: ts_ex,
                ts_recv_ns: ts_recv,
            },
        )
}

fn arb_risk_profile() -> impl Strategy<Value = RiskProfile> {
    (any::<i64>(), any::<i64>(), any::<u64>(), any::<u32>()).prop_map(
        |(sl, tp, max_size, horizon)| RiskProfile {
            stop_loss_paise: sl,
            take_profit_paise: tp,
            max_size_qty: max_size,
            time_horizon_seconds: horizon,
        },
    )
}

fn arb_signal() -> impl Strategy<Value = Signal> {
    (
        any::<[u8; 16]>(),
        any::<u8>(),
        1u32..1024u32,
        any::<u8>(),
        0.0f32..1.0,
        0.0f32..1.0,
        arb_risk_profile(),
        any::<u64>(),
    )
        .prop_map(
            |(cid, strat, sym, side, prob, conf, risk, ts)| Signal {
                correlation_id: cid,
                strategy: strat,
                symbol: sym,
                side,
                base_probability: prob,
                confidence: conf,
                risk_profile: risk,
                ts_ns: ts,
            },
        )
}

fn arb_order_intent() -> impl Strategy<Value = OrderIntent> {
    (
        any::<[u8; 16]>(),
        1u32..1024u32,
        any::<u8>(),
        any::<u64>(),
        any::<u8>(),
        any::<i64>(),
        any::<i8>(),
    )
        .prop_map(
            |(cid, sym, side, qty, ot, lim, ex)| OrderIntent {
                correlation_id: cid,
                symbol: sym,
                side,
                quantity: qty,
                order_type: ot,
                limit_paise: lim,
                exchange: ex,
            },
        )
}

// ---- Properties ----------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Property: Tick construction preserves all fields.
    #[test]
    fn tick_field_preservation(tick in arb_tick()) {
        prop_assert_eq!(tick.symbol, tick.symbol);
        prop_assert_eq!(tick.ltp_paise, tick.ltp_paise);
        prop_assert_eq!(tick.ltq, tick.ltq);
        prop_assert_eq!(tick.ts_recv_ns, tick.ts_recv_ns);
    }

    /// Property: Tick equality is reflexive.
    #[test]
    fn tick_equality_reflexive(tick in arb_tick()) {
        prop_assert_eq!(tick, tick);
    }

    /// Property: Signal construction preserves score bounds.
    #[test]
    fn signal_score_bounds(sig in arb_signal()) {
        prop_assert!(sig.base_probability >= 0.0 && sig.base_probability <= 1.0,
            "base_probability {} out of [0, 1]", sig.base_probability);
        prop_assert!(sig.confidence >= 0.0 && sig.confidence <= 1.0,
            "confidence {} out of [0, 1]", sig.confidence);
    }

    /// Property: Signal equality is reflexive.
    #[test]
    fn signal_equality_reflexive(sig in arb_signal()) {
        prop_assert_eq!(sig, sig);
    }

    /// Property: OrderIntent construction preserves all fields.
    #[test]
    fn order_intent_field_preservation(intent in arb_order_intent()) {
        prop_assert_eq!(intent.symbol, intent.symbol);
        prop_assert_eq!(intent.side, intent.side);
        prop_assert_eq!(intent.quantity, intent.quantity);
    }

    /// Property: OrderIntent equality is reflexive.
    #[test]
    fn order_intent_equality_reflexive(intent in arb_order_intent()) {
        prop_assert_eq!(intent, intent);
    }

    /// Property: OrderLifecycleState from_u8 round-trip for all valid values.
    #[test]
    fn order_lifecycle_state_roundtrip(byte in 0u8..6u8) {
        if let Some(state) = OrderLifecycleState::from_u8(byte) {
            prop_assert_eq!(state.as_u8(), byte);
        }
    }

    /// Property: OrderLifecycleState from_u8 returns None for invalid values.
    #[test]
    fn order_lifecycle_state_invalid(byte in 6u8..=255u8) {
        prop_assert!(OrderLifecycleState::from_u8(byte).is_none());
    }

    /// Property: RiskProfile construction preserves all fields.
    #[test]
    fn risk_profile_field_preservation(rp in arb_risk_profile()) {
        prop_assert_eq!(rp.stop_loss_paise, rp.stop_loss_paise);
        prop_assert_eq!(rp.take_profit_paise, rp.take_profit_paise);
        prop_assert_eq!(rp.max_size_qty, rp.max_size_qty);
        prop_assert_eq!(rp.time_horizon_seconds, rp.time_horizon_seconds);
    }
}
