//! Liquidity imbalance and orderflow strength.
//!
//! ## Liquidity imbalance
//!
//! `liquidity_imbalance = (buy_qty - sell_qty) / max(buy_qty + sell_qty, 1)`,
//! clamped to `[-1.0, 1.0]`. Computed from the `total_buy_qty` and
//! `total_sell_qty` fields of every `Tick_v1`.
//!
//! ## Orderflow strength
//!
//! Forwarded directly from the Orderflow_Engine via `of.event.<sym>`
//! (design § Components § Orderflow_Engine). The Orderflow_Engine
//! computes a richer `liquidity_pressure ∈ [-1, 1]` signal (R2.5) that
//! we surface here as `orderflow_strength`. Until the engine binary
//! plumbs that subscription, [`update_orderflow`] lets callers seed
//! the value directly, and [`update`] keeps the cached value untouched.

use hedge_schemas::Tick;

use crate::state::{FeatureState, LastBook};

/// Fold the new tick's book snapshot into the cached liquidity imbalance.
///
/// Also stores the new `LastBook` for the rolling-delta sign-volume
/// heuristic.
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    state.last_book = LastBook {
        bid_paise: tick.bid_paise,
        ask_paise: tick.ask_paise,
        total_buy_qty: tick.total_buy_qty,
        total_sell_qty: tick.total_sell_qty,
    };

    let buy = tick.total_buy_qty as f64;
    let sell = tick.total_sell_qty as f64;
    let denom = (buy + sell).max(1.0);
    let raw = (buy - sell) / denom;
    state.liquidity_imbalance_cached = raw.clamp(-1.0, 1.0) as f32;
}

/// Allow the engine binary to push the latest Orderflow_Engine
/// `liquidity_pressure` into `orderflow_strength_cached`.
///
/// `value` is clamped to `[-1.0, 1.0]` defensively.
#[inline]
pub fn update_orderflow(state: &mut FeatureState, value: f32) {
    state.orderflow_strength_cached = value.clamp(-1.0, 1.0);
}

/// Returns the cached liquidity imbalance.
#[inline]
pub fn compute_imbalance(state: &FeatureState) -> f32 {
    state.liquidity_imbalance_cached
}

/// Returns the cached orderflow strength.
#[inline]
pub fn compute_orderflow_strength(state: &FeatureState) -> f32 {
    state.orderflow_strength_cached
}

/// Schema-facing `compute` returns the imbalance (FlatBuffers
/// `liquidity_imbalance: float`).
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    compute_imbalance(state)
}

/// `true` once at least one tick has been folded in.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.tick_count > 0 || state.last_book.total_buy_qty + state.last_book.total_sell_qty > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick_full;

    #[test]
    fn liquidity_zero_before_any_tick() {
        let s = FeatureState::default();
        assert_eq!(compute_imbalance(&s), 0.0);
        assert_eq!(compute_orderflow_strength(&s), 0.0);
    }

    #[test]
    fn liquidity_imbalance_clamps_to_unit_range() {
        // 100 buy / 0 sell → imbalance == 1.0 exactly.
        // tick_full args: (ltp_paise, ltq, total_buy_qty, total_sell_qty, ts_exchange, ts_recv).
        let mut s = FeatureState::default();
        let t = tick_full(100_00, 1, 100, 0, 0, 0);
        update(&mut s, &t);
        assert!((compute_imbalance(&s) - 1.0).abs() < 1e-6_f32);
    }

    #[test]
    fn liquidity_imbalance_zero_when_balanced() {
        let mut s = FeatureState::default();
        // Same buy and sell quantities → imbalance == 0.
        let t = tick_full(100_00, 1, 50, 50, 0, 0);
        update(&mut s, &t);
        assert!(compute_imbalance(&s).abs() < 1e-6_f32);
    }

    #[test]
    fn liquidity_imbalance_negative_when_sellers_dominate() {
        let mut s = FeatureState::default();
        // Sellers dominate: 0 buy, 100 sell → imbalance == -1.0.
        let t = tick_full(100_00, 1, 0, 100, 0, 0);
        update(&mut s, &t);
        assert!((compute_imbalance(&s) + 1.0).abs() < 1e-6_f32);
    }

    #[test]
    fn orderflow_strength_clamps_external_input() {
        let mut s = FeatureState::default();
        update_orderflow(&mut s, 5.0); // out of range
        assert_eq!(compute_orderflow_strength(&s), 1.0);
        update_orderflow(&mut s, -5.0);
        assert_eq!(compute_orderflow_strength(&s), -1.0);
        update_orderflow(&mut s, 0.42);
        assert!((compute_orderflow_strength(&s) - 0.42).abs() < 1e-6_f32);
    }
}
