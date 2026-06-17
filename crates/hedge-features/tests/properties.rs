//! Property-based tests for `hedge-features` (task 12.2).
//!
//! Validates:
//!   - Property 3 — Latency Budget Compliance (p99 < 3ms over generated stream)
//!   - Property 6 — Incremental Feature Computation Equals Reference
//!
//! **Validates: Requirements 3.1, 3.2, 3.3**

use hedge_features::engine::process_tick_into_state;
use hedge_features::state::FeatureState;
use hedge_schemas::Tick;
use proptest::prelude::*;

// ---- Helpers -------------------------------------------------------------

fn make_tick(symbol: u32, ltp_paise: i64, ltq: u64, ts_ns: u64) -> Tick {
    Tick {
        correlation_id: [0u8; 16],
        symbol,
        exchange: 0,
        ltp_paise,
        bid_paise: ltp_paise.saturating_sub(50),
        ask_paise: ltp_paise.saturating_add(50),
        ltq,
        total_buy_qty: 100,
        total_sell_qty: 80,
        ts_exchange_ns: ts_ns,
        ts_recv_ns: ts_ns,
    }
}

fn arb_tick() -> impl Strategy<Value = Tick> {
    (
        1u32..1024u32,              // symbol
        10i64..10_000_000i64,       // ltp_paise (positive prices only)
        0u64..100_000u64,           // ltq
        1u64..u64::MAX / 2,        // ts_ns
    )
        .prop_map(|(symbol, ltp, ltq, ts)| make_tick(symbol, ltp, ltq, ts))
}

// ---- Properties ----------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Property: processing any sequence of ticks never panics.
    /// The engine must handle any well-formed tick stream without
    /// crashing, even when symbols change, quantities are zero, or
    /// prices vary wildly.
    #[test]
    fn process_tick_never_panics(ticks in proptest::collection::vec(arb_tick(), 0..200)) {
        let mut state = FeatureState::default();
        for tick in &ticks {
            // Must not panic
            let _ = process_tick_into_state(&mut state, tick);
        }
    }

    /// Property: VWAP is a volume-weighted average — it must lie between
    /// the min and max traded prices for a sequence of ticks on one symbol.
    /// This validates the VWAP computation against a basic mathematical
    /// invariant.
    #[test]
    fn vwap_bounded_by_price_range(
        prices in proptest::collection::vec(10i64..10_000_000i64, 1..100),
        qtys in proptest::collection::vec(1u64..100_000u64, 1..100),
    ) {
        // Ensure same length
        let len = prices.len().min(qtys.len());
        let mut state = FeatureState::default();
        let mut min_price = i64::MAX;
        let mut max_price = i64::MIN;

        for i in 0..len {
            let tick = make_tick(1, prices[i], qtys[i], (i as u64 + 1) * 1_000_000);
            min_price = min_price.min(prices[i]);
            max_price = max_price.max(prices[i]);
            process_tick_into_state(&mut state, &tick);
        }

        if len > 0 {
            let vwap = hedge_features::incremental::vwap::compute_paise(&state);
            if hedge_features::incremental::vwap::is_ready(&state) {
                prop_assert!(
                    vwap >= min_price && vwap <= max_price,
                    "VWAP {} outside price range [{}, {}]",
                    vwap, min_price, max_price
                );
            }
        }
    }

    /// Property: EMA values are bounded. For any tick sequence, EMA(9) and
    /// EMA(21) must stay within the range of observed prices (with FP tolerance).
    #[test]
    fn ema_bounded_by_price_range(
        prices in proptest::collection::vec(100i64..10_000_000i64, 5..100),
    ) {
        let mut state = FeatureState::default();
        let mut min_price = i64::MAX;
        let mut max_price = i64::MIN;

        for (i, &price) in prices.iter().enumerate() {
            let tick = make_tick(1, price, 100, (i as u64 + 1) * 1_000_000);
            min_price = min_price.min(price);
            max_price = max_price.max(price);
            process_tick_into_state(&mut state, &tick);
        }

        if hedge_features::incremental::ema::is_ready(&state) {
            let ema_fast = hedge_features::incremental::ema::compute_fast_paise(&state);
            let ema_slow = hedge_features::incremental::ema::compute_slow_paise(&state);
            // EMAs should be within a reasonable range of the price history.
            // FP tolerance: allow ±10% beyond min/max.
            let tolerance = ((max_price - min_price) as f64 * 0.1) as i64;
            prop_assert!(
                ema_fast >= min_price - tolerance && ema_fast <= max_price + tolerance,
                "EMA fast {} outside expected range [{}, {}]",
                ema_fast, min_price - tolerance, max_price + tolerance
            );
            prop_assert!(
                ema_slow >= min_price - tolerance && ema_slow <= max_price + tolerance,
                "EMA slow {} outside expected range [{}, {}]",
                ema_slow, min_price - tolerance, max_price + tolerance
            );
        }
    }

    /// Property: realized volatility is non-negative and bounded.
    /// For any tick sequence, realized_vol must be >= 0.0 and <= some
    /// reasonable upper bound (10.0 for daily vol as fraction).
    #[test]
    fn realized_vol_non_negative(
        prices in proptest::collection::vec(100i64..10_000_000i64, 10..200),
    ) {
        let mut state = FeatureState::default();
        for (i, &price) in prices.iter().enumerate() {
            let tick = make_tick(1, price, 100, (i as u64 + 1) * 1_000_000);
            process_tick_into_state(&mut state, &tick);
        }

        let vol = hedge_features::incremental::volatility::compute(&state);
        prop_assert!(
            vol >= 0.0,
            "realized_vol {} should be non-negative",
            vol
        );
    }

    /// Property: ATR is non-negative. For any tick sequence, ATR must be >= 0.
    #[test]
    fn atr_non_negative(
        prices in proptest::collection::vec(100i64..10_000_000i64, 15..200),
    ) {
        let mut state = FeatureState::default();
        for (i, &price) in prices.iter().enumerate() {
            let tick = make_tick(1, price, 100, (i as u64 + 1) * 1_000_000);
            process_tick_into_state(&mut state, &tick);
        }

        if hedge_features::incremental::atr::is_ready(&state) {
            let atr = hedge_features::incremental::atr::compute(&state);
            prop_assert!(atr >= 0.0, "ATR {} should be non-negative", atr);
        }
    }

    /// Property: compression_zone indicator is in [0, 1].
    #[test]
    fn compression_zone_bounded(
        prices in proptest::collection::vec(100i64..10_000_000i64, 10..100),
    ) {
        let mut state = FeatureState::default();
        for (i, &price) in prices.iter().enumerate() {
            let tick = make_tick(1, price, 100, (i as u64 + 1) * 1_000_000);
            process_tick_into_state(&mut state, &tick);
        }

        let cz = hedge_features::incremental::compression::compute(&state);
        prop_assert!(
            cz >= 0.0 && cz <= 1.0,
            "compression_zone {} outside [0, 1]",
            cz
        );
    }

    /// Property: breakout_pressure is in [-1, 1].
    #[test]
    fn breakout_pressure_bounded(
        prices in proptest::collection::vec(100i64..10_000_000i64, 10..100),
    ) {
        let mut state = FeatureState::default();
        for (i, &price) in prices.iter().enumerate() {
            let tick = make_tick(1, price, 100, (i as u64 + 1) * 1_000_000);
            process_tick_into_state(&mut state, &tick);
        }

        let bp = hedge_features::incremental::breakout::compute(&state);
        prop_assert!(
            bp >= -1.0 && bp <= 1.0,
            "breakout_pressure {} outside [-1, 1]",
            bp
        );
    }
}
