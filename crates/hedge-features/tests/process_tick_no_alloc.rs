//! Integration test: `process_tick_into_state` is allocation-free in
//! steady state (R2.6 / R3.4).
//!
//! `process_tick_into_state` is the alloc-relevant body of
//! [`FeatureExtractionEngine::process_tick`] — the per-symbol cell
//! lookup is a sharded hash + uncontended mutex, both allocation-free
//! after the first tick. Exercising `process_tick_into_state` directly
//! sidesteps the need for a live `NatsClient` in unit-test runs while
//! testing the same hot path the engine traverses on every tick.
//!
//! The harness uses [`hedge_core::alloc_harness::assert_no_alloc`],
//! which is a no-op when the `alloc-tracking` feature is disabled. CI
//! enables the feature to make the assertion meaningful; local
//! `cargo test -p hedge-features` runs it as a smoke check.

use hedge_features::{engine::process_tick_into_state, state::FeatureState};
use hedge_schemas::Tick;

fn tick(symbol: u32, ltp_paise: i64, ltq: u64, ts_ns: u64) -> Tick {
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

#[test]
fn process_tick_into_state_is_allocation_free_after_warmup() {
    let mut state = FeatureState::default();

    // Warm-up: 64 ticks fill every window. Any first-time allocation
    // for indicator scratch space happens here, OUTSIDE the harness.
    for i in 0..64u64 {
        let t = tick(1, 100_00 + (i as i64) * 10, 5, i * 1_000_000);
        process_tick_into_state(&mut state, &t);
    }

    // Steady state: the next 100 ticks must perform zero heap activity.
    // When the `alloc-tracking` feature is off this is a no-op; when on,
    // any allocator activity panics with the offending stage's name.
    hedge_core::alloc_harness::assert_no_alloc("process_tick steady-state", || {
        for i in 64..164u64 {
            let t = tick(1, 100_00 + (i as i64) * 10, 5, i * 1_000_000);
            let snap = process_tick_into_state(&mut state, &t);
            std::hint::black_box(snap);
        }
    });
}

#[test]
fn process_tick_into_state_drives_a_full_warmup_then_emits_non_zero_features() {
    let mut state = FeatureState::default();
    for i in 0..50u64 {
        let t = tick(1, 100_00 + (i as i64) * 10, 5, i * 1_000_000);
        let _ = process_tick_into_state(&mut state, &t);
    }
    let final_snap =
        process_tick_into_state(&mut state, &tick(1, 100_00 + 50 * 10, 5, 50 * 1_000_000));

    // After 51 ticks every windowed indicator should be warm and
    // emitting non-zero values.
    assert!(final_snap.vwap > 0, "vwap should be non-zero post-warmup");
    assert!(final_snap.atr > 0, "atr should be non-zero post-warmup");
    assert!(final_snap.ema_fast > 0, "ema_fast should be non-zero post-warmup");
    assert!(final_snap.ema_slow > 0, "ema_slow should be non-zero post-warmup");
    assert_ne!(final_snap.ema_slope, 0.0, "ema_slope should be non-zero on a trend");
    assert_ne!(final_snap.momentum, 0.0, "momentum should be non-zero on a trend");
    assert_eq!(final_snap.symbol, 1);
}

#[test]
fn process_tick_into_state_emits_zeroes_before_warmup() {
    let mut state = FeatureState::default();
    let snap = process_tick_into_state(&mut state, &tick(1, 100_00, 1, 0));
    // First-tick snapshot: VWAP becomes the trade price; everything
    // else is unset and emits 0 because warm-up gates are not met.
    assert_eq!(snap.vwap, 100_00);
    // ATR's window has 1 sample, but is_ready requires 14 → compute is
    // still defined (mean of 1 element) and equals 0 because TR(t0)=0.
    assert_eq!(snap.atr, 0);
    assert_eq!(snap.ema_slope, 0.0);
    assert_eq!(snap.realized_vol, 0.0);
    assert_eq!(snap.momentum, 0.0);
    assert_eq!(snap.compression_zone, 0.0);
}
