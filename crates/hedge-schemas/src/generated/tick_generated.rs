//! Fallback bindings for `tick.fbs`.
//!
//! Regenerate with `flatc --rust schemas/tick.fbs` once `flatc` is on PATH
//! (the workspace `build.rs` will do this automatically). Until then the
//! POD struct below mirrors the schema fields so consumer crates can
//! type-check against `hedge::v1::Tick_v1`.

/// Mirror of `table Tick_v1` in `schemas/tick.fbs` (R1.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct Tick_v1 {
    pub correlation_id: [u8; 16],
    pub symbol: u32,
    pub exchange: i8,
    pub ltp_paise: i64,
    pub bid_paise: i64,
    pub ask_paise: i64,
    pub ltq: u64,
    pub total_buy_qty: u64,
    pub total_sell_qty: u64,
    pub ts_exchange_ns: u64,
    pub ts_recv_ns: u64,
}
