//! Fallback bindings for `orderbook.fbs`.

/// Maximum number of bid or ask levels carried in a `OrderBook_v1`.
/// FlatBuffers vectors are dynamically sized, but the design caps level-2
/// depth at 20 (R1.5).
pub const MAX_BOOK_LEVELS: usize = 20;

/// Mirror of `struct BookLevel` in `schemas/orderbook.fbs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct BookLevel {
    pub price_paise: i64,
    pub qty: u64,
    pub orders: u32,
}

/// Mirror of `table OrderBook_v1` in `schemas/orderbook.fbs`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct OrderBook_v1 {
    pub correlation_id: [u8; 16],
    pub symbol: u32,
    pub exchange: i8,
    /// Up to `MAX_BOOK_LEVELS` entries, sorted from best to worst bid.
    pub bid_levels: Vec<BookLevel>,
    /// Up to `MAX_BOOK_LEVELS` entries, sorted from best to worst ask.
    pub ask_levels: Vec<BookLevel>,
    pub ts_ns: u64,
}
