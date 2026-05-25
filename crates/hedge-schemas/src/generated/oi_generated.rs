//! Fallback bindings for `oi.fbs`.

/// Mirror of `table OpenInterest_v1` in `schemas/oi.fbs` (R1.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct OpenInterest_v1 {
    pub correlation_id: [u8; 16],
    pub symbol: u32,
    pub oi_total: u64,
    pub oi_change: i64,
    pub ts_ns: u64,
}
