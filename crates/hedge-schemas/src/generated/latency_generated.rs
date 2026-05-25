//! Fallback bindings for `latency.fbs`.

/// Mirror of `table LatencyRecord_v1` in `schemas/latency.fbs` (R9, R28).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct LatencyRecord_v1 {
    pub correlation_id: [u8; 16],
    pub stage: u8,
    pub nanos: u64,
    pub budget_nanos: u64,
    pub breach: bool,
}
