//! Fallback bindings for `signal.fbs`.

/// Mirror of `struct RiskProfile_v1` in `schemas/signal.fbs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct RiskProfile_v1 {
    pub stop_loss_paise: i64,
    pub take_profit_paise: i64,
    pub max_size_qty: u64,
    pub time_horizon_seconds: u32,
}

/// Mirror of `table Signal_v1` in `schemas/signal.fbs` (R4, R1.5).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct Signal_v1 {
    pub correlation_id: [u8; 16],
    pub strategy: u8,
    pub symbol: u32,
    pub side: u8,
    pub base_probability: f32,
    pub confidence: f32,
    pub risk_profile: RiskProfile_v1,
    pub ts_ns: u64,
}
