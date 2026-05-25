//! Fallback bindings for `features.fbs`.

/// Mirror of `table FeatureSnapshot_v1` in `schemas/features.fbs` (R3, R1.5).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct FeatureSnapshot_v1 {
    pub correlation_id: [u8; 16],
    pub symbol: u32,
    pub vwap: i64,
    pub atr: i64,
    pub ema_fast: i64,
    pub ema_slow: i64,
    pub ema_slope: f32,
    pub realized_vol: f32,
    pub momentum: f32,
    pub rolling_delta: i64,
    pub liquidity_imbalance: f32,
    pub orderflow_strength: f32,
    pub candle_structure: u8,
    pub breakout_pressure: f32,
    pub compression_zone: f32,
    pub liquidity_sweep: f32,
    pub ts_ns: u64,
}
