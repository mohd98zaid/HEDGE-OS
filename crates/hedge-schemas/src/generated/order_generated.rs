//! Fallback bindings for `order.fbs`.

/// Mirror of `table OrderIntent_v1` in `schemas/order.fbs` (R6, R1.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct OrderIntent_v1 {
    pub correlation_id: [u8; 16],
    pub symbol: u32,
    pub side: u8,
    pub quantity: u64,
    pub order_type: u8,
    pub limit_paise: i64,
    pub exchange: i8,
}

/// Mirror of `table OrderState_v1` in `schemas/order.fbs`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct OrderState_v1 {
    pub correlation_id: [u8; 16],
    pub broker_order_id: String,
    pub state: u8,
    pub filled_qty: u64,
    pub avg_fill_paise: i64,
    pub ts_ns: u64,
}
