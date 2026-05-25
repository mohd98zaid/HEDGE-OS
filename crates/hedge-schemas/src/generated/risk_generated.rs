//! Fallback bindings for `risk.fbs`.

use super::order_generated::OrderIntent_v1;

/// Mirror of `table RiskApproval_v1` in `schemas/risk.fbs` (R5.13, R5.14).
///
/// `approval_token` is an HMAC-SHA256 (32 bytes) over canonical
/// `OrderIntent_v1` bytes. Single-use; the Execution_Engine consumes it
/// before submitting to a Broker_Adapter (R6.8, R21.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct RiskApproval_v1 {
    pub correlation_id: [u8; 16],
    pub approval_token: [u8; 32],
    pub intent: OrderIntent_v1,
    pub sized_quantity: u64,
    pub rationale_code: u8,
    pub ts_ns: u64,
}
