//! `hedge-risk` — the **Risk_Engine**, the highest-authority component in
//! the PROJECT HEDGE Hot_Path (R5, R31, R32, R21).
//!
//! ## Authority hierarchy
//!
//! The Risk_Engine is the single source of order-dispatch authority
//! (R5.1, R21.1):
//!
//! ```text
//! Risk_Engine > Execution_Engine > Signal_Engine > Warm_AI_Pipeline > Trader_Input
//! ```
//!
//! Every order submitted by the Execution_Engine to a Broker_Adapter must
//! carry an [`ApprovalToken`](approval::ApprovalToken) HMAC-SHA256-signed
//! by the Risk_Engine over canonical [`OrderIntent_v1`] bytes. Verification
//! happens twice — once at the Risk_Engine boundary
//! ([`ApprovalVerifier`](approval::ApprovalVerifier)) and once at the
//! Execution_Engine boundary (R6.8). The signing key is process-private
//! and never serialized or published.
//!
//! ## Evaluation pipeline
//!
//! [`RiskEngine::evaluate`](engine::RiskEngine::evaluate) is the **only**
//! public entry point that produces an `Approved` decision. It runs every
//! gate in a fixed order and short-circuits on the first rejection so the
//! 2 ms p99 budget (R5.12, R28.3) is met:
//!
//! 1. Kill_Switch
//! 2. Session-time gate (IST, R31.1)
//! 3. Daily loss
//! 4. Drawdown (auto-engages Kill_Switch on breach, R5.5)
//! 5. Trade frequency (per-minute / per-hour / per-session, R5.6)
//! 6. Position size (per-symbol / portfolio, R5.3)
//! 7. Leverage (per-symbol / account, R5.4)
//! 8. Exposure (per-symbol / per-sector, R5.7)
//! 9. Slippage cooldown (R5.8)
//! 10. Volatility block (R5.10)
//! 11. Broker latency block (R5.11)
//! 12. Daily-profit-target post-policy (R32.3)
//! 13. Adaptive_Risk computation (R5.13) — minted via WarmCache last-known-value
//! 14. Sized-quantity derivation; mint single-use `ApprovalToken` on success
//!
//! ## Edge-triggered emissions (Property 8)
//!
//! * `risk.killswitch.activated` only on `false → true` transition.
//! * `risk.target.reached` only on the first crossing of the upper target band.
//! * `risk.cooldown.<sym>` only on cooldown engage / release transitions.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod approval;
pub mod cooldown;
pub mod decision;
pub mod engine;
pub mod frequency;
pub mod kill_switch;
pub mod state;
pub mod target;
pub mod volatility;
pub mod warmcache;

// ---- Public API re-exports ---------------------------------------------

pub use approval::{
    canonicalize_intent_bytes, ApprovalSigner, ApprovalToken, ApprovalVerifier,
    APPROVAL_TOKEN_BYTES, APPROVAL_TOKEN_HEX_LEN, INTENT_CANONICAL_BYTES,
};
pub use cooldown::{CooldownReason, CooldownRegistry};
pub use decision::{RiskDecision, RiskRationale};
pub use engine::{evaluate, RiskEngine};
pub use frequency::FrequencyCounters;
pub use kill_switch::{KillReason, KillSwitchState};
pub use state::{BrokerLatencyTable, RiskState};
pub use target::{TargetState, TargetTransition};
pub use volatility::VolatilityTable;
pub use warmcache::{MockWarmCacheView, WarmCacheView};

// Convenience re-export of the rejection-reason enum so call sites can
// write `hedge_risk::RejectionReason::MaxDailyLoss`.
pub use hedge_schemas::rejection_reason::RejectionReason;
