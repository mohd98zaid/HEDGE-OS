//! `hedge-exec` — the **Execution_Engine** (R6).
//!
//! The Execution_Engine is the only component that submits orders to a
//! broker. It enforces three structural invariants (R6.8, R21.1):
//!
//! 1. Every submission requires a valid HMAC-SHA256 [`ApprovalToken`]
//!    minted by the Risk_Engine. `submit(&ApprovalToken, &OrderIntent)`
//!    is the only public entry point that produces a broker-side
//!    order — submission without a valid approval is unrepresentable.
//! 2. Every order traverses a fixed FSM
//!    `New → Submitted → {Partially_Filled → Filled, Filled,
//!    Cancelled, Rejected}` and exactly one
//!    `exec.order.<state>` event is published per transition
//!    (Property 9).
//! 3. `BrokerRouter` holds an active+backup adapter pair. A
//!    sliding-window error-rate or latency breach atomically swaps
//!    the active slot and emits `exec.broker.failover` (R6.5).
//!
//! ## Module layout
//!
//! * [`error`] — the unified [`ExecError`] enum.
//! * [`lifecycle`] — the [`OrderLifecycleTracker`] FSM (R6.3).
//! * [`retry`] — bounded exponential-backoff retry helper (R6.4).
//! * [`router`] — the [`BrokerRouter`] with active/backup, sliding
//!   window, atomic failover (R6.5).
//! * [`engine`] — the [`ExecutionEngine`] orchestrator (R6.1, R6.2,
//!   R6.6, R6.7, R6.8, R22.4).
//!
//! The [`hedge_broker_api::BrokerAdapter`] trait and its companion
//! types ([`OrderIntent`](hedge_broker_api::OrderIntent),
//! [`BrokerError`](hedge_broker_api::BrokerError),
//! [`SubmitAck`](hedge_broker_api::SubmitAck), etc.) live in the
//! `hedge-broker-api` crate so concrete brokers can implement against
//! the trait without depending on `hedge-exec`.
//!
//! ## Hot_Path discipline (R30)
//!
//! `.github/workflows/hot-path-purity.yml` enforces in CI:
//!
//! * No `pyo3`, `numpy`, `pandas`, or any Python runtime dependency.
//! * No `reqwest::blocking`. Adapters are async-only.
//! * No `tokio` polling timers on steady-state paths (the retry backoff
//!   in [`retry`] is annotated with the per-line `hedge-allow:
//!   polling-loop` marker because bounded exponential backoff is the
//!   intended use-case).
//! * No cloud LLM SDK — broker decisions are deterministic and local.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod engine;
pub mod error;
pub mod lifecycle;
pub mod retry;
pub mod router;

// ---- Public API re-exports ---------------------------------------------

pub use engine::{
    default_jitter_source, EngineEvent, ExecutionEngine, ReplayMode, EXEC_ROUTE_BUDGET_NS,
};
pub use error::ExecError;
pub use lifecycle::{is_legal_transition, LifecycleEvent, OrderLifecycleTracker};
pub use retry::{
    retry_with_backoff, JitterSource, NoJitter, RecordingSleeper, RetryPolicy, SeededJitter,
    Sleeper, TokioSleeper,
};
pub use router::{
    ActiveSlot, AdapterStats, BrokerRouter, FailoverEvent, FailoverThresholds, Outcome,
    WINDOW_SIZE,
};

// Re-export the broker-api trait surface so callers don't need a
// direct `hedge-broker-api` dependency just to construct `Arc<dyn
// BrokerAdapter>`.
pub use hedge_broker_api::{
    BrokerAdapter, BrokerError, BrokerMetric, BrokerOp, OrderIntent as BrokerOrderIntent,
    OrderModification, OrderStatus, OrderType, ReadyState, SubmitAck,
};

// Re-export the Risk_Engine approval surface so doc-links resolve and
// callers can type `hedge_exec::ApprovalToken`.
pub use hedge_risk::{ApprovalToken, ApprovalVerifier};
