//! `hedge-supervisor` — Self_Healing_Supervisor (R25.1–R25.5, R29.6).
//!
//! Three composable async components, all in the same crate but
//! independently exposed (design § Components § Self_Healing_Supervisor):
//!
//! | Component                                | Module        |
//! |------------------------------------------|---------------|
//! | [`FailureDetector`]                      | [`detector`]  |
//! | [`RecoveryPolicy`]                       | [`policy`]    |
//! | [`RecoveryActuator`]                     | [`actuator`]  |
//! | [`SupervisorStateStore`] (last-known-healthy persistence) | [`state`]     |
//! | [`Supervisor`] (orchestrator, run loop)  | [`supervisor`]|
//!
//! ### Architecture
//!
//! ```text
//!  Failure_Detector ── mpsc<FailureEvent> ──▶ Recovery_Policy ──▶ Recovery_Actuator
//!     (NATS sub)                                (decide)              (NATS pub)
//!                                                  │
//!                                                  └── SupervisorStateStore
//!                                                       (atomic JSON file)
//! ```
//!
//! Run as a **separate process** (binary
//! `crates/hedge-supervisor/src/main.rs` → `hedge/supervisor:dev`
//! container, see `docker/hot_path/Dockerfile.supervisor`) so a
//! Hot_Path crash never kills the supervisor (R29.6, design
//! § Self-Healing Flow).
//!
//! ### Hot_Path discipline (R30)
//!
//! Although the supervisor runs in its own process, its dependencies
//! are still vetted by the workspace-wide forbidden-deps CI gate
//! (task 8.1, `scripts/check-forbidden-deps.sh`). The supervisor must
//! be small and reliable, so:
//!
//! * `#![forbid(unsafe_code)]` (defensive).
//! * No `pyo3`, `numpy`, `pandas`, or any Python runtime.
//! * No `reqwest::blocking`, no cloud LLM SDKs, no TradingView /
//!   Pine Script.
//!
//! The exhaustive list lives in [`forbid::FORBIDDEN_DEPENDENCIES`].
//!
//! Unlike Hot_Path crates the supervisor is **allowed** to use timer
//! primitives — exponential-backoff scheduling for the WS reconnect
//! action uses `tokio::time::sleep` via
//! [`supervisor::reconnect_sleep_for`]. The crate is therefore
//! exempt from `scripts/check-no-polling.sh` (see the exclusion in
//! that script).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod actuator;
pub mod detector;
pub mod event;
pub mod forbid;
pub mod policy;
pub mod state;
pub mod supervisor;

pub use actuator::{
    build_ops_action_payload, AiOllamaDegradedPayload, CacheRedisDegradedPayload,
    ExecBrokerFailoverPayload, OpsActionPayload, RecoveryActuator,
};
pub use detector::{
    BrokerFailoverThresholds, FailureDetector, LatencySpikeThresholds,
};
pub use event::{FailureEvent, FailureKind, RecoveryAction, RecoveryActionKind};
pub use forbid::FORBIDDEN_DEPENDENCIES;
pub use policy::{BackoffParams, RecoveryPolicy};
pub use state::{
    save_to_path, BrokerFailoverRecord, StateError, SupervisorState, SupervisorStateStore,
    DEFAULT_STATE_PATH, STATE_VERSION,
};
pub use supervisor::{reconnect_sleep_for, Supervisor, SupervisorError, FAILURE_CHANNEL_DEPTH};
