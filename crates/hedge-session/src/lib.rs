//! `hedge-session` — Session manager and `Market_Open_War_Mode`
//! controller.
//!
//! ## Scope
//!
//! This crate delivers two cooperating IST-clock observers:
//!
//! * The **War_Mode controller** half of the Cross-Cutting group
//!   (task 42.1, design § Operating Modes, R26.1–R26.4) which fires
//!   `ops.warmode.start` / `ops.warmode.end` over the
//!   `[09:15:00, 09:45:00]` IST window.
//! * The **Trading_Session controller** half (task 43.1, design
//!   § Configuration Surface and Defaults; Components § Risk_Engine,
//!   R31.1–R31.4) which fires `ops.session.start` / `ops.session.end`
//!   over the `[09:15:00, 15:30:00]` IST window.
//!
//! Both controllers share the [`controller::WallClock`] abstraction so
//! a single deterministic test clock can drive both simultaneously, but
//! each carries a phase-typed event ([`WarModeEvent`] vs
//! [`SessionEvent`]) and uses its own publisher trait so the wire
//! schemas stay independent.
//!
//! ## Module layout
//!
//! * [`event`] — typed `WarModeEvent` payload mirroring
//!   `hedge-schemas/json_schemas/ops_warmode.schema.json`.
//! * [`session_event`] — typed `SessionEvent` payload mirroring
//!   `hedge-schemas/json_schemas/ops_session.schema.json`.
//! * [`controller`] — [`controller::WarModeController`], its
//!   `Inactive`/`Active` state machine, and the
//!   [`controller::WallClock`] / [`controller::OpsEventPublisher`]
//!   traits.
//! * [`session_controller`] — [`session_controller::SessionController`],
//!   its `Inactive`/`Active` state machine, and the
//!   [`session_controller::SessionEventPublisher`] trait. Reuses
//!   [`controller::WallClock`].
//! * [`publisher`] — [`publisher::NatsOpsEventPublisher`] and
//!   [`publisher::NatsSessionEventPublisher`], the production
//!   bus-backed publishers.
//!
//! ## Hot_Path discipline
//!
//! `hedge-session` is a Hot_Path-adjacent crate (it appears in the
//! Hot_Path purity allowlist at `scripts/check-no-polling.sh`). Both
//! controllers' tokio tasks use one one-shot `sleep_until` per
//! state-machine edge — never a steady-state polled timer — so they
//! satisfy the no-polling rule (R30.3) without an opt-out at the call
//! site (the `sleep_until` line carries the documented
//! `hedge-allow: polling-loop` marker).
//!
//! ## Downstream consumers (informational)
//!
//! Hot_Path components subscribe to either family of `ops.*` subjects
//! and apply edge-triggered behaviour:
//!
//! * `ops.warmode.*` — `hedge-features`, `hedge-orderflow`,
//!   `hedge-signals`, `hedge-risk`, `hedge-ui-gateway` (R26.2, R26.3,
//!   R26.4). See [`controller`] module docs and the crate `README.md`
//!   for the full subscriber map.
//! * `ops.session.*` — `hedge-risk` (session-time gate corroboration,
//!   session-end cancel of non-persistent orders per R31.4),
//!   `hedge-features` (cumulative-VWAP reset on `start`, R15.3),
//!   `hedge-warmcache`, `hedge-position`, the Previous_Day_Memory_Engine
//!   (pre-session compute job on `end`, R15.3), and the UI gateway.
//!
//! The Risk_Engine's session-time gate (R31.1) is implemented locally
//! inside [`hedge_risk::RiskEngine::evaluate`] against
//! [`hedge_config::SessionConfig`]; the `ops.session.*` events from
//! this crate do **not** drive the gate directly. They are the
//! announcement that lets every component pivot edge-triggered on the
//! transition.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod controller;
pub mod event;
pub mod publisher;
pub mod session_controller;
pub mod session_event;

// Re-exports to keep the public API ergonomic at the crate root.
pub use controller::{
    DaySchedule, OpsEventPublisher, SystemWallClock, WallClock, WarModeController, WarModeState,
};
pub use event::{WarModeEvent, WarModePhase};
pub use publisher::{NatsOpsEventPublisher, NatsSessionEventPublisher};
pub use session_controller::{
    SessionController, SessionDaySchedule, SessionEventPublisher, SessionState,
};
pub use session_event::{SessionEvent, SessionPhase};
