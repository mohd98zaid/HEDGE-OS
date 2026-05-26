//! `hedge-ui-gateway` — NATS-to-WebSocket bridge for the Human_Control_UI.
//!
//! NOTE: this crate is the **only** Rust crate in the workspace that is
//! NOT part of the Hot_Path. It runs alongside the Hot_Path on the
//! Mumbai VPS but is on the operator-facing edge, not the order path.
//!
//! ### Responsibilities (task 36.1)
//!
//! 1. Expose a single WebSocket endpoint with a topic-subscription
//!    protocol; payloads are JSON for UI ergonomics
//!    (design § Data Models § WebSocket Channels (UI Gateway)).
//! 2. Surface eleven curated channels: `market`, `orderflow`, `signals`,
//!    `risk`, `exec`, `news`, `psych`, `alerts`, `replay`, `latency`,
//!    `control` — each backed by a curated NATS subject set.
//! 3. Join `sig.emitted` with `ai.rank.*` by `correlation_id` for the
//!    `/signals` channel and filter out shadowed AI sources per
//!    `AI_Shadow_Mode` (R23.2, R24.3).
//! 4. Sort `/alerts` by severity DESC, timestamp DESC so critical
//!    alerts surface above non-critical ones (R20.5).
//! 5. Track `md.breadth.volatility` against `ui.high_vol_threshold` and
//!    flip the cockpit into high-volatility presentation mode when the
//!    threshold is crossed (R20.4).
//! 6. Publish `trader.intent.*` events on the `/control` channel:
//!    `trader.intent.killswitch`, `trader.intent.strategy_toggle`,
//!    `trader.intent.priority`, `trader.intent.order` (R20.6, R20.7,
//!    R20.8). Authority Hierarchy is enforced downstream by the
//!    Risk_Engine.
//!
//! ### Hot_Path purity
//!
//! This crate runs adjacent to the Hot_Path but is *not* part of it.
//! Even so, every dependency in `Cargo.toml` is allowed under the
//! `forbid_modules` rule (no `pyo3`, `numpy`, `pandas`,
//! `reqwest::blocking`, or cloud LLM SDK).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod alerts;
pub mod channels;
pub mod dispatcher;
pub mod gateway;
pub mod intents;
pub mod protocol;
pub mod signals_join;
pub mod subscriptions;
pub mod volatility;

pub use alerts::{severity_for_subject, AlertBuffer, Severity, UiAlert};
pub use channels::{classify_subject, nats_patterns, ChannelMatch};
pub use dispatcher::{Dispatcher, DispatcherState, NatsEvent};
pub use gateway::{run_session, serve, GatewayConfig};
pub use intents::{
    publish_intent, validate_intent, IntentError, IntentPublisher, NatsIntentPublisher,
    RecordingPublisher,
};
pub use protocol::{Channel, ClientMsg, ErrorCode, IntentKind, ServerMsg};
pub use signals_join::{AiShadowFilter, JoinOutcome, SignalsJoiner};
pub use subscriptions::Subscriptions;
pub use volatility::{RefreshCadence, VolatilityTracker};
