//! Per-subject synth generator tasks.
//!
//! Every generator takes a clone of the `SuppressionRegistry`, the
//! shared `LtpBoard`, a per-stream `Mulberry32` RNG, and a NATS publisher
//! handle. They run as long-lived tokio tasks owned by `coordinator.rs`.
//!
//! Each generator wraps every publish in
//! `if registry.allow_publish(subject) { ... }` so a real publisher
//! coming online silences the synth automatically.

pub mod book;
pub mod breadth;
pub mod connection;
pub mod features;
pub mod latency;
pub mod news;
pub mod oi;
pub mod orderflow;
pub mod psych;
pub mod replay;
pub mod signal;
pub mod tick;
pub mod trade_chain;
