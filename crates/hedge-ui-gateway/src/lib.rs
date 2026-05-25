//! `hedge-ui-gateway` — NATS-to-WebSocket bridge for the Human_Control_UI.
//!
//! NOTE: this crate is the **only** Rust crate in the workspace that is NOT
//! part of the Hot_Path. It runs alongside the Hot_Path on the Mumbai VPS but
//! is on the operator-facing edge, not the order path. Implementation lands
//! in task E1. Stub for task **1.1**.
