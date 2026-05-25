//! `hedge-orderflow` — Orderflow_Engine library (task **11.1**).
//!
//! Implements `design.md § Components § Orderflow_Engine` and Requirements
//! R2.1–R2.6:
//!
//! * **Metrics** ([`metrics`]) — bid/ask imbalance, aggressive buyer /
//!   seller volume, rolling delta, top-5 liquidity pressure.
//! * **Events** ([`events`]) — typed [`OrderflowEvent`] variants and the
//!   stateful detector that emits them: liquidity gap (>3 ticks),
//!   absorption, hidden liquidity, spoofing (large quote that disappears
//!   within 500 ms without filling).
//! * **Heatmap** ([`heatmap`]) — `tokio::sync::watch`-exposed
//!   [`HeatmapSnapshot`] that the UI gateway forwards to the React
//!   cockpit (R2.4).
//! * **Engine** ([`engine`]) — [`OrderflowEngine`] orchestrator that
//!   consumes `md.book.<sym>` and `md.tick.<sym>` from NATS, produces
//!   `of.event.<sym>` and `of.heatmap.<sym>`, and stores per-symbol state
//!   in a `parking_lot::Mutex<HashMap<SymbolId, OrderflowState>>`.
//!
//! ### Hot_Path discipline
//!
//! The Orderflow_Engine is a **primary alpha source** (R2.1) and lives in
//! the Hot_Path. The crate body contains:
//!
//! * No `pyo3`, `numpy`, `pandas` (R30.4, R30.8).
//! * No `reqwest::blocking` (R30.7).
//! * No cloud LLM SDK (R30.6).
//! * No heap allocation in the steady-state book-update path (R2.6,
//!   verified via `hedge_core::alloc_harness::assert_no_alloc` under the
//!   `alloc-tracking` feature).
//!
//! ### Public surface
//!
//! Top-level re-exports favour ergonomics: `use hedge_orderflow::*` brings
//! the [`OrderflowEngine`], [`OrderflowSnapshot`], [`OrderflowEvent`], and
//! [`HeatmapSnapshot`] types into scope.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod book;
pub mod engine;
pub mod events;
pub mod heatmap;
pub mod metrics;

pub use book::{LiveBook, MAX_BOOK_LEVELS};
pub use engine::{OrderflowEngine, OrderflowState};
pub use events::{
    Detector, OrderflowEvent, DEFAULT_TICK_SIZE_PAISE, LIQUIDITY_GAP_TICKS,
    SPOOF_MEDIAN_WINDOW, SPOOF_PENDING_CAPACITY, SPOOF_WINDOW_NS,
};
pub use heatmap::{HeatmapRow, HeatmapSnapshot, OrderflowHeatmap};
pub use metrics::{
    bid_ask_imbalance, liquidity_pressure, OrderflowSnapshot, RollingDelta,
    DEFAULT_ROLLING_DELTA_WINDOW_NS, LIQUIDITY_PRESSURE_DEPTH, ROLLING_DELTA_BUCKETS,
};
