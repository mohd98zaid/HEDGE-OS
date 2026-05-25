//! `hedge-position` — the **Position_Engine** (R8).
//!
//! Live per-symbol position tracking, realised + unrealised PnL, exposure,
//! used margin, per-strategy capital allocation, and aggregate trader risk
//! state. Subscribes to `hedge.hot.fills` (Redis Streams consumer-group
//! `position_engine`) and `md.tick.<sym>` (NATS); publishes
//! `pos.update.<sym>` and `pos.risk_state` (NATS).
//!
//! ## Authority and Hot_Path discipline
//!
//! The Position_Engine is a **read-side** Hot_Path component — it never
//! issues orders and never approves anything. Its outputs feed the
//! Risk_Engine (`pos.risk_state` is consumed by `RiskState`) and the
//! Human_Control_UI dashboard.
//!
//! All arithmetic is integer paise. No floating-point math participates in
//! position state, satisfying R3.6, R30.4, and Property 4 (Score and
//! Formula Equivalence).
//!
//! ## Module layout
//!
//! * [`pnl`] — pure VWAP and realised-PnL arithmetic.
//! * [`position`] — [`Position`] and [`StrategyAllocation`] types plus
//!   the `apply_fill` / `apply_mark` mutators.
//! * [`risk_state`] — [`TraderRiskState`] aggregator (R8.5).
//! * [`engine`] — [`PositionEngine`] registry, throttling, and event
//!   emission.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod engine;
pub mod pnl;
pub mod position;
pub mod risk_state;

// ---- Public API re-exports ----------------------------------------------

pub use engine::{
    project_unrealized, PositionEngine, PositionEvent, PositionSnapshot,
    DEFAULT_POS_UPDATE_THROTTLE_NS,
};
pub use pnl::{apply_fill_inner, unrealized_pnl_paise, FillOutcome};
pub use position::{Position, StrategyAllocation};
pub use risk_state::{aggregate_state, TraderRiskState};
