//! `hedge-signals` — Signal_Engine library.
//!
//! Emits typed `Signal_v1` events on every `feat.update.<sym>` from the
//! Feature_Extraction_Engine. Six strategies are configured (R4.1):
//!
//! * [`strategies::orb`] — Opening_Range_Breakout (09:15-09:30 IST range).
//! * [`strategies::vwap_pullback`] — VWAP_Pullback continuation.
//! * [`strategies::momentum_breakout`] — Momentum_Breakout.
//! * [`strategies::liquidity_sweep_reversal`] — Liquidity_Sweep_Reversal.
//! * [`strategies::options_oi_expansion_breakout`] —
//!   Options_OI_Expansion_Breakout (skeleton; OI cache wiring in task 24.1).
//! * [`strategies::volatility_compression_breakout`] —
//!   Volatility_Compression_Breakout.
//!
//! ### Hot_Path discipline
//!
//! * Strategies are evaluated **on every feature update** through the
//!   in-process MPSC channel between `hedge-features` and the
//!   `SignalEngine`. There is no scheduler poll (R4.4).
//! * `base_probability` and `confidence` are clamped to `[0.0, 1.0]`
//!   at the type-level boundary in [`strategies::util::build_signal`]
//!   (R4.3).
//! * Gating logic ([`gating`]) runs **before** evaluation. Each gate is
//!   a pure function so the gating decision is reproducible and testable
//!   in isolation (Property 7).
//! * No pandas, no NumPy, no Python (R3.6, R30.8).
//!
//! ### Public surface
//!
//! ```ignore
//! use hedge_signals::{SignalEngine, Strategy, StrategyId, StrategyToggles};
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod context;
pub mod engine;
pub mod gating;
pub mod strategies;
pub mod strategy;

pub use context::{NewsGates, PreviousDayMemory, SectorId, StrategyContext, StrategyToggles};
pub use engine::{
    encode_signal, evaluate_strategies, SignalEngine, SignalEngineConfig, SIGNAL_WIRE_SIZE,
};
pub use gating::{check_gates, GateOutcome, GateReason};
pub use strategies::{
    LiquiditySweepReversal, MomentumBreakout, OpeningRangeBreakout,
    OptionsOiExpansionBreakout, VolatilityCompressionBreakout, VwapPullback,
};
pub use strategy::Strategy;

// Re-export `StrategyId` from `hedge-schemas` so the rest of the workspace
// imports it from a single canonical place.
pub use hedge_schemas::strategy_id::StrategyId;
