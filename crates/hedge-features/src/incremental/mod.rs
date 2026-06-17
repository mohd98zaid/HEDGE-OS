//! Incremental indicator modules for the Feature_Extraction_Engine.
//!
//! Every module in this directory exposes the same trio of functions:
//!
//! ```ignore
//! pub fn update(state: &mut FeatureState, tick: &Tick);
//! pub fn compute(state: &FeatureState) -> f32;
//! pub fn is_ready(state: &FeatureState) -> bool;
//! ```
//!
//! plus, where it makes sense, a `compute_paise` variant that returns the
//! integer-domain result for indicators where preserving paise precision
//! is preferable to a `f32` cast (VWAP, ATR, EMAs).
//!
//! ## Update contract
//!
//! `update` is O(1) — it folds the current `Tick` into the state's rolling
//! buffers without recomputing from scratch (R3.3, R3.4). The function
//! must:
//!
//! 1. read only `state` and the borrowed `tick`,
//! 2. perform zero heap allocation, and
//! 3. never panic on any well-formed `Tick` (the only invalid tick is
//!    one with `ltq > 0 && ltp_paise <= 0`, which is rejected by the
//!    Market_Data_Engine before it reaches us).
//!
//! ## Compute contract
//!
//! `compute` reads only `state` and produces the indicator's primary
//! scalar in the type the design names (`f32` for normalised signals
//! such as EMA slope, momentum, realized vol, breakout pressure; `i64`
//! paise for price-domain values like VWAP, ATR, EMAs).
//!
//! ## Is_ready contract
//!
//! `is_ready` returns `true` once enough samples have been folded in to
//! make `compute` meaningful. Before that threshold is reached, the
//! engine emits the indicator's value as `0` / `0.0` (R3.4) and the
//! Signal_Engine treats the symbol as "warm-up not complete".

pub mod atr;
pub mod breakout;
pub mod candle;
pub mod compression;
pub mod ema;
pub mod liquidity;
pub mod momentum;
pub mod rolling_delta;
pub mod sweep;
pub mod volatility;
pub mod vwap;
pub mod rsi;
pub mod donchian;
pub mod adx;
