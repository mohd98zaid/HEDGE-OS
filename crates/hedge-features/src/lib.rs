//! `hedge-features` — Feature_Extraction_Engine library.
//!
//! Computes incremental, per-symbol technical features on each tick:
//! VWAP, ATR(14), EMA(9)/EMA(21)/EMA slope, realized volatility,
//! momentum, rolling delta, liquidity imbalance, orderflow strength,
//! candle structure, breakout pressure, compression zone, and
//! liquidity sweep (R3.1 – R3.6).
//!
//! ### Hot_Path discipline
//!
//! * **No `pandas`. No `numpy`. No Python runtime.** All math is on
//!   `i64`, `i128`, `f32`, and `f64` primitives (R3.6, R30.8).
//! * **Every increment is O(1).** No window is recomputed from scratch
//!   (R3.4).
//! * **Every buffer is inline.** Indicator state is stored as
//!   `RingWindow<T, N>` from `hedge-core`; the steady-state hot loop
//!   performs zero heap allocations (verified by the
//!   `assert_no_alloc` harness in tests).
//! * **3 ms p99 budget.** The engine emits a per-stage latency record
//!   on `obs.latency.FeatureExtraction` and a `obs.budget.breach.*`
//!   event when the compute window exceeds the budget (R28.2).
//!
//! ### Public surface
//!
//! ```ignore
//! use hedge_features::{FeatureExtractionEngine, FEATURE_EXTRACTION_BUDGET_NS};
//! use hedge_obs::tracer::NoopEmitter;
//! use std::sync::Arc;
//!
//! let engine = FeatureExtractionEngine::new(nats_client, Arc::new(NoopEmitter));
//! let snap = engine.process_tick(&tick);  // synchronous compute
//! engine.ingest_tick(&tick).await?;       // compute + publish + latency
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod engine;
pub mod incremental;
pub mod state;

#[cfg(test)]
pub(crate) mod tests;

pub use engine::{
    encode, process_tick_into_state, FeatureExtractionEngine, FlatBuffersCodecBridge,
    RawFeaturePayload, FEATURE_EXTRACTION_BUDGET_NS, FEATURE_WIRE_SIZE,
};
pub use incremental::candle::CandleStructure;
pub use state::{
    DeltaSample, FeatureState, LastBook, ATR_WINDOW, COMPRESSION_WINDOW, EMA_FAST_PERIOD,
    EMA_SLOPE_LOOKBACK, EMA_SLOW_PERIOD, MOMENTUM_WINDOW, ROLLING_DELTA_CAPACITY,
    ROLLING_DELTA_WINDOW_NS, SWEEP_LOOKAHEAD, VOLATILITY_WINDOW,
};
