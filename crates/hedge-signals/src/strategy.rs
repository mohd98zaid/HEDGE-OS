//! `Strategy` trait — the contract every signal-emitting strategy implements.
//!
//! The trait is `Send + Sync` so a single `Arc<dyn Strategy>` can be shared
//! across any number of evaluator tasks. In practice the `SignalEngine`
//! holds the strategies in a stable-order array on a single thread, but the
//! `Send + Sync` bound keeps the engine free to fan-out per-symbol
//! evaluation in the future without touching the trait surface.

use hedge_core::Regime;
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;

/// Implemented by every concrete strategy in [`crate::strategies`].
///
/// The engine evaluates each strategy on every feature update; each call
/// receives the latest [`FeatureSnapshot`] and a [`StrategyContext`]
/// carrying the regime, trader toggles, war-mode flag, and ancillary
/// state. Strategies emit **at most one** [`Signal`] per call (R4.2,
/// R4.4).
pub trait Strategy: Send + Sync {
    /// Stable wire identifier (R4.2). Encoded into `Signal_v1.strategy`
    /// as a `u8` via [`StrategyId::as_u8`].
    fn id(&self) -> StrategyId;

    /// Evaluate the strategy against the latest feature snapshot.
    ///
    /// Returns `Some(Signal)` when the strategy's preconditions fire, or
    /// `None` otherwise. Implementations are pure: given identical
    /// `(snap, ctx)` they MUST return the same value.
    ///
    /// `base_probability` and `confidence` MUST be in `[0.0, 1.0]` —
    /// the engine re-clamps via [`Signal::clamped`] as defence in depth.
    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal>;

    /// Whether this strategy is enabled in the given market regime.
    ///
    /// A `false` return value blocks the strategy at the regime gate
    /// (R4.6). The default implementation enables every strategy in
    /// every regime; concrete strategies override to enforce
    /// regime-specific gating.
    fn enabled_in(&self, _regime: Regime) -> bool {
        true
    }
}
