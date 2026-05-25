//! `WarmCacheView` — read-only abstraction the Risk_Engine uses to consume
//! Warm_AI_Pipeline last-known-value scores during `Adaptive_Risk`
//! computation (R5.13, R13.5, R16.5–16.7, R17, R23, R24).
//!
//! The full WarmCache implementation lives in `hedge-warmcache` (task 44.1)
//! which is not yet built. The Risk_Engine therefore declares the **read
//! contract** here as a trait so the engine can be implemented and tested
//! in full isolation today; the future `hedge-warmcache` crate implements
//! this trait.
//!
//! ### Hot_Path discipline
//!
//! Every method on this trait MUST be:
//!
//! * **Non-blocking** — returns the most recent known value (or `None`
//!   for `trade_confidence`). Never awaits, never locks against a writer
//!   beyond an atomic load (R5.12, R17.4, R24.2 forbid blocking on the
//!   per-tick path).
//! * **Allocation-free** — no `Vec`, no `String` returned to the caller.
//! * **Bounded latency** — the design's "AI scoring fetch" budget is
//!   `< 50 µs` (design § Latency Budget Allocation).
//!
//! ### Values and ranges
//!
//! All factor values are clamped to `[0.0, 1.0]` by the Risk_Engine before
//! they participate in `Adaptive_Risk = base × m × s × t`. The trait does
//! **not** enforce the clamp — implementations are expected to publish
//! sane values, and the engine defends in depth.
//!
//! ### Staleness handling (R24.2)
//!
//! When a WarmCache entry is stale or missing the engine falls back:
//!
//! | Factor | Fallback when missing/stale |
//! |---|---|
//! | `market_stability` | `1.0` (neutral — no penalty applied) |
//! | `trade_confidence(cid)` | `signal.confidence` from `Signal_v1` |
//! | `trader_stability` | `1.0` (neutral — no penalty applied) |
//!
//! These conservative fallbacks mean a degraded Warm_AI_Pipeline never
//! drives `Adaptive_Risk` toward zero on its own; the engine still applies
//! its hard limits afterwards.

use hedge_core::CorrelationId;

/// Read-only view of the Warm_AI_Pipeline last-known-value cache.
///
/// `Send + Sync` because the Risk_Engine holds the cache behind an `Arc`
/// shared across the (single) evaluator task and any future structured
/// reload tasks.
pub trait WarmCacheView: Send + Sync {
    /// `MarketStability ∈ [0.0, 1.0]` — driven by the
    /// Market_Regime_Engine via `ai.regime.changed` (R13.5).
    ///
    /// Returns `1.0` when no entry has been published yet (neutral).
    fn market_stability(&self) -> f32;

    /// `Trade_Confidence_Score ∈ [0.0, 1.0]` for a specific signal —
    /// keyed on its `correlation_id` (R17.3). Returns `None` when the AI
    /// ranking has not been published yet **or** the entry is older than
    /// the configured staleness window. The Risk_Engine then falls back
    /// to the `Signal_v1.confidence` field per design (R24.2).
    fn trade_confidence(&self, cid: CorrelationId) -> Option<f32>;

    /// `Trader_Stability_Score ∈ [0.0, 1.0]` — emitted by the
    /// Trader_Psychology_Engine on `ai.psych.stability` (R16.3, R25).
    ///
    /// Returns `1.0` when no entry has been published yet (neutral).
    fn trader_stability(&self) -> f32;
}

/// In-memory mock for unit tests. Stores the three factors plus an
/// optional `trade_confidence` keyed by `CorrelationId`.
///
/// `parking_lot::Mutex` is used internally so the mock satisfies
/// `Send + Sync` even with interior mutability — unit tests sometimes
/// flip values mid-evaluation to exercise edge cases.
pub struct MockWarmCacheView {
    inner: parking_lot::Mutex<MockState>,
}

struct MockState {
    market_stability: f32,
    trader_stability: f32,
    trade_confidence: std::collections::HashMap<u128, f32>,
    /// When `true`, every `trade_confidence` lookup returns `None`,
    /// simulating a fully degraded Warm_AI_Pipeline.
    confidence_stale: bool,
}

impl MockWarmCacheView {
    /// Construct a mock pre-populated with neutral (1.0) factors and an
    /// empty confidence map. By default `trade_confidence` returns `None`
    /// so the engine falls back to `Signal_v1.confidence`.
    pub fn neutral() -> Self {
        Self {
            inner: parking_lot::Mutex::new(MockState {
                market_stability: 1.0,
                trader_stability: 1.0,
                trade_confidence: std::collections::HashMap::new(),
                confidence_stale: false,
            }),
        }
    }

    /// Construct a mock that returns the given fixed values.
    pub fn with_values(market_stability: f32, trader_stability: f32) -> Self {
        let m = Self::neutral();
        m.set_market_stability(market_stability);
        m.set_trader_stability(trader_stability);
        m
    }

    /// Override the published `market_stability` value.
    pub fn set_market_stability(&self, v: f32) {
        self.inner.lock().market_stability = v;
    }

    /// Override the published `trader_stability` value.
    pub fn set_trader_stability(&self, v: f32) {
        self.inner.lock().trader_stability = v;
    }

    /// Publish a `trade_confidence` entry for a specific correlation.
    pub fn set_trade_confidence(&self, cid: CorrelationId, v: f32) {
        self.inner.lock().trade_confidence.insert(cid.as_u128(), v);
    }

    /// Mark the confidence map as stale — every lookup returns `None`.
    pub fn set_confidence_stale(&self, stale: bool) {
        self.inner.lock().confidence_stale = stale;
    }
}

impl Default for MockWarmCacheView {
    fn default() -> Self {
        Self::neutral()
    }
}

impl WarmCacheView for MockWarmCacheView {
    #[inline]
    fn market_stability(&self) -> f32 {
        self.inner.lock().market_stability
    }

    #[inline]
    fn trade_confidence(&self, cid: CorrelationId) -> Option<f32> {
        let g = self.inner.lock();
        if g.confidence_stale {
            return None;
        }
        g.trade_confidence.get(&cid.as_u128()).copied()
    }

    #[inline]
    fn trader_stability(&self) -> f32 {
        self.inner.lock().trader_stability
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_mock_returns_one_for_market_and_trader_stability() {
        let m = MockWarmCacheView::neutral();
        assert!((m.market_stability() - 1.0).abs() < f32::EPSILON);
        assert!((m.trader_stability() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn neutral_mock_returns_none_for_unset_correlation() {
        let m = MockWarmCacheView::neutral();
        assert_eq!(m.trade_confidence(CorrelationId::new()), None);
    }

    #[test]
    fn set_trade_confidence_round_trip() {
        let m = MockWarmCacheView::neutral();
        let cid = CorrelationId::new();
        m.set_trade_confidence(cid, 0.75);
        assert_eq!(m.trade_confidence(cid), Some(0.75));
        // Different correlation ids do not collide.
        assert_eq!(m.trade_confidence(CorrelationId::new()), None);
    }

    #[test]
    fn set_confidence_stale_masks_previously_set_values() {
        let m = MockWarmCacheView::neutral();
        let cid = CorrelationId::new();
        m.set_trade_confidence(cid, 0.5);
        m.set_confidence_stale(true);
        assert_eq!(m.trade_confidence(cid), None);
        m.set_confidence_stale(false);
        assert_eq!(m.trade_confidence(cid), Some(0.5));
    }

    #[test]
    fn with_values_sets_both_factors() {
        let m = MockWarmCacheView::with_values(0.4, 0.6);
        assert!((m.market_stability() - 0.4).abs() < f32::EPSILON);
        assert!((m.trader_stability() - 0.6).abs() < f32::EPSILON);
    }
}
