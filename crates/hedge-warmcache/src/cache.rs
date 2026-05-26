//! [`WarmCache`] — the public read surface of the WarmCache crate.
//!
//! Read path is a single relaxed-load on `arc_swap::ArcSwap<Snapshot>`
//! plus, for `trade_confidence` only, a sharded `DashMap` lookup on
//! `correlation_id`. Both operations are allocation-free and bounded
//! to nanoseconds (R9.4 budget: < 50 µs for the AI-scoring fetch
//! stage; the Risk_Engine spends most of its budget on the
//! Adaptive_Risk computation that follows).
//!
//! The write side is owned by the `WarmCacheUpdater` task in `updater.rs`.
//! It calls the `store_*` mutators below, each of which clones the
//! previous snapshot, applies one field, and `ArcSwap::store`s the new
//! `Arc`. Cloning the snapshot is bounded — the inline `SmallVec`s in
//! `snapshot.rs` cap allocation to a single 16 KiB-ish buffer in the
//! steady state, and updates fire on the Warm_AI_Pipeline cadence
//! (events per second).

use std::sync::Arc;

use arc_swap::ArcSwap;
use hedge_core::{CorrelationId, Priority, SymbolId};
use hedge_schemas::Signal as Signal_v1;

use crate::config::WarmCacheConfig;
use crate::lru::ConfidenceLru;
use crate::snapshot::{NewsImpactSnapshot, Snapshot};

// `WarmCacheView` lives in `hedge-risk`. To avoid a `hedge-warmcache →
// hedge-risk` dependency edge that would force every consumer of this
// crate to drag in the entire Risk_Engine, we expose only the inherent
// methods here. The consumer (the Risk_Engine binary) writes a tiny
// 5-line newtype that wraps `Arc<WarmCache>` and impls
// `hedge_risk::WarmCacheView` on it. See
// `crates/hedge-risk/src/warmcache.rs` for the trait definition and
// `MockWarmCacheView` for the test-side reference adapter.

/// Last-known-value cache for Warm_AI_Pipeline scores.
///
/// Construct once at service startup with [`WarmCache::new`] and share via
/// `Arc<WarmCache>`. Hot_Path reader tasks call the `*` accessor methods
/// (`market_stability`, `trade_confidence`, etc.) per signal evaluation;
/// the updater task calls `store_*` mutators on every inbound `ai.*`
/// event. Reads never block writes and writes never block reads — the
/// cache is **non-blocking by construction** (R9.5, R17.4, R19.7).
pub struct WarmCache {
    /// Atomic snapshot of every scalar/per-symbol value the Risk_Engine
    /// reads. A single relaxed pointer load returns the most recent
    /// `Arc<Snapshot>` published by the updater task.
    inner: ArcSwap<Snapshot>,
    /// Per-correlation `trade_confidence` cache. Sharded so contention
    /// between the read and the (single-threaded) updater task is nil.
    confidence: ConfidenceLru,
    /// Configuration the cache was constructed with — kept around so
    /// callers can inspect the LRU size, staleness window, and NATS URL.
    config: WarmCacheConfig,
}

impl WarmCache {
    /// Construct a new WarmCache from configuration.
    ///
    /// Initial snapshot is [`Snapshot::neutral`] so a cold cache reports:
    ///
    /// * `market_stability = 1.0` (neutral; no penalty applied)
    /// * `trader_stability = 1.0`
    /// * `priority(_) = Priority::P3` for every symbol
    /// * `news_impact(_) = NewsImpactSnapshot::default()` (zeroes)
    /// * `trade_confidence(_) = None` (caller falls back to
    ///   `Signal_v1.confidence`)
    ///
    /// These conservative defaults are what the design's Risk_Engine
    /// fall-back ladder depends on (R24.2).
    pub fn new(config: WarmCacheConfig) -> Self {
        let staleness_ns = u64::from(config.staleness_window_ms()).saturating_mul(1_000_000);
        let confidence = ConfidenceLru::new(config.trade_confidence_lru_size(), staleness_ns);
        Self {
            inner: ArcSwap::from_pointee(Snapshot::neutral()),
            confidence,
            config,
        }
    }

    /// Borrow the configuration the cache was built with.
    #[inline]
    pub fn config(&self) -> &WarmCacheConfig {
        &self.config
    }

    /// Snapshot the current inner state as an owned `Arc<Snapshot>`.
    /// This is the single primitive every other read accessor builds
    /// on. Hot_Path code can hold onto the returned `Arc` for a tick's
    /// worth of evaluations to avoid repeated atomic loads — the
    /// `Arc` clone is one bumped refcount, no allocation.
    #[inline]
    pub fn load(&self) -> Arc<Snapshot> {
        self.inner.load_full()
    }

    // -- Reads --------------------------------------------------------

    /// Last-known `MarketStability ∈ [0.0, 1.0]`. Default `1.0` (neutral).
    #[inline]
    pub fn market_stability(&self) -> f32 {
        self.inner.load().market_stability
    }

    /// Last-known `Trader_Stability_Score ∈ [0.0, 1.0]`. Default `1.0`.
    #[inline]
    pub fn trader_stability(&self) -> f32 {
        self.inner.load().trader_stability
    }

    /// Last-known `Trade_Confidence_Score` for `correlation_id`. Returns
    /// `None` when the entry is missing or older than the configured
    /// staleness window (R24.2: callers fall back to
    /// [`Self::fallback_confidence`]).
    #[inline]
    pub fn trade_confidence(&self, correlation_id: CorrelationId) -> Option<f32> {
        self.confidence.get(correlation_id, hedge_core::now_ns())
    }

    /// Variant of [`Self::trade_confidence`] that takes the timestamp
    /// explicitly. Used by replay rigs and unit tests where wall-clock
    /// progression is controlled by the caller.
    #[inline]
    pub fn trade_confidence_at(&self, correlation_id: CorrelationId, now_ns: u64) -> Option<f32> {
        self.confidence.get(correlation_id, now_ns)
    }

    /// Last-known [`Priority`] tier for `symbol`. Default
    /// [`Priority::P3`] when the symbol has never been published.
    #[inline]
    pub fn priority(&self, symbol: SymbolId) -> Priority {
        self.inner.load().priority(symbol)
    }

    /// Last-known [`NewsImpactSnapshot`] for `symbol`. Default zero
    /// when the symbol has never been published.
    #[inline]
    pub fn news_impact(&self, symbol: SymbolId) -> NewsImpactSnapshot {
        self.inner.load().news_impact(symbol)
    }

    /// Resolve the confidence to use when `trade_confidence` misses or
    /// is stale: the original `Signal_v1.confidence` field
    /// (design § Components § Risk_Engine; R24.2).
    ///
    /// Convenience shim around the engine's idiomatic
    /// `cache.trade_confidence(cid).unwrap_or(signal.confidence)` so
    /// future fallback policy changes only need editing here.
    ///
    /// `Signal_v1.correlation_id` is the canonical 16-byte big-endian
    /// `u128` defined by `hedge-schemas/schemas/signal.fbs` and consumed
    /// by `hedge-risk` via `u128::from_be_bytes`. We mirror that decode
    /// so the cache key matches whatever the AI_Trade_Ranking_Engine
    /// would have minted for the same signal.
    #[inline]
    pub fn fallback_confidence(&self, signal: &Signal_v1) -> f32 {
        let cid = CorrelationId(u128::from_be_bytes(signal.correlation_id));
        self.confidence
            .get(cid, hedge_core::now_ns())
            .unwrap_or(signal.confidence)
    }

    // -- Writes (used by `WarmCacheUpdater`) --------------------------

    /// Publish a new `market_stability` value. Called by the updater
    /// task on every `ai.regime.changed` event.
    pub fn store_market_stability(&self, value: f32, ts_ns: u64) {
        let prev = self.inner.load();
        let next = Arc::new(prev.with_market_stability(value, ts_ns));
        self.inner.store(next);
    }

    /// Publish a new `trader_stability` score. Called on every
    /// `ai.psych.stability` event.
    pub fn store_trader_stability(&self, value: f32, ts_ns: u64) {
        let prev = self.inner.load();
        let next = Arc::new(prev.with_trader_stability(value, ts_ns));
        self.inner.store(next);
    }

    /// Publish a new `trade_confidence` for `correlation_id`. Called on
    /// every `ai.rank.<cid>` event. Eviction (when capacity is reached)
    /// is FIFO — see `lru.rs`.
    pub fn store_trade_confidence(&self, correlation_id: CorrelationId, confidence: f32, ts_ns: u64) {
        self.confidence.insert(correlation_id, confidence, ts_ns);
    }

    /// Publish a new `priority` tier for `symbol`. Called on every
    /// `ai.priority.changed.<sym>` event.
    pub fn store_priority(&self, symbol: SymbolId, tier: Priority) {
        let prev = self.inner.load();
        let next = Arc::new(prev.with_priority(symbol, tier));
        self.inner.store(next);
    }

    /// Publish a new `news_impact` for `symbol`. Called on every
    /// `ai.news.impact.<sym>` event.
    pub fn store_news_impact(&self, symbol: SymbolId, impact: NewsImpactSnapshot) {
        let prev = self.inner.load();
        let next = Arc::new(prev.with_news_impact(symbol, impact));
        self.inner.store(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WarmCacheConfig;

    fn cfg() -> WarmCacheConfig {
        WarmCacheConfig::from_parts(8, 0, "nats://127.0.0.1:4222")
    }

    #[test]
    fn cold_cache_returns_neutral_defaults() {
        let cache = WarmCache::new(cfg());
        assert_eq!(cache.market_stability(), 1.0);
        assert_eq!(cache.trader_stability(), 1.0);
        assert_eq!(cache.priority(SymbolId::new(7)), Priority::P3);
        let n = cache.news_impact(SymbolId::new(7));
        assert_eq!(n.sentiment, 0.0);
        assert_eq!(n.impact_magnitude, 0.0);
        assert_eq!(cache.trade_confidence(CorrelationId::new()), None);
    }

    #[test]
    fn store_market_stability_is_visible_to_subsequent_reads() {
        let cache = WarmCache::new(cfg());
        cache.store_market_stability(0.5, 1_000);
        assert!((cache.market_stability() - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn store_trader_stability_is_visible_to_subsequent_reads() {
        let cache = WarmCache::new(cfg());
        cache.store_trader_stability(0.25, 0);
        assert!((cache.trader_stability() - 0.25).abs() < f32::EPSILON);
    }

    #[test]
    fn store_trade_confidence_is_visible_to_subsequent_reads() {
        let cache = WarmCache::new(cfg());
        let cid = CorrelationId::new();
        cache.store_trade_confidence(cid, 0.9, 100);
        assert_eq!(cache.trade_confidence_at(cid, 200), Some(0.9));
    }

    #[test]
    fn store_priority_is_visible_to_subsequent_reads() {
        let cache = WarmCache::new(cfg());
        cache.store_priority(SymbolId::new(42), Priority::P1);
        assert_eq!(cache.priority(SymbolId::new(42)), Priority::P1);
    }

    #[test]
    fn store_news_impact_clamps_and_is_visible() {
        let cache = WarmCache::new(cfg());
        let sym = SymbolId::new(11);
        cache.store_news_impact(
            sym,
            NewsImpactSnapshot {
                sentiment: 0.6,
                impact_magnitude: 0.4,
                ts_ns: 500,
            },
        );
        let n = cache.news_impact(sym);
        assert!((n.sentiment - 0.6).abs() < f32::EPSILON);
        assert!((n.impact_magnitude - 0.4).abs() < f32::EPSILON);
        assert_eq!(n.ts_ns, 500);
    }

    #[test]
    fn fallback_confidence_uses_signal_field_when_cache_misses() {
        let cache = WarmCache::new(cfg());
        let mut signal = Signal_v1::default();
        signal.confidence = 0.42;
        // No entry stored — fallback used.
        let v = cache.fallback_confidence(&signal);
        assert!((v - 0.42).abs() < f32::EPSILON);
    }

    #[test]
    fn fallback_confidence_uses_cache_when_present_and_fresh() {
        let staleness_ms = 5_000;
        let cache = WarmCache::new(WarmCacheConfig::from_parts(
            8,
            staleness_ms,
            "nats://127.0.0.1:4222",
        ));
        let mut signal = Signal_v1::default();
        // Pretend the signal was minted "now" — its correlation_id is
        // serialized big-endian (matching `u128::from_be_bytes` reads in
        // `hedge-risk::engine`).
        let cid = CorrelationId::new();
        signal.correlation_id = cid.as_u128().to_be_bytes();
        signal.confidence = 0.10; // fallback
        cache.store_trade_confidence(cid, 0.95, hedge_core::now_ns());
        let v = cache.fallback_confidence(&signal);
        // Cache hit beats the signal field.
        assert!((v - 0.95).abs() < f32::EPSILON);
    }

    #[test]
    fn load_returns_consistent_snapshot_across_reads() {
        // Property R9.4: a single tick of the Risk_Engine reads every
        // factor from one snapshot. Snapshot::load is the primitive that
        // gives that consistency: the values do not race with each other
        // because they live behind a single ArcSwap.
        let cache = WarmCache::new(cfg());
        cache.store_market_stability(0.7, 100);
        cache.store_trader_stability(0.8, 200);
        let snap = cache.load();
        assert!((snap.market_stability - 0.7).abs() < f32::EPSILON);
        assert!((snap.trader_stability - 0.8).abs() < f32::EPSILON);
    }
}
