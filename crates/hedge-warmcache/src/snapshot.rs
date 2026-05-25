//! Atomic-load snapshot of the Warm_AI_Pipeline last-known values.
//!
//! [`Snapshot`] is the immutable payload that `arc_swap::ArcSwap` swaps in
//! and out atomically. Every Hot_Path read goes through [`WarmCache::load`]
//! (a single relaxed pointer load) and reads fields off the resulting
//! `Arc<Snapshot>` directly — no hashing, no locking, no allocation
//! (R9.4, R17.4, R19.7).
//!
//! Per-correlation_id `trade_confidence` lookups are NOT stored on the
//! snapshot itself: that map is large (default 8 192 entries) and is held
//! out-of-band in a [`dashmap::DashMap`] so the read path does an O(1)
//! sharded lookup without copying the whole map on every update. See
//! `lru.rs`.
//!
//! The snapshot owns:
//!
//! * Two scalar factor values (`market_stability`, `trader_stability`)
//!   defaulting to `1.0` (neutral).
//! * A bounded `SmallVec` of per-symbol `(SymbolId, Priority)` pairs and
//!   a parallel `(SymbolId, NewsImpactSnapshot)` table. Bounds match the
//!   design's universe size (≤ 256 active symbols on a ₹20 000 base) so
//!   iteration is cache-friendly and read does not allocate.
//! * Capture timestamps so callers can apply staleness windows uniformly.
//!
//! Updates always rebuild a new `Snapshot` and `ArcSwap::store` it. The
//! cost is one allocation per update — but updates fire on the
//! Warm_AI_Pipeline cadence (events per second), not the per-tick cadence
//! (events per millisecond). The Hot_Path side never allocates.

use hedge_core::{Priority, SymbolId};
use smallvec::SmallVec;

/// Maximum number of distinct symbols whose priority and news_impact we
/// track in the inline snapshot. The design budgets 256 active symbols on
/// the default ₹20 000 base; we round up to 512 to leave headroom for
/// universe expansion without re-allocation on the read path.
pub const MAX_TRACKED_SYMBOLS: usize = 512;

/// Per-symbol news impact value object, mirroring the
/// `ai.news.impact.<sym>` JSON schema fields the Risk_Engine consumes.
///
/// Layout is `#[repr(C)]` so the struct can be copied off the snapshot
/// without padding surprises on the Hot_Path. The two fields are clamped
/// to their schema-declared ranges by [`NewsImpactSnapshot::clamp`]:
/// `sentiment ∈ [-1.0, 1.0]`, `impact_magnitude ∈ [0.0, 1.0]`.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Default)]
pub struct NewsImpactSnapshot {
    /// Polarity in `[-1.0, 1.0]`. `0.0` is neutral (no entry yet).
    pub sentiment: f32,
    /// Magnitude in `[0.0, 1.0]`. `0.0` is the "no news" default.
    pub impact_magnitude: f32,
    /// Capture timestamp (`hedge_core::now_ns()`) the entry was written.
    /// `0` means "never written" — the Risk_Engine treats this as no
    /// entry and applies the neutral default.
    pub ts_ns: u64,
}

impl NewsImpactSnapshot {
    /// Clamp `sentiment` and `impact_magnitude` into their schema-declared
    /// ranges. Defence in depth — the JSON schema already enforces these
    /// bounds at decode time.
    #[inline]
    pub fn clamp(self) -> Self {
        Self {
            sentiment: clamp(self.sentiment, -1.0, 1.0),
            impact_magnitude: clamp(self.impact_magnitude, 0.0, 1.0),
            ts_ns: self.ts_ns,
        }
    }
}

#[inline]
fn clamp(v: f32, lo: f32, hi: f32) -> f32 {
    if v.is_nan() {
        lo
    } else if v < lo {
        lo
    } else if v > hi {
        hi
    } else {
        v
    }
}

/// Immutable snapshot of every WarmCache value the Hot_Path reads.
///
/// Stored behind `arc_swap::ArcSwap<Snapshot>` so reads are a single
/// atomic pointer load. Updates copy-on-write a new `Snapshot` and
/// publish it via `ArcSwap::store`.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// `MarketStability ∈ [0.0, 1.0]` — most recent value published on
    /// `ai.regime.changed`. Default `1.0` (neutral).
    pub(crate) market_stability: f32,
    /// Capture timestamp for `market_stability` (`hedge_core::now_ns()`).
    /// `0` means "never written".
    pub(crate) market_stability_ts_ns: u64,

    /// `Trader_Stability_Score ∈ [0.0, 1.0]` — most recent value published
    /// on `ai.psych.stability`. Default `1.0` (neutral).
    pub(crate) trader_stability: f32,
    /// Capture timestamp for `trader_stability`.
    pub(crate) trader_stability_ts_ns: u64,

    /// Per-symbol priority tier, last-known value from
    /// `ai.priority.changed.<sym>`. Default `Priority::P3` for any
    /// symbol that has never been updated. Bounded `SmallVec` keeps the
    /// snapshot heap-free for small universes.
    pub(crate) priority: SmallVec<[(SymbolId, Priority); 64]>,

    /// Per-symbol last-known news impact from `ai.news.impact.<sym>`.
    pub(crate) news_impact: SmallVec<[(SymbolId, NewsImpactSnapshot); 64]>,
}

impl Snapshot {
    /// Construct a neutral snapshot with no per-symbol entries.
    /// `market_stability` and `trader_stability` default to `1.0`
    /// (neutral) so a cold WarmCache never multiplies `Adaptive_Risk`
    /// down to zero on its own (R5.13, R24.2).
    pub fn neutral() -> Self {
        Self {
            market_stability: 1.0,
            market_stability_ts_ns: 0,
            trader_stability: 1.0,
            trader_stability_ts_ns: 0,
            priority: SmallVec::new(),
            news_impact: SmallVec::new(),
        }
    }

    /// O(n) lookup of the last-known priority for `sym`. Returns
    /// `Priority::P3` when `sym` has never been published. Linear scan
    /// is preferred over a hashmap because the per-symbol universe is
    /// small (`MAX_TRACKED_SYMBOLS` ≤ 512) and the inline `SmallVec`
    /// keeps the data in one cache line per few entries — significantly
    /// cheaper than a `HashMap` indirection on the Hot_Path.
    #[inline]
    pub fn priority(&self, sym: SymbolId) -> Priority {
        for (s, p) in &self.priority {
            if *s == sym {
                return *p;
            }
        }
        Priority::P3
    }

    /// O(n) lookup of the last-known news impact for `sym`. Returns the
    /// zero / neutral default when `sym` has never been published.
    #[inline]
    pub fn news_impact(&self, sym: SymbolId) -> NewsImpactSnapshot {
        for (s, n) in &self.news_impact {
            if *s == sym {
                return *n;
            }
        }
        NewsImpactSnapshot::default()
    }

    /// Build a new snapshot with `market_stability` updated. Used by the
    /// `WarmCacheUpdater` task.
    pub(crate) fn with_market_stability(&self, value: f32, ts_ns: u64) -> Self {
        let mut out = self.clone();
        out.market_stability = clamp(value, 0.0, 1.0);
        out.market_stability_ts_ns = ts_ns;
        out
    }

    /// Build a new snapshot with `trader_stability` updated.
    pub(crate) fn with_trader_stability(&self, value: f32, ts_ns: u64) -> Self {
        let mut out = self.clone();
        out.trader_stability = clamp(value, 0.0, 1.0);
        out.trader_stability_ts_ns = ts_ns;
        out
    }

    /// Build a new snapshot with `sym`'s priority set to `tier`.
    /// If the entry already exists it is overwritten; otherwise it is
    /// appended unless the inline budget is exhausted, in which case the
    /// oldest entry is replaced.
    pub(crate) fn with_priority(&self, sym: SymbolId, tier: Priority) -> Self {
        let mut out = self.clone();
        if let Some(slot) = out.priority.iter_mut().find(|(s, _)| *s == sym) {
            slot.1 = tier;
        } else if out.priority.len() < MAX_TRACKED_SYMBOLS {
            out.priority.push((sym, tier));
        } else {
            // Bounded eviction: replace the first entry. With a 512-symbol
            // budget against a ~256-symbol universe this branch should
            // never fire in production; we keep the cache full rather
            // than dropping the update.
            out.priority[0] = (sym, tier);
        }
        out
    }

    /// Build a new snapshot with `sym`'s news_impact updated.
    pub(crate) fn with_news_impact(&self, sym: SymbolId, value: NewsImpactSnapshot) -> Self {
        let mut out = self.clone();
        let value = value.clamp();
        if let Some(slot) = out.news_impact.iter_mut().find(|(s, _)| *s == sym) {
            slot.1 = value;
        } else if out.news_impact.len() < MAX_TRACKED_SYMBOLS {
            out.news_impact.push((sym, value));
        } else {
            out.news_impact[0] = (sym, value);
        }
        out
    }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::neutral()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_snapshot_has_one_for_factors_and_empty_tables() {
        let s = Snapshot::neutral();
        assert!((s.market_stability - 1.0).abs() < f32::EPSILON);
        assert!((s.trader_stability - 1.0).abs() < f32::EPSILON);
        assert_eq!(s.market_stability_ts_ns, 0);
        assert_eq!(s.trader_stability_ts_ns, 0);
        assert!(s.priority.is_empty());
        assert!(s.news_impact.is_empty());
    }

    #[test]
    fn priority_default_is_p3_for_unknown_symbol() {
        let s = Snapshot::neutral();
        assert_eq!(s.priority(SymbolId::new(7)), Priority::P3);
    }

    #[test]
    fn news_impact_default_is_zero_for_unknown_symbol() {
        let s = Snapshot::neutral();
        let n = s.news_impact(SymbolId::new(11));
        assert_eq!(n.sentiment, 0.0);
        assert_eq!(n.impact_magnitude, 0.0);
        assert_eq!(n.ts_ns, 0);
    }

    #[test]
    fn with_market_stability_clamps_into_unit_interval() {
        let s = Snapshot::neutral().with_market_stability(2.5, 100);
        assert!((s.market_stability - 1.0).abs() < f32::EPSILON);
        let s2 = Snapshot::neutral().with_market_stability(-1.0, 100);
        assert_eq!(s2.market_stability, 0.0);
    }

    #[test]
    fn with_priority_overwrites_existing_entry() {
        let s = Snapshot::neutral()
            .with_priority(SymbolId::new(1), Priority::P1)
            .with_priority(SymbolId::new(1), Priority::P4);
        assert_eq!(s.priority.len(), 1);
        assert_eq!(s.priority(SymbolId::new(1)), Priority::P4);
    }

    #[test]
    fn with_news_impact_clamps_to_schema_ranges() {
        let s = Snapshot::neutral().with_news_impact(
            SymbolId::new(2),
            NewsImpactSnapshot {
                sentiment: 5.0,
                impact_magnitude: -1.0,
                ts_ns: 42,
            },
        );
        let n = s.news_impact(SymbolId::new(2));
        assert!((n.sentiment - 1.0).abs() < f32::EPSILON);
        assert_eq!(n.impact_magnitude, 0.0);
        assert_eq!(n.ts_ns, 42);
    }

    #[test]
    fn news_impact_snapshot_clamp_handles_nan() {
        let n = NewsImpactSnapshot {
            sentiment: f32::NAN,
            impact_magnitude: f32::NAN,
            ts_ns: 0,
        }
        .clamp();
        assert_eq!(n.sentiment, -1.0);
        assert_eq!(n.impact_magnitude, 0.0);
    }
}
