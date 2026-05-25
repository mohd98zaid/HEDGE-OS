//! Bounded last-known-value map for `trade_confidence(correlation_id)`.
//!
//! Backed by a sharded [`dashmap::DashMap`] so the Hot_Path read path
//! takes a single shard lock (which is uncontended in steady state — one
//! Risk_Engine task does the reads, one updater task does the writes)
//! and never allocates. The dashmap shard count is the default (number
//! of CPU cores), which is what `hedge-features` and `hedge-position`
//! also use.
//!
//! Capacity bound is enforced by tracking insertion order in a separate
//! `parking_lot::Mutex<VecDeque<u128>>`. The dequeue is only touched on
//! **insert** (write side) — the read side never observes it. When the
//! dequeue grows beyond the configured `lru_size`, we evict the oldest
//! correlation_id from the dashmap. The semantics are FIFO rather than
//! true LRU, but that is what the design wants: stale rankings should
//! drop out the back, and callers always look up by the same
//! correlation_id within a few milliseconds of receiving the
//! `ai.rank.<cid>` event so true recency is irrelevant.
//!
//! Stale entries are filtered at lookup time: an entry whose age (now —
//! `ts_ns`) exceeds the configured staleness window is reported as
//! `None` so the Risk_Engine falls back to `Signal_v1.confidence`
//! (R24.2; design § Components § Risk_Engine).

use std::collections::VecDeque;
use std::num::NonZeroUsize;

use dashmap::DashMap;
use hedge_core::CorrelationId;
use parking_lot::Mutex;

/// Single entry in the trade_confidence LRU.
#[derive(Copy, Clone, Debug)]
struct Entry {
    /// `Trade_Confidence_Score ∈ [0.0, 1.0]` from `ai.rank.<cid>`.
    confidence: f32,
    /// `hedge_core::now_ns()` at insert time. Used to apply the
    /// staleness window at lookup.
    ts_ns: u64,
}

/// Bounded `correlation_id → trade_confidence` cache.
pub struct ConfidenceLru {
    /// Sharded map keyed on `CorrelationId.as_u128()`.
    map: DashMap<u128, Entry>,
    /// FIFO order ring used to enforce the size bound. Only touched by
    /// the writer (the `WarmCacheUpdater` task), so contention is nil.
    order: Mutex<VecDeque<u128>>,
    /// Maximum number of entries. Construction asserts non-zero.
    capacity: NonZeroUsize,
    /// Staleness window in nanoseconds. Lookups older than this return
    /// `None`. `0` disables the staleness filter.
    staleness_ns: u64,
}

impl ConfidenceLru {
    /// Construct an empty cache bounded to `capacity` entries.
    /// `capacity == 0` is rejected (use a positive bound).
    pub fn new(capacity: usize, staleness_ns: u64) -> Self {
        let capacity = NonZeroUsize::new(capacity).expect("ConfidenceLru capacity must be > 0");
        Self {
            map: DashMap::with_capacity(capacity.get()),
            order: Mutex::new(VecDeque::with_capacity(capacity.get())),
            capacity,
            staleness_ns,
        }
    }

    /// Number of entries currently held.
    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the cache is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Insert (or overwrite) the entry for `cid`. If insertion would
    /// exceed `capacity`, the oldest correlation_id is evicted.
    ///
    /// `now_ns` is taken as a parameter rather than read inside the
    /// function so the caller (the updater task) controls the timestamp
    /// — useful for replay rigs and unit tests.
    pub fn insert(&self, cid: CorrelationId, confidence: f32, now_ns: u64) {
        let key = cid.as_u128();
        let mut order = self.order.lock();
        let was_present = self.map.insert(key, Entry { confidence, ts_ns: now_ns }).is_some();
        if !was_present {
            order.push_back(key);
            while order.len() > self.capacity.get() {
                if let Some(evict) = order.pop_front() {
                    self.map.remove(&evict);
                } else {
                    break;
                }
            }
        }
        // When `was_present` is true the entry was overwritten in place;
        // we deliberately do not move it to the back of the FIFO. That
        // would matter for true LRU semantics; for the WarmCache use case
        // (single observed cid per signal lifecycle) it never matters.
    }

    /// Look up the most recent confidence for `cid`. Returns `None` when
    /// the entry is missing or older than the configured staleness
    /// window. The read path is a single sharded `DashMap::get` — no
    /// allocation.
    #[inline]
    pub fn get(&self, cid: CorrelationId, now_ns: u64) -> Option<f32> {
        let key = cid.as_u128();
        let entry = self.map.get(&key)?;
        if self.staleness_ns > 0 {
            let age = now_ns.saturating_sub(entry.ts_ns);
            if age > self.staleness_ns {
                return None;
            }
        }
        Some(entry.confidence)
    }

    /// Configured capacity bound.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity.get()
    }

    /// Configured staleness window in nanoseconds. `0` means disabled.
    #[inline]
    pub fn staleness_ns(&self) -> u64 {
        self.staleness_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_core::CorrelationId;

    #[test]
    fn insert_and_get_round_trips() {
        let lru = ConfidenceLru::new(8, 0);
        let cid = CorrelationId::new();
        lru.insert(cid, 0.75, 100);
        assert_eq!(lru.get(cid, 200), Some(0.75));
    }

    #[test]
    fn get_returns_none_for_unknown_cid() {
        let lru = ConfidenceLru::new(8, 0);
        let cid = CorrelationId::new();
        assert_eq!(lru.get(cid, 0), None);
    }

    #[test]
    fn capacity_bound_evicts_oldest_in_fifo_order() {
        let lru = ConfidenceLru::new(2, 0);
        let a = CorrelationId(1);
        let b = CorrelationId(2);
        let c = CorrelationId(3);
        lru.insert(a, 0.1, 100);
        lru.insert(b, 0.2, 100);
        lru.insert(c, 0.3, 100); // evicts a
        assert_eq!(lru.len(), 2);
        assert_eq!(lru.get(a, 100), None);
        assert_eq!(lru.get(b, 100), Some(0.2));
        assert_eq!(lru.get(c, 100), Some(0.3));
    }

    #[test]
    fn overwriting_existing_cid_does_not_grow_or_evict() {
        let lru = ConfidenceLru::new(2, 0);
        let a = CorrelationId(1);
        let b = CorrelationId(2);
        lru.insert(a, 0.1, 100);
        lru.insert(b, 0.2, 100);
        // Overwrite a; capacity is still 2 so b must survive.
        lru.insert(a, 0.9, 100);
        assert_eq!(lru.len(), 2);
        assert_eq!(lru.get(a, 100), Some(0.9));
        assert_eq!(lru.get(b, 100), Some(0.2));
    }

    #[test]
    fn staleness_window_filters_old_entries() {
        let lru = ConfidenceLru::new(8, 1_000_000); // 1 ms in nanoseconds
        let cid = CorrelationId::new();
        lru.insert(cid, 0.5, 0);
        // Within the window — visible.
        assert_eq!(lru.get(cid, 500_000), Some(0.5));
        // Past the window — masked out.
        assert_eq!(lru.get(cid, 5_000_000), None);
    }

    #[test]
    fn zero_staleness_means_no_filter() {
        let lru = ConfidenceLru::new(8, 0);
        let cid = CorrelationId::new();
        lru.insert(cid, 0.5, 0);
        // Even after a "long" time the entry is visible because staleness=0
        // disables the filter.
        assert_eq!(lru.get(cid, u64::MAX / 2), Some(0.5));
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn zero_capacity_panics() {
        let _ = ConfidenceLru::new(0, 0);
    }
}
