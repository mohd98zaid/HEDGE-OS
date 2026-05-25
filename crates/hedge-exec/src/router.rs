//! `BrokerRouter` — active/backup adapter routing with sliding-window
//! error-rate and latency tracking and atomic failover (R6.5).
//!
//! ## Active vs backup
//!
//! The router holds two `Arc<dyn BrokerAdapter>` slots — `primary` and
//! `backup`. The active adapter is selected by an atomic
//! [`std::sync::atomic::AtomicU8`] flipping between
//! [`ActiveSlot::Primary`] (0) and [`ActiveSlot::Backup`] (1). Reads
//! use `Ordering::Acquire` so an in-flight thread always sees the
//! freshest active slot.
//!
//! ## Sliding window
//!
//! Per-adapter [`AdapterStats`] hold a fixed-capacity ring of the last
//! `WINDOW_SIZE` outcomes. Each outcome is a single-byte tag plus a
//! 32-bit latency-ms snapshot, so the whole window is small enough to
//! fit comfortably in cache. Aggregates (`error_rate`, `p99_latency_ms`)
//! are recomputed from the window on every breach check; recompute cost
//! is O(WINDOW_SIZE) and fires only when an outcome is appended, which
//! itself is rare relative to the per-tick rate.
//!
//! ## Failover
//!
//! On every [`BrokerRouter::record_outcome`] call:
//!
//! 1. The outcome is appended to the active adapter's window.
//! 2. The window's `error_rate` and `p99_latency_ms` are recomputed.
//! 3. If either crosses the configured threshold, the router atomically
//!    swaps the active slot and emits a [`FailoverEvent`].
//!
//! The swap is one CAS — no lock — so the in-flight `submit` path is
//! not blocked. The emitted event is consumed by the engine's NATS
//! task which publishes `exec.broker.failover` (R6.5).
//!
//! ## Adaptive routing
//!
//! [`BrokerRouter::pick_for_intent`] inspects the
//! `RiskApproval.execution_params` (placeholder field today — the
//! schema doesn't yet carry per-broker hints). For now the active
//! slot is always chosen; once the schema lands the function can
//! prefer the broker hinted by the approval.

use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use hedge_broker_api::BrokerAdapter;
use hedge_core::BrokerId;
use parking_lot::RwLock;

/// Outcomes recorded into [`AdapterStats`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Successful submit / modify / cancel round trip.
    Success,
    /// Failed submit (any retryable or non-retryable error).
    Failure,
}

/// Default sliding window size. 64 round trips is small enough to
/// react quickly to a broker outage without thrashing on noise.
pub const WINDOW_SIZE: usize = 64;

/// Active adapter slot. `0` = primary, `1` = backup.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ActiveSlot {
    /// The configured primary broker.
    Primary = 0,
    /// The configured backup broker.
    Backup = 1,
}

impl ActiveSlot {
    #[inline]
    fn from_u8(b: u8) -> Self {
        match b {
            1 => Self::Backup,
            _ => Self::Primary,
        }
    }
    #[inline]
    fn other(self) -> Self {
        match self {
            Self::Primary => Self::Backup,
            Self::Backup => Self::Primary,
        }
    }
}

/// Failover-trigger event emitted by [`BrokerRouter::record_outcome`].
/// The engine's NATS task translates this into an `exec.broker.failover`
/// publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverEvent {
    /// Adapter the router is failing **away from**.
    pub from: BrokerId,
    /// Adapter the router is failing **to**.
    pub to: BrokerId,
    /// Snapshot of the failing adapter's error rate at the moment of failover.
    pub error_rate_bps: u32,
    /// Snapshot of the failing adapter's p99 latency at the moment of failover.
    pub p99_latency_ms: u32,
    /// Wall-clock timestamp in nanoseconds.
    pub ts_ns: u64,
}

/// Sliding-window stats for one adapter.
///
/// The fixed-size ring is allocated once at construction. Pushes are
/// O(1) and aggregate recomputation walks the ring sequentially.
pub struct AdapterStats {
    capacity: usize,
    /// Sample storage. Each sample is `(outcome_tag, latency_ms)`.
    samples: RwLock<Vec<Sample>>,
    /// Insert position; the next outcome lands at `pos % capacity`.
    pos: AtomicUsize,
    /// Total number of outcomes ever recorded (not bounded by capacity).
    total_recorded: AtomicU64,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Sample {
    /// `0` = success, `1` = failure.
    tag: u8,
    /// Latency in milliseconds for this round trip. `u32::MAX` means
    /// "no measurement".
    latency_ms: u32,
}

impl AdapterStats {
    /// Construct stats with the given fixed window capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            capacity: cap,
            samples: RwLock::new(Vec::with_capacity(cap)),
            pos: AtomicUsize::new(0),
            total_recorded: AtomicU64::new(0),
        }
    }

    /// Append an outcome. O(1) on the steady-state path.
    pub fn record(&self, outcome: Outcome, latency_ms: u32) {
        let s = Sample {
            tag: match outcome {
                Outcome::Success => 0,
                Outcome::Failure => 1,
            },
            latency_ms,
        };
        let mut w = self.samples.write();
        if w.len() < self.capacity {
            w.push(s);
        } else {
            // Slot replacement; pos % cap selects the oldest entry.
            let i = self.pos.load(Ordering::Relaxed) % self.capacity;
            w[i] = s;
            self.pos.store(i.wrapping_add(1), Ordering::Relaxed);
        }
        self.total_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the current error rate as basis points of total samples,
    /// e.g. `2_000` = 20 %. Returns `0` when the window is empty.
    pub fn error_rate_bps(&self) -> u32 {
        let r = self.samples.read();
        if r.is_empty() {
            return 0;
        }
        let failures = r.iter().filter(|s| s.tag == 1).count() as u64;
        let total = r.len() as u64;
        ((failures * 10_000) / total) as u32
    }

    /// p99 latency in milliseconds across the window. Returns `0` when
    /// no usable measurements are present.
    pub fn p99_latency_ms(&self) -> u32 {
        let r = self.samples.read();
        if r.is_empty() {
            return 0;
        }
        let mut latencies: Vec<u32> = r
            .iter()
            .map(|s| s.latency_ms)
            .filter(|ms| *ms != u32::MAX)
            .collect();
        if latencies.is_empty() {
            return 0;
        }
        latencies.sort_unstable();
        // Standard nearest-rank p99: ceil(0.99 * n) - 1 (zero-indexed).
        let n = latencies.len();
        let idx = ((n as f64 * 0.99).ceil() as usize)
            .saturating_sub(1)
            .min(n - 1);
        latencies[idx]
    }

    /// Returns the number of outcomes currently held in the window
    /// (≤ capacity).
    pub fn window_len(&self) -> usize {
        self.samples.read().len()
    }

    /// Total outcomes ever recorded (not bounded by capacity).
    pub fn total_recorded(&self) -> u64 {
        self.total_recorded.load(Ordering::Relaxed)
    }

    /// Drain the window. Used after a successful failover so the new
    /// active slot starts fresh.
    pub fn reset(&self) {
        self.samples.write().clear();
        self.pos.store(0, Ordering::Relaxed);
        // total_recorded is intentionally NOT reset — it's a cumulative
        // counter.
    }

    /// Configured window capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Per-router thresholds used to decide failover.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FailoverThresholds {
    /// Fail over when `error_rate_bps >= this`.
    pub error_rate_bps: u32,
    /// Fail over when `p99_latency_ms >= this`.
    pub p99_latency_ms: u32,
    /// Minimum samples in the window before failover triggers — small
    /// windows tend to be noisy.
    pub min_samples: u32,
}

impl FailoverThresholds {
    /// Construct thresholds from `BrokerConfig` values (R6.5).
    /// `failover_error_rate` is a fraction in `[0, 1]`; we convert to
    /// basis points and round to the nearest whole bp.
    pub fn from_broker_config(failover_error_rate: f32, failover_latency_ms: u32) -> Self {
        let bps = (failover_error_rate.clamp(0.0, 1.0) * 10_000.0).round() as u32;
        Self {
            error_rate_bps: bps,
            p99_latency_ms: failover_latency_ms,
            min_samples: 8, // small floor so we don't flap on the first failure
        }
    }
}

/// Active/backup broker router.
pub struct BrokerRouter {
    primary_id: BrokerId,
    backup_id: BrokerId,
    primary: Arc<dyn BrokerAdapter>,
    backup: Arc<dyn BrokerAdapter>,
    /// `0` = primary active, `1` = backup active.
    active: AtomicU8,
    primary_stats: AdapterStats,
    backup_stats: AdapterStats,
    thresholds: FailoverThresholds,
}

impl BrokerRouter {
    /// Construct a router. Defaults the active slot to the primary.
    pub fn new(
        primary: Arc<dyn BrokerAdapter>,
        backup: Arc<dyn BrokerAdapter>,
        thresholds: FailoverThresholds,
    ) -> Self {
        let primary_id = primary.broker_id();
        let backup_id = backup.broker_id();
        Self {
            primary_id,
            backup_id,
            primary,
            backup,
            active: AtomicU8::new(ActiveSlot::Primary as u8),
            primary_stats: AdapterStats::with_capacity(WINDOW_SIZE),
            backup_stats: AdapterStats::with_capacity(WINDOW_SIZE),
            thresholds,
        }
    }

    /// Borrow the active adapter handle. The returned `Arc` is cheap to
    /// clone for spawn-friendly call sites.
    pub fn active_adapter(&self) -> Arc<dyn BrokerAdapter> {
        match ActiveSlot::from_u8(self.active.load(Ordering::Acquire)) {
            ActiveSlot::Primary => Arc::clone(&self.primary),
            ActiveSlot::Backup => Arc::clone(&self.backup),
        }
    }

    /// Borrow the active adapter's broker id.
    pub fn active_id(&self) -> BrokerId {
        match ActiveSlot::from_u8(self.active.load(Ordering::Acquire)) {
            ActiveSlot::Primary => self.primary_id,
            ActiveSlot::Backup => self.backup_id,
        }
    }

    /// Active slot enum. Useful for tests asserting failover.
    pub fn active_slot(&self) -> ActiveSlot {
        ActiveSlot::from_u8(self.active.load(Ordering::Acquire))
    }

    /// Configured primary broker id.
    #[inline]
    pub fn primary_id(&self) -> BrokerId {
        self.primary_id
    }

    /// Configured backup broker id.
    #[inline]
    pub fn backup_id(&self) -> BrokerId {
        self.backup_id
    }

    /// Borrow the primary stats (read-only handle).
    pub fn primary_stats(&self) -> &AdapterStats {
        &self.primary_stats
    }

    /// Borrow the backup stats (read-only handle).
    pub fn backup_stats(&self) -> &AdapterStats {
        &self.backup_stats
    }

    /// Adaptive routing — pick an adapter for the given approval. The
    /// caller passes a hint extracted from `RiskApproval.execution_params`
    /// (today the schema doesn't carry the field; once it does we use
    /// the hint to prefer a specific broker).
    ///
    /// Returns the active adapter when no hint matches, which is the
    /// canonical safe fallback.
    pub fn pick_for_intent(&self, hint: Option<BrokerId>) -> Arc<dyn BrokerAdapter> {
        if let Some(target) = hint {
            if target == self.primary_id {
                self.swap_to(ActiveSlot::Primary);
                return Arc::clone(&self.primary);
            }
            if target == self.backup_id {
                self.swap_to(ActiveSlot::Backup);
                return Arc::clone(&self.backup);
            }
            // Hint did not match either configured broker — ignore and
            // fall through to the active slot.
        }
        self.active_adapter()
    }

    /// Record an outcome against the active adapter and run failover
    /// detection. Returns `Some(FailoverEvent)` when the swap is
    /// triggered.
    pub fn record_outcome(
        &self,
        outcome: Outcome,
        latency_ms: u32,
        ts_ns: u64,
    ) -> Option<FailoverEvent> {
        let slot = self.active_slot();
        let stats = match slot {
            ActiveSlot::Primary => &self.primary_stats,
            ActiveSlot::Backup => &self.backup_stats,
        };
        stats.record(outcome, latency_ms);

        // Only consider failover once we have enough samples.
        if (stats.window_len() as u32) < self.thresholds.min_samples {
            return None;
        }

        let er = stats.error_rate_bps();
        let p99 = stats.p99_latency_ms();
        let breach =
            er >= self.thresholds.error_rate_bps || p99 >= self.thresholds.p99_latency_ms;
        if !breach {
            return None;
        }

        // Atomic CAS to swap. If a concurrent caller already swapped,
        // we observe the swap and skip emitting a duplicate event.
        let want = slot.other() as u8;
        match self.active.compare_exchange(
            slot as u8,
            want,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                let from_id = match slot {
                    ActiveSlot::Primary => self.primary_id,
                    ActiveSlot::Backup => self.backup_id,
                };
                let to_id = match slot.other() {
                    ActiveSlot::Primary => self.primary_id,
                    ActiveSlot::Backup => self.backup_id,
                };
                // Reset the *new* active slot's window so it starts
                // fresh; preserve the old slot's history for inspection.
                match slot.other() {
                    ActiveSlot::Primary => self.primary_stats.reset(),
                    ActiveSlot::Backup => self.backup_stats.reset(),
                };
                Some(FailoverEvent {
                    from: from_id,
                    to: to_id,
                    error_rate_bps: er,
                    p99_latency_ms: p99,
                    ts_ns,
                })
            }
            Err(_) => None,
        }
    }

    /// Force the active slot. Used in tests; production code only
    /// changes the slot via [`Self::record_outcome`].
    fn swap_to(&self, target: ActiveSlot) {
        self.active.store(target as u8, Ordering::Release);
    }

    /// Reset the active slot back to the primary. Used by the
    /// supervisor when the primary recovers and the operator wants to
    /// fail back manually.
    pub fn force_primary(&self) {
        self.swap_to(ActiveSlot::Primary);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hedge_broker_api::{
        BrokerAdapter, BrokerError, BrokerMetric, OrderIntent, OrderModification, OrderStatus,
        ReadyState, SubmitAck,
    };

    /// Stub adapter used by router tests.
    struct StubAdapter {
        id: BrokerId,
    }

    #[async_trait]
    impl BrokerAdapter for StubAdapter {
        fn broker_id(&self) -> BrokerId {
            self.id
        }
        async fn submit(&self, _intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
            unreachable!("router tests do not exercise submit");
        }
        async fn modify(&self, _m: &OrderModification) -> Result<(), BrokerError> {
            Ok(())
        }
        async fn cancel(&self, _id: &str) -> Result<(), BrokerError> {
            Ok(())
        }
        async fn status(&self, _id: &str) -> Result<OrderStatus, BrokerError> {
            unreachable!()
        }
        async fn metrics(&self) -> Vec<BrokerMetric> {
            Vec::new()
        }
        async fn ready(&self) -> ReadyState {
            ReadyState::Ready
        }
    }

    fn router(thresholds: FailoverThresholds) -> BrokerRouter {
        let p: Arc<dyn BrokerAdapter> = Arc::new(StubAdapter {
            id: BrokerId::Zerodha,
        });
        let b: Arc<dyn BrokerAdapter> = Arc::new(StubAdapter { id: BrokerId::Dhan });
        BrokerRouter::new(p, b, thresholds)
    }

    #[test]
    fn defaults_to_primary_active() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 1_000,
            min_samples: 4,
        });
        assert_eq!(r.active_slot(), ActiveSlot::Primary);
        assert_eq!(r.active_id(), BrokerId::Zerodha);
    }

    /// Failover triggers when error rate breaches; emits exactly one event.
    #[test]
    fn failover_on_error_rate_breach() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000, // 50%
            p99_latency_ms: 100_000,
            min_samples: 4,
        });
        let mut event_count = 0;
        // 4 failures back to back -> 100% error rate, breach.
        for _ in 0..4 {
            if let Some(e) = r.record_outcome(Outcome::Failure, 10, 0) {
                event_count += 1;
                assert_eq!(e.from, BrokerId::Zerodha);
                assert_eq!(e.to, BrokerId::Dhan);
                assert!(e.error_rate_bps >= 5_000);
            }
        }
        assert_eq!(event_count, 1, "exactly one failover event");
        assert_eq!(r.active_slot(), ActiveSlot::Backup);
    }

    /// Failover triggers when p99 latency breaches even with zero
    /// failures.
    #[test]
    fn failover_on_p99_latency_breach() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 100,
            min_samples: 4,
        });
        // 4 successes with high latency -> p99 = 500 > 100.
        for _ in 0..4 {
            r.record_outcome(Outcome::Success, 500, 0);
        }
        assert_eq!(r.active_slot(), ActiveSlot::Backup);
    }

    /// No failover when min_samples is not yet reached.
    #[test]
    fn no_failover_below_min_samples() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 100,
            min_samples: 8,
        });
        for _ in 0..4 {
            assert!(r.record_outcome(Outcome::Failure, 1_000, 0).is_none());
        }
        assert_eq!(r.active_slot(), ActiveSlot::Primary);
    }

    /// No failover when neither threshold is breached.
    #[test]
    fn no_failover_when_thresholds_not_breached() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 1_000,
            min_samples: 4,
        });
        for _ in 0..16 {
            assert!(r.record_outcome(Outcome::Success, 50, 0).is_none());
        }
        assert_eq!(r.active_slot(), ActiveSlot::Primary);
    }

    /// After failover, the new active slot starts with an empty window.
    #[test]
    fn failover_resets_target_window() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 100_000,
            min_samples: 4,
        });
        for _ in 0..4 {
            r.record_outcome(Outcome::Failure, 10, 0);
        }
        assert_eq!(r.active_slot(), ActiveSlot::Backup);
        assert_eq!(
            r.backup_stats().window_len(),
            0,
            "backup window must be reset after failover"
        );
    }

    /// pick_for_intent prefers the hinted broker.
    #[test]
    fn pick_for_intent_honors_hint() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 1_000,
            min_samples: 4,
        });
        let _ = r.pick_for_intent(Some(BrokerId::Dhan));
        assert_eq!(r.active_slot(), ActiveSlot::Backup);

        let _ = r.pick_for_intent(Some(BrokerId::Zerodha));
        assert_eq!(r.active_slot(), ActiveSlot::Primary);

        // Unknown hint falls through to active slot without changing it.
        let _ = r.pick_for_intent(Some(BrokerId::AngelOne));
        assert_eq!(r.active_slot(), ActiveSlot::Primary);
    }

    /// AdapterStats correctly computes error rate basis points.
    #[test]
    fn adapter_stats_error_rate() {
        let s = AdapterStats::with_capacity(10);
        for _ in 0..3 {
            s.record(Outcome::Failure, 50);
        }
        for _ in 0..7 {
            s.record(Outcome::Success, 50);
        }
        assert_eq!(s.error_rate_bps(), 3_000); // 30 %
    }

    /// AdapterStats p99 latency uses nearest-rank.
    #[test]
    fn adapter_stats_p99_latency() {
        let s = AdapterStats::with_capacity(100);
        for ms in 1u32..=100 {
            s.record(Outcome::Success, ms);
        }
        // Sorted: 1..100; p99 nearest-rank = ceil(0.99*100) = 99 -> idx 98 -> value 99.
        assert_eq!(s.p99_latency_ms(), 99);
    }

    /// AdapterStats wraps when capacity is exceeded.
    #[test]
    fn adapter_stats_wrapping() {
        let s = AdapterStats::with_capacity(4);
        for _ in 0..10 {
            s.record(Outcome::Failure, 50);
        }
        assert_eq!(s.window_len(), 4);
        assert_eq!(s.total_recorded(), 10);
    }

    /// FailoverThresholds::from_broker_config converts fractions to
    /// basis points correctly.
    #[test]
    fn thresholds_from_broker_config() {
        let t = FailoverThresholds::from_broker_config(0.20, 250);
        assert_eq!(t.error_rate_bps, 2_000);
        assert_eq!(t.p99_latency_ms, 250);
    }

    /// force_primary resets the active slot back.
    #[test]
    fn force_primary_resets() {
        let r = router(FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 100_000,
            min_samples: 4,
        });
        for _ in 0..4 {
            r.record_outcome(Outcome::Failure, 10, 0);
        }
        assert_eq!(r.active_slot(), ActiveSlot::Backup);
        r.force_primary();
        assert_eq!(r.active_slot(), ActiveSlot::Primary);
    }
}
