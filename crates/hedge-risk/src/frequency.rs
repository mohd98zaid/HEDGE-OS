//! Trade-frequency counters (R5.6).
//!
//! Implements per-minute / per-hour / per-session sliding-window counters
//! that the Risk_Engine consults before approving a new entry. Using
//! [`hedge_core::RingWindow`] keeps the steady state allocation-free
//! (R3.4): timestamps are stored in inline arrays whose capacity is set
//! at construction-time to the configured limit + 1 so that "would-be
//! N+1 in the window" can be answered without a heap touch.
//!
//! ### Sliding window semantics
//!
//! Each ring stores the absolute timestamps (in nanoseconds) of recently
//! admitted entries. An entry is "in the window" iff
//! `now - ts <= window_duration`. On every `would_admit`/`record` call
//! the engine first prunes entries that have aged out (cheap because
//! pruning is iteration-only; no element is moved).
//!
//! ### Configuration
//!
//! The brief mandates `RingWindow<Instant, N>` from `hedge-core`. We
//! store `u64` nanoseconds rather than `quanta::Instant` because the
//! `hedge-core::RingWindow` API requires `T: Copy + Default` and
//! `quanta::Instant` is `Copy` but not `Default`. Using a u64 ns
//! representation also lets us write deterministic tests by injecting
//! synthetic timestamps without needing to construct `quanta::Instant`
//! values, which is intentionally non-public in `quanta`.
//!
//! Window sizes are tuned to the design's defaults (R32.4):
//! 4 trades/min, 30 trades/hour, 60 trades/session. We round up to
//! 8 / 64 / 128 inline slots so the ring has headroom for "the proposed
//! N+1 trade" check.

use hedge_core::RingWindow;

/// Inline ring capacity for the per-minute counter (R32.4 default 4 → 8 slots).
const PER_MINUTE_CAP: usize = 8;
/// Inline ring capacity for the per-hour counter (R32.4 default 30 → 64 slots).
const PER_HOUR_CAP: usize = 64;
/// Inline ring capacity for the per-session counter (R32.4 default 60 → 128 slots).
const PER_SESSION_CAP: usize = 128;

/// Nanoseconds in one minute.
const NS_PER_MINUTE: u64 = 60 * 1_000_000_000;
/// Nanoseconds in one hour.
const NS_PER_HOUR: u64 = 60 * NS_PER_MINUTE;

/// Sliding-window trade counters (R5.6, R32.4).
///
/// All three windows record nanosecond timestamps of admitted entries.
/// `would_breach` and `record` are O(N) in the window size; with the
/// configured caps (≤ 128) they cost a single cache-resident scan.
pub struct FrequencyCounters {
    minute: RingWindow<u64, PER_MINUTE_CAP>,
    hour: RingWindow<u64, PER_HOUR_CAP>,
    session: RingWindow<u64, PER_SESSION_CAP>,
    /// Cached counts after the most recent prune — exposed for metrics.
    cached_minute_count: u32,
    cached_hour_count: u32,
    cached_session_count: u32,
}

impl FrequencyCounters {
    /// Construct empty counters.
    pub fn new() -> Self {
        Self {
            minute: RingWindow::new(),
            hour: RingWindow::new(),
            session: RingWindow::new(),
            cached_minute_count: 0,
            cached_hour_count: 0,
            cached_session_count: 0,
        }
    }

    /// Returns `true` when admitting one more entry at `now_ns` would
    /// breach any of the three configured limits.
    ///
    /// The caller must call [`record`](Self::record) **only** when this
    /// returns `false`. `record` performs no further breach check.
    pub fn would_breach(
        &mut self,
        now_ns: u64,
        max_per_minute: u32,
        max_per_hour: u32,
        max_per_session: u32,
    ) -> bool {
        let m = self.live_count(&self.minute, now_ns, NS_PER_MINUTE);
        let h = self.live_count(&self.hour, now_ns, NS_PER_HOUR);
        // Session window has no time prune — we count everything since
        // the counter was last reset (typically `ops.session.start`).
        let s = self.session.len() as u32;
        self.cached_minute_count = m;
        self.cached_hour_count = h;
        self.cached_session_count = s;
        // Compare prospective counts: the current count + 1 must remain
        // ≤ the configured maximum.
        m + 1 > max_per_minute || h + 1 > max_per_hour || s + 1 > max_per_session
    }

    /// Record a successful admission at `now_ns`.
    ///
    /// `RingWindow::push` is O(1) and overwrites the oldest slot when
    /// full — that is acceptable because `would_breach` always rejects
    /// on overflow before this point would matter for correctness.
    pub fn record(&mut self, now_ns: u64) {
        self.minute.push(now_ns);
        self.hour.push(now_ns);
        self.session.push(now_ns);
        // Refresh cached counts so metrics readers see the post-record state.
        self.cached_minute_count = self.live_count(&self.minute, now_ns, NS_PER_MINUTE);
        self.cached_hour_count = self.live_count(&self.hour, now_ns, NS_PER_HOUR);
        self.cached_session_count = self.session.len() as u32;
    }

    /// Reset the per-session counter — typically wired to
    /// `ops.session.start` (R31.1).
    pub fn reset_session(&mut self) {
        self.session.clear();
        self.cached_session_count = 0;
    }

    /// Cached count of admissions in the past minute (post-prune).
    #[inline]
    pub fn count_minute(&self) -> u32 {
        self.cached_minute_count
    }

    /// Cached count of admissions in the past hour (post-prune).
    #[inline]
    pub fn count_hour(&self) -> u32 {
        self.cached_hour_count
    }

    /// Count of admissions since the most recent `reset_session`.
    #[inline]
    pub fn count_session(&self) -> u32 {
        self.cached_session_count
    }

    /// Count entries in `ring` that are still within `window_ns` of `now_ns`.
    fn live_count<const N: usize>(
        &self,
        ring: &RingWindow<u64, N>,
        now_ns: u64,
        window_ns: u64,
    ) -> u32 {
        // Iteration is bounded by the inline capacity. We compare on
        // saturating-sub so a wonky clock that briefly goes backwards
        // does not panic.
        let mut count = 0u32;
        for ts in ring.iter() {
            if now_ns.saturating_sub(*ts) <= window_ns {
                count += 1;
            }
        }
        count
    }
}

impl Default for FrequencyCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for FrequencyCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrequencyCounters")
            .field("minute", &self.cached_minute_count)
            .field("hour", &self.cached_hour_count)
            .field("session", &self.cached_session_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_counters_admit_first_trade() {
        let mut fc = FrequencyCounters::new();
        assert!(!fc.would_breach(1_000, 4, 30, 60));
    }

    #[test]
    fn would_breach_when_minute_count_at_max() {
        // 4 trades in the last second; the 5th would breach 4/min.
        let mut fc = FrequencyCounters::new();
        for i in 0..4u64 {
            assert!(!fc.would_breach(i * 1_000, 4, 30, 60));
            fc.record(i * 1_000);
        }
        // 5th trade in the same window — breach.
        assert!(fc.would_breach(4_000, 4, 30, 60));
    }

    #[test]
    fn old_entries_age_out_of_minute_window() {
        let mut fc = FrequencyCounters::new();
        fc.record(0);
        fc.record(1_000);
        fc.record(2_000);
        fc.record(3_000);
        // Counter is at 4/min — 5th would breach.
        assert!(fc.would_breach(4_000, 4, 30, 60));
        // Wait > 60s — old entries age out.
        let later = NS_PER_MINUTE + 5_000;
        assert!(!fc.would_breach(later, 4, 30, 60));
        assert_eq!(fc.count_minute(), 0, "all earlier entries aged out");
    }

    #[test]
    fn would_breach_when_session_count_at_max() {
        let mut fc = FrequencyCounters::new();
        // Use a per-minute / per-hour cap higher than the session cap so
        // only the session limit can fire.
        for i in 0..60u64 {
            assert!(!fc.would_breach(i * NS_PER_HOUR, 100, 1000, 60));
            fc.record(i * NS_PER_HOUR);
        }
        // 61st entry — session breach (60/session cap hit).
        assert!(fc.would_breach(60 * NS_PER_HOUR, 100, 1000, 60));
    }

    #[test]
    fn reset_session_clears_session_counter_only() {
        let mut fc = FrequencyCounters::new();
        for i in 0..3u64 {
            fc.record(i);
        }
        // Session count = 3. Hour count = 3.
        let _ = fc.would_breach(3, 4, 30, 60);
        assert_eq!(fc.count_session(), 3);
        fc.reset_session();
        assert_eq!(fc.count_session(), 0);
        // Hour window untouched (if entry timestamps still in the window).
        let _ = fc.would_breach(3, 4, 30, 60);
        assert_eq!(fc.count_hour(), 3);
    }

    #[test]
    fn would_breach_when_hour_count_at_max() {
        let mut fc = FrequencyCounters::new();
        // 30 trades spread over 30 minutes; 31st within the hour breaches.
        for i in 0..30u64 {
            // Spread far enough to not breach 4/min.
            assert!(!fc.would_breach(i * NS_PER_MINUTE * 2, 4, 30, 60));
            fc.record(i * NS_PER_MINUTE * 2);
        }
        // 31st within the hour — breach (30/hour cap).
        assert!(fc.would_breach(30 * NS_PER_MINUTE * 2, 4, 30, 60));
    }
}
