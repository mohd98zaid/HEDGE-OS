//! Monotonic clock and latency-timer helpers.
//!
//! The Hot_Path measures every per-stage latency in nanoseconds against
//! the budgets in design § "Latency Budget Allocation":
//!
//! | Stage              | p99 budget |
//! |--------------------|-----------:|
//! | Tick ingest        | 2 ms       |
//! | Feature extraction | 3 ms       |
//! | Risk check         | 2 ms       |
//! | Execution routing  | 5 ms       |
//! | End-to-end         | 50 ms      |
//!
//! `quanta::Instant` is used because it reads the rdtsc / mach
//! absolute-time counter directly and is materially cheaper than
//! `std::time::Instant`. The crate-level `now_ns` returns a `u64`
//! monotonic counter that lines up with the FlatBuffers `ts_ns`
//! fields (`Tick_v1.ts_recv_ns`, `Signal_v1.ts_ns`, ...).
//!
//! Two `LatencyTimer` flavours are provided:
//!
//! * [`CallbackLatencyTimer`] — invokes a user closure on drop, useful
//!   when the caller wants to push the elapsed nanos into a metric or
//!   emit a `LatencyRecord_v1` directly.
//! * [`AtomicLatencyTimer`] — writes the elapsed nanos into a
//!   `&'a AtomicU64` on drop, useful for steady-state hot loops where
//!   allocating a `Box<dyn FnOnce(u64)>` is unacceptable.
//!
//! Both record their elapsed time **once** on `Drop`. Use
//! [`LatencyTimer::elapsed_ns`] to read the running elapsed without
//! consuming the timer.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use quanta::Instant;

/// Process-startup reference instant. The first call to [`now_ns`] (or
/// [`epoch_instant`]) seeds this; thereafter every nanosecond timestamp is
/// expressed as the delta from this fixed reference.
static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Returns the lazily-initialised epoch used by [`now_ns`].
#[inline]
fn epoch_instant() -> Instant {
    *EPOCH.get_or_init(Instant::now)
}

/// Returns the current monotonic timestamp in nanoseconds since process
/// startup.
///
/// Backed by `quanta::Instant`, which reads the rdtsc / mach
/// absolute-time counter directly and is materially cheaper than
/// `std::time::Instant`. The reference epoch is process startup, so
/// values are only meaningful as deltas — never as wall clock times.
/// Wall-clock conversions (e.g. for IST session boundaries) belong in
/// `hedge-session`, not here.
///
/// Output values map directly into the FlatBuffers `ts_ns` fields
/// (`Tick_v1.ts_recv_ns`, `Signal_v1.ts_ns`, ...).
#[inline]
pub fn now_ns() -> u64 {
    let epoch = epoch_instant();
    let elapsed = Instant::now().duration_since(epoch);
    let n = elapsed.as_nanos();
    if n > u64::MAX as u128 {
        u64::MAX
    } else {
        n as u64
    }
}

/// Convenience trait — both timer flavours expose `elapsed_ns()`.
pub trait LatencyTimer {
    /// Nanoseconds elapsed since the timer was created.
    fn elapsed_ns(&self) -> u64;
}

// --- internal helper ------------------------------------------------------

/// Internal: compute elapsed nanoseconds from a `quanta::Instant`,
/// saturating at zero (the monotonic clock cannot produce a negative
/// delta but we still defend against arithmetic surprises).
#[inline]
fn elapsed_ns_from(start: Instant) -> u64 {
    let d = start.elapsed();
    // `Duration::as_nanos()` returns `u128`; clamp to `u64`.
    let n = d.as_nanos();
    if n > u64::MAX as u128 {
        u64::MAX
    } else {
        n as u64
    }
}

// --- callback variant -----------------------------------------------------

/// RAII timer that invokes a user callback with the elapsed nanoseconds
/// when dropped.
///
/// The callback is `FnOnce(u64)` and is held in a `Box<dyn FnOnce(u64)>` —
/// **this allocates** at construction. Use this variant in cold paths
/// (startup, configuration reload, journal emission) and prefer
/// [`AtomicLatencyTimer`] in steady-state hot loops.
pub struct CallbackLatencyTimer {
    start: Instant,
    on_drop: Option<Box<dyn FnOnce(u64) + Send>>,
}

impl CallbackLatencyTimer {
    /// Start a new timer. The callback is invoked exactly once, on drop.
    #[inline]
    pub fn new<F>(on_drop: F) -> Self
    where
        F: FnOnce(u64) + Send + 'static,
    {
        Self {
            start: Instant::now(),
            on_drop: Some(Box::new(on_drop)),
        }
    }
}

impl LatencyTimer for CallbackLatencyTimer {
    #[inline]
    fn elapsed_ns(&self) -> u64 {
        elapsed_ns_from(self.start)
    }
}

impl Drop for CallbackLatencyTimer {
    fn drop(&mut self) {
        if let Some(cb) = self.on_drop.take() {
            cb(elapsed_ns_from(self.start));
        }
    }
}

// --- atomic variant -------------------------------------------------------

/// RAII timer that writes its elapsed nanos into a borrowed `AtomicU64`
/// on drop.
///
/// The atomic store uses [`Ordering::Relaxed`] — the consumer that reads
/// the recorded latency is responsible for applying the appropriate
/// ordering when reading. Hot_Path callers typically hold the atomic in
/// a per-task `LatencyState` struct and read it in a follow-up
/// `obs.latency.<stage>` emission step that already has its own happens-
/// before edge.
///
/// Construction performs **no allocation**, satisfying R2.6 and R3.4.
pub struct AtomicLatencyTimer<'a> {
    start: Instant,
    sink: &'a AtomicU64,
    armed: bool,
}

impl<'a> AtomicLatencyTimer<'a> {
    /// Start a new timer that will write into `sink` on drop.
    #[inline]
    pub fn new(sink: &'a AtomicU64) -> Self {
        Self {
            start: Instant::now(),
            sink,
            armed: true,
        }
    }

    /// Stop the timer **without** writing to the sink. Used when a hot-loop
    /// branch does not want to pollute the latency channel (e.g. a no-op
    /// fast path that early-returns).
    #[inline]
    pub fn cancel(mut self) {
        self.armed = false;
    }
}

impl LatencyTimer for AtomicLatencyTimer<'_> {
    #[inline]
    fn elapsed_ns(&self) -> u64 {
        elapsed_ns_from(self.start)
    }
}

impl Drop for AtomicLatencyTimer<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.sink.store(elapsed_ns_from(self.start), Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn now_ns_is_monotonic_non_decreasing() {
        // Property: successive `now_ns()` calls never go backwards.
        let mut prev = now_ns();
        for _ in 0..1000 {
            let cur = now_ns();
            assert!(cur >= prev, "clock went backwards: {} -> {}", prev, cur);
            prev = cur;
        }
    }

    #[test]
    fn callback_timer_invokes_callback_with_non_zero_elapsed() {
        let sink: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
        {
            let s = sink.clone();
            let _t = CallbackLatencyTimer::new(move |ns| {
                *s.lock().unwrap() = Some(ns);
            });
            // Force a measurable elapsed window so the assertion is robust
            // against very fast machines.
            thread::sleep(Duration::from_millis(2));
        }
        let elapsed = sink.lock().unwrap().expect("callback was not invoked");
        assert!(
            elapsed >= 1_000_000,
            "elapsed should be >= 1ms, got {}ns",
            elapsed
        );
    }

    #[test]
    fn callback_timer_records_non_decreasing_nanos() {
        // Property R28: every per-stage latency is monotonic non-decreasing
        // when sampled within a single timer's lifetime.
        let timer = CallbackLatencyTimer::new(|_| {});
        let a = timer.elapsed_ns();
        thread::sleep(Duration::from_millis(1));
        let b = timer.elapsed_ns();
        assert!(b >= a, "elapsed not monotonic: {} -> {}", a, b);
    }

    #[test]
    fn atomic_timer_writes_elapsed_on_drop() {
        let sink = AtomicU64::new(0);
        {
            let _t = AtomicLatencyTimer::new(&sink);
            thread::sleep(Duration::from_millis(2));
        }
        let recorded = sink.load(Ordering::Relaxed);
        assert!(recorded >= 1_000_000, "atomic recorded {} ns", recorded);
    }

    #[test]
    fn atomic_timer_cancel_does_not_write() {
        let sink = AtomicU64::new(0xdead_beef);
        {
            let t = AtomicLatencyTimer::new(&sink);
            t.cancel();
        }
        // Still the original sentinel — proves cancel suppresses the store.
        assert_eq!(sink.load(Ordering::Relaxed), 0xdead_beef);
    }

    #[test]
    fn atomic_timer_elapsed_ns_is_non_decreasing() {
        let sink = AtomicU64::new(0);
        let timer = AtomicLatencyTimer::new(&sink);
        let mut prev = timer.elapsed_ns();
        for _ in 0..10 {
            let cur = timer.elapsed_ns();
            assert!(cur >= prev);
            prev = cur;
            // Tiny work item to make ticks visible without dominating the
            // test runtime.
            std::hint::black_box(0u64);
        }
        // Drop will write `prev` (or a slightly larger value) into the sink.
    }
}
