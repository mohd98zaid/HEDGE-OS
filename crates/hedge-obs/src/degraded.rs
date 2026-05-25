//! Degraded-telemetry primitives.
//!
//! The design (Error Handling § Degraded Telemetry) requires that telemetry
//! pipeline failures are themselves failure-tolerant:
//!
//! 1. **Loki unavailable** — low-severity logs are dropped at the source;
//!    high-severity logs are buffered in a bounded local ring; the Hot_Path
//!    is never blocked on log shipping.
//! 2. **Jaeger overloaded** — traces are downsampled per
//!    `degraded_mode.sample_traces_at_jaeger_overload` (default 0.10).
//! 3. **Prometheus** is pull-based and lossless within the scrape window;
//!    it has no degraded-mode behaviour.
//!
//! This module exposes:
//!
//! * [`DegradedState`] — process-global atomic flags read by the logging
//!   and tracing layers to make per-event drop / sample decisions.
//! * [`BoundedRingLogBuffer<N>`] — a const-generic, fixed-capacity FIFO
//!   that drops the **oldest** entry when full. Used to hold high-severity
//!   logs while Loki is unreachable so they can be drained on reconnect.
//!
//! Both primitives are lock-light (`Mutex` only on a const-N `VecDeque`-like
//! structure backed by `parking_lot`) so they remain cheap for the spawn-on-
//! drop emission path that calls them.

use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::Lazy;
use parking_lot::Mutex;

/// Process-global degraded-telemetry state.
///
/// Use [`degraded_state`] to read or mutate. The flags are independent so the
/// logging path can fall back without affecting tracing and vice versa.
pub struct DegradedState {
    loki_unavailable: AtomicBool,
    jaeger_overloaded: AtomicBool,
}

impl DegradedState {
    /// All flags off — the steady-state observed at process start.
    const fn new() -> Self {
        Self {
            loki_unavailable: AtomicBool::new(false),
            jaeger_overloaded: AtomicBool::new(false),
        }
    }

    /// Returns `true` when the Loki client has signalled the endpoint is
    /// unreachable. The logging layer reads this to decide whether to drop
    /// low-severity records.
    #[inline]
    pub fn loki_unavailable(&self) -> bool {
        self.loki_unavailable.load(Ordering::Acquire)
    }

    /// Set or clear the Loki-unavailable flag. The store uses `Release`
    /// ordering so a subsequent `Acquire` read by the logging layer happens-
    /// after the toggle.
    #[inline]
    pub fn set_loki_unavailable(&self, value: bool) {
        self.loki_unavailable.store(value, Ordering::Release);
    }

    /// Returns `true` when the OTLP exporter has signalled Jaeger is
    /// overloaded. The tracing layer reads this to apply the configured
    /// downsample ratio.
    #[inline]
    pub fn jaeger_overloaded(&self) -> bool {
        self.jaeger_overloaded.load(Ordering::Acquire)
    }

    /// Set or clear the Jaeger-overloaded flag.
    #[inline]
    pub fn set_jaeger_overloaded(&self, value: bool) {
        self.jaeger_overloaded.store(value, Ordering::Release);
    }
}

static DEGRADED: Lazy<DegradedState> = Lazy::new(DegradedState::new);

/// Borrow the process-global [`DegradedState`].
///
/// The value is initialised lazily on first access and shared for the
/// lifetime of the process. Tests in the same process must reset flags they
/// have toggled — the helper does **not** snapshot or roll back.
#[inline]
pub fn degraded_state() -> &'static DegradedState {
    &DEGRADED
}

/// Convenience predicate matching the design's spelling
/// (`degraded::loki_unavailable()`).
#[inline]
pub fn loki_unavailable() -> bool {
    degraded_state().loki_unavailable()
}

/// Convenience setter.
#[inline]
pub fn set_loki_unavailable(value: bool) {
    degraded_state().set_loki_unavailable(value);
}

/// Convenience predicate.
#[inline]
pub fn jaeger_overloaded() -> bool {
    degraded_state().jaeger_overloaded()
}

/// Convenience setter.
#[inline]
pub fn set_jaeger_overloaded(value: bool) {
    degraded_state().set_jaeger_overloaded(value);
}

// ---- Bounded ring buffer ------------------------------------------------

/// Fixed-capacity FIFO that drops the **oldest** entry when full.
///
/// Used by the logging layer to retain high-severity records while Loki is
/// unreachable. The const-generic capacity `N` keeps the buffer's storage
/// inline-allocated (no `Vec` resize) so worst-case memory usage is
/// predictable on the Hot_Path.
///
/// The implementation backs storage with a heap-allocated `Vec<Option<T>>`
/// of length `N` rather than an `[Option<T>; N]` array because `Option<T>`
/// is not `Copy` for the typical `T = LogEnvelope` payload, and because the
/// `Vec` is allocated **once** at construction (not on each push).
pub struct BoundedRingLogBuffer<const N: usize, T> {
    inner: Mutex<RingState<T>>,
    capacity: usize,
}

struct RingState<T> {
    buf: Vec<Option<T>>,
    head: usize, // index of the next slot to read
    len: usize,
}

impl<const N: usize, T> BoundedRingLogBuffer<N, T> {
    /// Construct an empty ring with capacity `N`.
    ///
    /// # Panics
    ///
    /// Panics if `N == 0`.
    pub fn new() -> Self {
        assert!(N > 0, "BoundedRingLogBuffer capacity must be > 0");
        let mut buf: Vec<Option<T>> = Vec::with_capacity(N);
        for _ in 0..N {
            buf.push(None);
        }
        Self {
            inner: Mutex::new(RingState { buf, head: 0, len: 0 }),
            capacity: N,
        }
    }

    /// The compile-time capacity.
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Number of entries currently held.
    pub fn len(&self) -> usize {
        self.inner.lock().len
    }

    /// `true` when no entries are buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `true` when at capacity.
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity
    }

    /// Push an entry. Returns the displaced **oldest** entry when the ring
    /// is full so callers can observe drops (e.g. increment a counter).
    ///
    /// The drop-oldest policy is intentional: high-severity logs that
    /// describe the *most recent* failure context are more useful to drain
    /// on reconnect than ancient entries that may already be stale.
    pub fn push(&self, value: T) -> Option<T> {
        let mut g = self.inner.lock();
        let cap = self.capacity;
        let evicted = if g.len == cap {
            // Full — replace the oldest slot and advance head.
            let displaced = g.buf[g.head].take();
            g.buf[g.head] = Some(value);
            g.head = (g.head + 1) % cap;
            displaced
        } else {
            // Not full — append at (head + len) mod cap.
            let idx = (g.head + g.len) % cap;
            g.buf[idx] = Some(value);
            g.len += 1;
            None
        };
        evicted
    }

    /// Drain every buffered entry in FIFO order. The buffer is empty
    /// afterwards.
    ///
    /// Used by the Loki shipper task on reconnect.
    pub fn drain(&self) -> Vec<T> {
        let mut g = self.inner.lock();
        let cap = self.capacity;
        let len = g.len;
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let idx = (g.head + i) % cap;
            if let Some(v) = g.buf[idx].take() {
                out.push(v);
            }
        }
        g.head = 0;
        g.len = 0;
        out
    }
}

impl<const N: usize, T> Default for BoundedRingLogBuffer<N, T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};
    use std::thread;

    #[test]
    fn degraded_flags_default_off() {
        // Note: `degraded_state()` is process-global. Other tests may have
        // toggled the flags; reset and observe.
        let s = degraded_state();
        s.set_loki_unavailable(false);
        s.set_jaeger_overloaded(false);
        assert!(!s.loki_unavailable());
        assert!(!s.jaeger_overloaded());
    }

    #[test]
    fn degraded_flags_transition_atomically_for_readers() {
        // Property: a Release write becomes visible to an Acquire read.
        // Use a worker thread to assert the visibility round-trip.
        let s = degraded_state();
        s.set_loki_unavailable(false);

        let observed = AtomicUsize::new(0);
        thread::scope(|scope| {
            let observed_ref = &observed;
            scope.spawn(move || {
                // Spin briefly until the writer flips the flag.
                for _ in 0..1_000_000 {
                    if loki_unavailable() {
                        observed_ref.store(1, AOrdering::Relaxed);
                        return;
                    }
                    std::hint::spin_loop();
                }
            });
            // Tiny back-off so the spawned thread starts spinning first.
            std::thread::yield_now();
            set_loki_unavailable(true);
        });
        assert_eq!(observed.load(AOrdering::Relaxed), 1);
        // Restore default for other tests in this process.
        set_loki_unavailable(false);
    }

    #[test]
    fn bounded_ring_drops_oldest_when_full() {
        // Property (task spec): `BoundedRingLogBuffer::<2>` drops the oldest
        // entry when full. The push that overflows returns that oldest value.
        let ring: BoundedRingLogBuffer<2, u32> = BoundedRingLogBuffer::new();
        assert_eq!(ring.capacity(), 2);
        assert!(ring.is_empty());

        assert_eq!(ring.push(1), None);
        assert_eq!(ring.push(2), None);
        assert!(ring.is_full());

        // Now full — push 3 evicts 1, push 4 evicts 2.
        assert_eq!(ring.push(3), Some(1));
        assert_eq!(ring.push(4), Some(2));
        assert!(ring.is_full());

        // Drain returns in FIFO order: 3, 4.
        let drained = ring.drain();
        assert_eq!(drained, vec![3, 4]);
        assert!(ring.is_empty());
    }

    #[test]
    fn bounded_ring_handles_partial_fill_drain() {
        let ring: BoundedRingLogBuffer<8, &'static str> = BoundedRingLogBuffer::new();
        ring.push("alpha");
        ring.push("beta");
        assert_eq!(ring.len(), 2);
        let drained = ring.drain();
        assert_eq!(drained, vec!["alpha", "beta"]);
        assert!(ring.is_empty());

        // Drain on empty is a no-op.
        assert!(ring.drain().is_empty());
    }

    #[test]
    fn bounded_ring_wraps_correctly_after_eviction_and_drain() {
        let ring: BoundedRingLogBuffer<3, u8> = BoundedRingLogBuffer::new();
        ring.push(1);
        ring.push(2);
        ring.push(3);
        assert_eq!(ring.push(4), Some(1));
        assert_eq!(ring.push(5), Some(2));
        // Buffer now contains 3, 4, 5 in order.
        let drained = ring.drain();
        assert_eq!(drained, vec![3, 4, 5]);
        // Subsequent pushes start fresh.
        ring.push(10);
        assert_eq!(ring.drain(), vec![10]);
    }

    #[test]
    #[should_panic(expected = "BoundedRingLogBuffer capacity must be > 0")]
    fn bounded_ring_zero_capacity_panics() {
        let _: BoundedRingLogBuffer<0, u8> = BoundedRingLogBuffer::new();
    }
}
