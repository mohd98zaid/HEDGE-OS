//! Lock-free ring buffer wrappers used by the Hot_Path.
//!
//! The Hot_Path (R1.4, R29.2) routes events between stages with lock-free
//! data structures. This module exposes ergonomic, typed wrappers around
//! the relevant `crossbeam::queue` primitives:
//!
//! * [`MpmcRing<T>`] — bounded multi-producer multi-consumer queue
//!   (wraps `crossbeam_queue::ArrayQueue`). Capacity is fixed at
//!   construction; `push` returns the original value back when full so
//!   callers can apply backpressure rather than allocate.
//! * [`MpscRing<T, N>`] — convenience newtype that pins the ring's
//!   capacity at the type level via a `const N: usize`. Internally still
//!   `ArrayQueue`-backed; the const generic only documents the intent
//!   that exactly one consumer drains the ring.
//! * [`UnboundedRing<T>`] — unbounded MPSC for low-frequency control
//!   plane events (`ops.*`, `risk.killswitch.activated`, ...) where
//!   blocking the producer is unacceptable but volume is low. Wraps
//!   `crossbeam_queue::SegQueue`. **Not** for steady-state Hot_Path use.
//!
//! ## No-allocation property
//!
//! The bounded rings preallocate their backing storage in
//! [`MpmcRing::with_capacity`] / [`MpscRing::new`]. After construction,
//! `push` and `pop` perform zero allocations — exactly what R2.6 and
//! R3.4 require for the steady-state Orderflow_Engine and
//! Feature_Extraction_Engine paths.

use std::sync::Arc;

use crossbeam::queue::{ArrayQueue, SegQueue};

/// Bounded lock-free MPMC ring. Construction allocates the backing
/// `ArrayQueue` once; subsequent operations are allocation-free.
///
/// Cloning the ring shares the same backing storage via `Arc` — clones
/// are cheap and intended as the way to give multiple producers /
/// consumers access to the same ring.
pub struct MpmcRing<T> {
    inner: Arc<ArrayQueue<T>>,
}

impl<T> MpmcRing<T> {
    /// Creates a new ring with capacity for `cap` items.
    ///
    /// # Panics
    ///
    /// Panics if `cap == 0` — a zero-capacity ring is never useful.
    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap > 0, "ring capacity must be > 0");
        Self {
            inner: Arc::new(ArrayQueue::new(cap)),
        }
    }

    /// Push an item. Returns `Err(value)` when the ring is full,
    /// allowing the caller to back-pressure or drop without allocating.
    #[inline]
    pub fn push(&self, value: T) -> Result<(), T> {
        self.inner.push(value)
    }

    /// Pop the oldest item. Returns `None` when empty.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.inner.pop()
    }

    /// Maximum number of items the ring can hold.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Number of items currently stored. Best-effort under concurrent
    /// access — useful for metrics and tests, not for control flow.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when no items are stored.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// `true` when the ring is at capacity.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }
}

impl<T> Clone for MpmcRing<T> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<T> std::fmt::Debug for MpmcRing<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MpmcRing")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .finish()
    }
}

/// Const-generic MPSC ring. Capacity is encoded in the type so a typo
/// at construction (`new(64)` vs `new(640)`) cannot silently shift the
/// memory footprint of a Hot_Path stage.
///
/// This is structurally identical to [`MpmcRing`] — the const-generic
/// variant exists so engines that want to declare *"this stage owns a
/// ring of exactly N elements"* can do so at the type level. The
/// `_N` size is exposed via [`MpscRing::CAPACITY`].
pub struct MpscRing<T, const N: usize> {
    inner: MpmcRing<T>,
}

impl<T, const N: usize> MpscRing<T, N> {
    /// The compile-time capacity declared via the `N` const generic.
    pub const CAPACITY: usize = N;

    /// Construct an empty ring of exactly `N` elements.
    ///
    /// # Panics
    ///
    /// Panics if `N == 0`.
    pub fn new() -> Self {
        Self {
            inner: MpmcRing::with_capacity(N),
        }
    }

    /// Push an item. Returns `Err(value)` if the ring is full.
    #[inline]
    pub fn push(&self, value: T) -> Result<(), T> {
        self.inner.push(value)
    }

    /// Pop the oldest item.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.inner.pop()
    }

    /// Capacity (always `N`).
    #[inline]
    pub fn capacity(&self) -> usize {
        N
    }

    /// Approximate length under concurrent access.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when no items are stored.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// `true` when the ring is at capacity.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }
}

impl<T, const N: usize> Default for MpscRing<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> Clone for MpscRing<T, N> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T, const N: usize> std::fmt::Debug for MpscRing<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MpscRing")
            .field("capacity", &N)
            .field("len", &self.len())
            .finish()
    }
}

/// Unbounded MPSC ring backed by `crossbeam::queue::SegQueue`. **Not for
/// steady-state Hot_Path use** — `SegQueue` allocates segments on demand.
/// Reserved for low-frequency control-plane channels where unbounded
/// growth is preferable to dropping events.
pub struct UnboundedRing<T> {
    inner: Arc<SegQueue<T>>,
}

impl<T> UnboundedRing<T> {
    /// Construct an empty unbounded ring.
    pub fn new() -> Self {
        Self { inner: Arc::new(SegQueue::new()) }
    }

    /// Push an item; never blocks, never allocates from the caller's
    /// perspective during steady-state use of the same segment.
    #[inline]
    pub fn push(&self, value: T) {
        self.inner.push(value);
    }

    /// Pop the oldest item.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.inner.pop()
    }

    /// Approximate length.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when no items are stored.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T> Default for UnboundedRing<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for UnboundedRing<T> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl<T> std::fmt::Debug for UnboundedRing<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnboundedRing")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn mpmc_ring_push_pop_preserves_fifo_order() {
        // Property: single-producer / single-consumer push & pop preserves
        // insertion order — this is the contract Signal_Engine relies on.
        let ring = MpmcRing::with_capacity(8);
        for i in 0u32..5 {
            ring.push(i).unwrap();
        }
        assert_eq!(ring.len(), 5);
        for i in 0u32..5 {
            assert_eq!(ring.pop(), Some(i));
        }
        assert_eq!(ring.pop(), None);
        assert!(ring.is_empty());
    }

    #[test]
    fn mpmc_ring_push_returns_value_when_full() {
        let ring = MpmcRing::with_capacity(2);
        ring.push(1u8).unwrap();
        ring.push(2u8).unwrap();
        assert!(ring.is_full());
        // Backpressure: the original value is returned to the caller.
        let err = ring.push(3u8).unwrap_err();
        assert_eq!(err, 3);
        assert_eq!(ring.len(), 2);
    }

    #[test]
    #[should_panic(expected = "ring capacity must be > 0")]
    fn mpmc_ring_zero_capacity_panics() {
        let _: MpmcRing<u8> = MpmcRing::with_capacity(0);
    }

    #[test]
    fn mpsc_ring_capacity_matches_const_generic() {
        let ring: MpscRing<u32, 16> = MpscRing::new();
        assert_eq!(ring.capacity(), 16);
        assert_eq!(MpscRing::<u32, 16>::CAPACITY, 16);
        assert!(ring.is_empty());
    }

    #[test]
    fn mpsc_ring_push_pop_ordering_under_single_producer() {
        let ring: MpscRing<u64, 32> = MpscRing::new();
        for i in 0u64..10 {
            ring.push(i).unwrap();
        }
        for i in 0u64..10 {
            assert_eq!(ring.pop(), Some(i));
        }
    }

    #[test]
    fn mpsc_ring_overflow_returns_value() {
        let ring: MpscRing<u8, 2> = MpscRing::new();
        ring.push(1).unwrap();
        ring.push(2).unwrap();
        assert!(ring.is_full());
        assert_eq!(ring.push(3), Err(3));
    }

    #[test]
    fn mpmc_ring_supports_concurrent_producers() {
        let ring = MpmcRing::with_capacity(4096);
        let counter = AtomicUsize::new(0);
        thread::scope(|s| {
            for tid in 0..4u32 {
                let r = ring.clone();
                let c = &counter;
                s.spawn(move || {
                    for i in 0..256u32 {
                        let v = (tid * 1000) + i;
                        if r.push(v).is_ok() {
                            c.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        let pushed = counter.load(Ordering::Relaxed);
        assert_eq!(pushed, 1024);
        let mut drained = 0usize;
        while ring.pop().is_some() {
            drained += 1;
        }
        assert_eq!(drained, pushed);
    }

    #[test]
    fn unbounded_ring_grows_past_initial_segment() {
        let ring = UnboundedRing::<u32>::new();
        for i in 0..10_000 {
            ring.push(i);
        }
        assert_eq!(ring.len(), 10_000);
        for i in 0..10_000 {
            assert_eq!(ring.pop(), Some(i));
        }
        assert!(ring.is_empty());
    }

    #[test]
    fn rings_are_clone_to_share_backing_storage() {
        let a = MpmcRing::with_capacity(4);
        let b = a.clone();
        a.push(7u32).unwrap();
        // Cloned handle sees the same item — shared backing storage.
        assert_eq!(b.pop(), Some(7));
        assert!(a.is_empty() && b.is_empty());
    }
}
