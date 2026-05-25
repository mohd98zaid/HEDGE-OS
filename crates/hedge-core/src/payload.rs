//! Bounded inline event payloads.
//!
//! The Orderflow_Engine emits up to four `OrderflowEvent` variants per
//! book update (design § Components — Orderflow_Engine), and other
//! Hot_Path stages emit similar small batches. Allocating a `Vec` per
//! batch would violate R2.6.
//!
//! [`BoundedEvents<T, N>`] wraps a `SmallVec<[T; N]>` with an explicit
//! [`try_push`](BoundedEvents::try_push) that returns
//! [`BoundedPushError`] when the inline capacity is exhausted —
//! making overflow **observable**, not silent. Callers that want to
//! drop on overflow can `let _ = events.try_push(e);`; callers that want
//! to surface the breach can emit `obs.error.<stage>.payload_overflow`.
//!
//! `SmallVec` permits spilling to the heap at higher capacities, but
//! `BoundedEvents` deliberately exposes only the inline-capacity API so
//! Hot_Path crates cannot accidentally trigger a spill.

use smallvec::SmallVec;

/// Returned by [`BoundedEvents::try_push`] when the inline capacity is
/// already at its maximum. The original value is returned so the caller
/// can decide whether to drop, requeue, or surface the failure.
///
/// Implemented manually (rather than via `#[derive(thiserror::Error)]`)
/// so the wrapper does not impose a `T: Debug` bound on `BoundedEvents`.
/// `BoundedPushError` is `Debug` when `T: Debug` and `Display + Error`
/// unconditionally.
pub struct BoundedPushError<T>(pub T);

impl<T> std::fmt::Display for BoundedPushError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BoundedEvents capacity exhausted")
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for BoundedPushError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoundedPushError").field(&self.0).finish()
    }
}

// `std::error::Error` requires `Debug + Display`. We satisfy `Display`
// unconditionally and require `T: Debug` for the `Error` impl so users
// can `?` their errors when their payload is debuggable.
impl<T: std::fmt::Debug> std::error::Error for BoundedPushError<T> {}

/// Inline event-batch container with capacity exactly `N`. Allocates
/// nothing in the steady state.
pub struct BoundedEvents<T, const N: usize> {
    inner: SmallVec<[T; N]>,
}

impl<T, const N: usize> BoundedEvents<T, N> {
    /// Construct an empty batch. Performs no heap allocation.
    #[inline]
    pub fn new() -> Self {
        Self { inner: SmallVec::new() }
    }

    /// Maximum number of items that fit without spilling.
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Current number of stored items.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// `true` when the batch is at capacity. The next [`try_push`] will
    /// fail rather than spilling onto the heap.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.inner.len() >= N
    }

    /// Append a value. Returns [`BoundedPushError`] when full so the
    /// overflow is explicit rather than silent or heap-spilling.
    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<(), BoundedPushError<T>> {
        if self.is_full() {
            Err(BoundedPushError(value))
        } else {
            self.inner.push(value);
            Ok(())
        }
    }

    /// Remove all elements without releasing inline capacity.
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Borrow the items as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        self.inner.as_slice()
    }

    /// Borrow the items as a mutable slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }

    /// Iterate the contained items in insertion order.
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.inner.iter()
    }
}

impl<T, const N: usize> Default for BoundedEvents<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> std::fmt::Debug for BoundedEvents<T, N>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedEvents")
            .field("capacity", &N)
            .field("len", &self.len())
            .field("items", &self.as_slice())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_push_succeeds_until_capacity_then_overflows() {
        let mut b: BoundedEvents<u32, 3> = BoundedEvents::new();
        assert!(b.is_empty());
        b.try_push(1).unwrap();
        b.try_push(2).unwrap();
        b.try_push(3).unwrap();
        assert!(b.is_full());

        // Property R2.6: overflow is observable, not silent.
        let err = b.try_push(4).unwrap_err();
        assert_eq!(err.0, 4);
        assert_eq!(b.len(), 3);
        assert_eq!(b.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn clear_empties_the_batch() {
        let mut b: BoundedEvents<u8, 2> = BoundedEvents::new();
        b.try_push(10).unwrap();
        b.try_push(20).unwrap();
        b.clear();
        assert!(b.is_empty());
        // Capacity is preserved.
        b.try_push(99).unwrap();
        assert_eq!(b.as_slice(), &[99]);
    }

    #[test]
    fn as_slice_and_iter_match_insertion_order() {
        let mut b: BoundedEvents<i32, 4> = BoundedEvents::new();
        for v in [-1, 0, 1, 2] {
            b.try_push(v).unwrap();
        }
        assert_eq!(b.as_slice(), &[-1, 0, 1, 2]);
        let collected: Vec<i32> = b.iter().copied().collect();
        assert_eq!(collected, vec![-1, 0, 1, 2]);
    }

    #[test]
    fn capacity_matches_const_generic() {
        let b: BoundedEvents<u8, 8> = BoundedEvents::new();
        assert_eq!(b.capacity(), 8);
    }

    #[test]
    fn debug_format_includes_items() {
        let mut b: BoundedEvents<u32, 2> = BoundedEvents::new();
        b.try_push(1).unwrap();
        b.try_push(2).unwrap();
        let s = format!("{:?}", b);
        assert!(s.contains("[1, 2]"), "debug missing items: {}", s);
    }
}
