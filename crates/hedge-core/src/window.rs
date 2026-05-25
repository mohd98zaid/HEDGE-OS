//! No-allocation rolling window for incremental feature buffers.
//!
//! The Feature_Extraction_Engine (R3.1, R3.2) computes VWAP, ATR, EMA,
//! realized volatility, momentum, etc. **incrementally** — every new
//! tick or book update slides a window forward by one element. Storing
//! that window on the heap would violate R3.4 ("hold all live feature
//! state in-memory"... the design pinpoints `RingWindow` on `ArrayVec`
//! storage so the steady-state path is allocation-free).
//!
//! [`RingWindow<T, N>`] keeps `N` slots inline as `[T; N]`. The window
//! starts empty; once `N` elements have been pushed it overwrites the
//! oldest in place. All methods are O(1) and allocation-free.
//!
//! # Iteration order
//!
//! [`iter`](RingWindow::iter) and [`iter_recent`](RingWindow::iter_recent)
//! yield items in **insertion order**, oldest first. So the most-recently
//! pushed element is always last. This matches the natural reading of
//! a rolling window in feature math: `iter().sum()` averages over the
//! whole window in time order.

use std::iter::Chain;
use std::slice::Iter;

/// Inline rolling window of `N` elements with FIFO overwrite-on-full
/// semantics. `T: Copy + Default` because the backing storage is a
/// `[T; N]` array initialised at construction.
pub struct RingWindow<T: Copy + Default, const N: usize> {
    /// Backing storage. Slot `i` is "valid" iff `i < len` (during the
    /// initial fill) or `len == N` (steady state, every slot valid).
    buf: [T; N],
    /// Index of the oldest valid element. After steady state, this is
    /// also the slot where the next `push` will overwrite.
    head: usize,
    /// Number of valid elements currently stored. Saturates at `N`.
    len: usize,
}

impl<T: Copy + Default, const N: usize> RingWindow<T, N> {
    /// Create an empty window. The backing array is filled with
    /// `T::default()` but those slots are not considered valid until
    /// they are written to via [`push`](RingWindow::push).
    ///
    /// # Panics
    ///
    /// Panics if `N == 0` — a zero-length window cannot store anything
    /// and is almost certainly a programmer error.
    #[inline]
    pub fn new() -> Self {
        assert!(N > 0, "RingWindow capacity N must be > 0");
        Self {
            buf: [T::default(); N],
            head: 0,
            len: 0,
        }
    }

    /// Maximum number of elements the window holds.
    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Current number of valid elements.
    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// `true` when no elements have been pushed.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// `true` once `N` elements have been pushed and the window is
    /// overwriting in place on every subsequent push.
    #[inline]
    pub const fn is_full(&self) -> bool {
        self.len == N
    }

    /// Push a value. When the window is full, the oldest element is
    /// silently overwritten — that is the entire point of a rolling
    /// window. Performs zero heap allocation.
    #[inline]
    pub fn push(&mut self, value: T) {
        if self.is_full() {
            // Steady state: head points to the slot we are about to
            // overwrite, and that slot is the *current oldest*. After
            // writing, advance head so the next-oldest takes its place.
            self.buf[self.head] = value;
            self.head = (self.head + 1) % N;
        } else {
            // Filling phase: slots fill from index 0 upward; head stays
            // at 0 because nothing has wrapped yet.
            let idx = (self.head + self.len) % N;
            self.buf[idx] = value;
            self.len += 1;
        }
    }

    /// Reset the window to empty. The backing storage is **not** zeroed
    /// out; the unread slots are simply marked invalid via `len = 0`.
    /// This is allocation-free and constant time.
    #[inline]
    pub fn clear(&mut self) {
        self.head = 0;
        self.len = 0;
    }

    /// Most recently pushed element, or `None` when empty.
    #[inline]
    pub fn latest(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            // Most-recent = the slot just before "next write".
            let idx = (self.head + self.len + N - 1) % N;
            Some(&self.buf[idx])
        }
    }

    /// Oldest element still in the window, or `None` when empty.
    #[inline]
    pub fn oldest(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self.buf[self.head])
        }
    }

    /// Iterate over every valid element, oldest first.
    ///
    /// The iterator borrows from the inline buffer — no allocation. Two
    /// chained `slice::Iter` segments are returned because a wrapped
    /// window is stored in two contiguous halves of the backing array.
    #[inline]
    pub fn iter(&self) -> Chain<Iter<'_, T>, Iter<'_, T>> {
        if self.is_full() {
            // [head .. N) followed by [0 .. head)
            let (lo, hi) = self.buf.split_at(self.head);
            hi.iter().chain(lo.iter())
        } else {
            // [head .. head + len)  — never wraps during fill
            let end = self.head + self.len;
            let valid = &self.buf[self.head..end];
            // Build a `Chain` of `valid` and an empty slice so the return
            // type matches the `is_full` branch.
            valid.iter().chain(self.buf[0..0].iter())
        }
    }

    /// Iterate over the `k` most recent elements (oldest of those first).
    /// If `k >= self.len()` this yields the same as [`iter`](Self::iter).
    /// If `k == 0` the returned iterator yields nothing.
    #[inline]
    pub fn iter_recent(&self, k: usize) -> impl Iterator<Item = &T> {
        let take = k.min(self.len);
        let skip = self.len - take;
        self.iter().skip(skip)
    }
}

impl<T: Copy + Default, const N: usize> Default for RingWindow<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + Default + std::fmt::Debug, const N: usize> std::fmt::Debug for RingWindow<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingWindow")
            .field("capacity", &N)
            .field("len", &self.len)
            .field("items", &self.iter().copied().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_window_has_no_latest_or_oldest() {
        let w: RingWindow<i64, 4> = RingWindow::new();
        assert!(w.is_empty());
        assert!(!w.is_full());
        assert_eq!(w.len(), 0);
        assert_eq!(w.latest(), None);
        assert_eq!(w.oldest(), None);
        assert!(w.iter().next().is_none());
    }

    #[test]
    fn push_until_full_preserves_insertion_order() {
        let mut w: RingWindow<u32, 5> = RingWindow::new();
        for i in 1..=5u32 {
            w.push(i);
        }
        assert!(w.is_full());
        assert_eq!(w.len(), 5);
        assert_eq!(w.oldest(), Some(&1));
        assert_eq!(w.latest(), Some(&5));
        let collected: Vec<u32> = w.iter().copied().collect();
        assert_eq!(collected, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn overwrite_on_full_drops_oldest_first() {
        // Property: once full, push overwrites the oldest element.
        let mut w: RingWindow<u32, 3> = RingWindow::new();
        for i in 1..=5u32 {
            w.push(i);
        }
        // After 5 pushes, the window should hold [3, 4, 5].
        assert!(w.is_full());
        assert_eq!(w.oldest(), Some(&3));
        assert_eq!(w.latest(), Some(&5));
        let collected: Vec<u32> = w.iter().copied().collect();
        assert_eq!(collected, vec![3, 4, 5]);
    }

    #[test]
    fn iter_recent_returns_last_k_in_order() {
        let mut w: RingWindow<u32, 5> = RingWindow::new();
        for i in 1..=5u32 {
            w.push(i);
        }
        let last3: Vec<u32> = w.iter_recent(3).copied().collect();
        assert_eq!(last3, vec![3, 4, 5]);
    }

    #[test]
    fn iter_recent_with_k_larger_than_len_returns_full_window() {
        let mut w: RingWindow<u32, 5> = RingWindow::new();
        w.push(10);
        w.push(20);
        let v: Vec<u32> = w.iter_recent(99).copied().collect();
        assert_eq!(v, vec![10, 20]);
    }

    #[test]
    fn iter_recent_with_k_zero_yields_nothing() {
        let mut w: RingWindow<u32, 4> = RingWindow::new();
        w.push(1);
        w.push(2);
        let v: Vec<u32> = w.iter_recent(0).copied().collect();
        assert!(v.is_empty());
    }

    #[test]
    fn clear_resets_to_empty_state() {
        let mut w: RingWindow<u32, 3> = RingWindow::new();
        w.push(1);
        w.push(2);
        w.push(3);
        w.push(4); // window now wrapped
        w.clear();
        assert!(w.is_empty());
        assert_eq!(w.latest(), None);
        // Pushing after clear restarts insertion order.
        w.push(99);
        assert_eq!(w.oldest(), Some(&99));
        assert_eq!(w.latest(), Some(&99));
        let v: Vec<u32> = w.iter().copied().collect();
        assert_eq!(v, vec![99]);
    }

    #[test]
    fn capacity_matches_const_generic() {
        let w: RingWindow<i32, 7> = RingWindow::new();
        assert_eq!(w.capacity(), 7);
    }

    #[test]
    fn is_full_reports_correctly_during_fill_and_after_wrap() {
        let mut w: RingWindow<u8, 2> = RingWindow::new();
        assert!(!w.is_full());
        w.push(1);
        assert!(!w.is_full());
        w.push(2);
        assert!(w.is_full());
        w.push(3); // overwrites
        assert!(w.is_full());
    }

    #[test]
    #[should_panic(expected = "RingWindow capacity N must be > 0")]
    fn zero_capacity_panics() {
        let _: RingWindow<u32, 0> = RingWindow::new();
    }

    #[test]
    fn iter_after_many_wraps_remains_correct() {
        // Push 100 values into a window of size 4; iteration should
        // always produce the most recent 4 in order.
        let mut w: RingWindow<u32, 4> = RingWindow::new();
        for i in 0..100u32 {
            w.push(i);
        }
        let v: Vec<u32> = w.iter().copied().collect();
        assert_eq!(v, vec![96, 97, 98, 99]);
        assert_eq!(w.latest(), Some(&99));
        assert_eq!(w.oldest(), Some(&96));
    }

    #[test]
    fn ring_window_is_copy_default_safe() {
        // Compile-time check via the bound; runtime sanity check.
        let w: RingWindow<i64, 8> = RingWindow::default();
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn debug_format_lists_window_contents_in_order() {
        let mut w: RingWindow<u8, 3> = RingWindow::new();
        w.push(1);
        w.push(2);
        w.push(3);
        let s = format!("{:?}", w);
        assert!(s.contains("[1, 2, 3]"), "debug missing items: {}", s);
    }
}
