//! Test harness for enforcing the no-allocation rule on Hot_Path code.
//!
//! Requirements R2.6 (Orderflow_Engine "SHALL process each orderbook
//! update without allocating heap memory in the steady-state path") and
//! R3.4 (Feature_Extraction_Engine "SHALL hold all live feature state
//! in-memory") demand that Hot_Path crates never allocate per-tick.
//!
//! [`assert_no_alloc`] wraps a closure in a [`stats_alloc::Region`] and
//! panics if the closure performed any allocation, deallocation, or
//! reallocation. Hot_Path test crates can use it like so:
//!
//! ```ignore
//! use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};
//! use std::alloc::System;
//!
//! #[global_allocator]
//! static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
//!
//! #[test]
//! fn orderflow_steady_state_is_alloc_free() {
//!     let mut engine = build_engine();
//!     hedge_core::alloc_harness::assert_no_alloc("orderflow tick", || {
//!         engine.process(&book);
//!     });
//! }
//! ```
//!
//! The full instrumentation only runs when the crate is built with the
//! `alloc-tracking` feature. Without the feature the helpers compile to
//! no-ops so release binaries pay zero cost.

#[cfg(feature = "alloc-tracking")]
use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};

#[cfg(feature = "alloc-tracking")]
use std::alloc::System;

/// Snapshot of allocator activity captured during a measurement window.
///
/// Available regardless of the `alloc-tracking` feature so callers can
/// exchange snapshots in tests without conditional compilation. When
/// the feature is off the values are always zero.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct AllocSnapshot {
    /// Number of distinct allocations performed.
    pub allocations: usize,
    /// Number of distinct deallocations performed.
    pub deallocations: usize,
    /// Number of distinct reallocations performed.
    pub reallocations: usize,
    /// Total bytes allocated.
    pub bytes_allocated: usize,
    /// Total bytes deallocated.
    pub bytes_deallocated: usize,
    /// Net bytes reallocated (positive = growing structures, negative
    /// = shrinking). Matches `stats_alloc::Stats::bytes_reallocated`,
    /// which is `isize` for that reason.
    pub bytes_reallocated: isize,
}

impl AllocSnapshot {
    /// `true` when the snapshot recorded zero allocator activity.
    #[inline]
    pub fn is_quiet(&self) -> bool {
        self.allocations == 0 && self.deallocations == 0 && self.reallocations == 0
    }
}

/// Run `f` with allocation accounting and return the snapshot.
///
/// When the `alloc-tracking` feature is **not** enabled the snapshot is
/// unconditionally zero. Tests that depend on real measurements must
/// enable the feature via `--features alloc-tracking`.
#[inline]
pub fn measure_alloc<F: FnOnce()>(f: F) -> AllocSnapshot {
    #[cfg(feature = "alloc-tracking")]
    {
        let reg = Region::new(GLOBAL_ALLOC);
        f();
        let stats = reg.change();
        AllocSnapshot {
            allocations: stats.allocations,
            deallocations: stats.deallocations,
            reallocations: stats.reallocations,
            bytes_allocated: stats.bytes_allocated,
            bytes_deallocated: stats.bytes_deallocated,
            bytes_reallocated: stats.bytes_reallocated,
        }
    }

    #[cfg(not(feature = "alloc-tracking"))]
    {
        f();
        AllocSnapshot::default()
    }
}

/// Panic if `f` performs any heap allocation.
///
/// `name` is included in the panic message so a failing CI log points
/// at the offending Hot_Path stage. When the `alloc-tracking` feature
/// is disabled this becomes a no-op (the snapshot is always quiet).
#[inline]
pub fn assert_no_alloc<F: FnOnce()>(name: &str, f: F) {
    let snap = measure_alloc(f);
    assert!(
        snap.is_quiet(),
        "{} performed heap allocation under the no-alloc harness: {:?}",
        name,
        snap
    );
}

/// Allocator handle used by [`measure_alloc`]. Crates that opt into
/// allocation tracking must register `INSTRUMENTED_SYSTEM` as the
/// `#[global_allocator]` and re-export it through this module by
/// activating the `alloc-tracking` feature.
#[cfg(feature = "alloc-tracking")]
pub static GLOBAL_ALLOC: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_snapshot_is_quiet_by_default() {
        // Default is "no activity"; this anchors the helper.
        let s = AllocSnapshot::default();
        assert!(s.is_quiet());
    }

    #[test]
    fn assert_no_alloc_passes_for_pure_integer_arithmetic() {
        // With `alloc-tracking` disabled this is a no-op; with it
        // enabled, integer math allocates nothing on stable Rust.
        assert_no_alloc("pure arithmetic", || {
            let mut x: u64 = 1;
            for i in 1..1000u64 {
                x = x.wrapping_add(i);
            }
            std::hint::black_box(x);
        });
    }

    #[test]
    fn measure_alloc_returns_quiet_snapshot_for_no_op() {
        let s = measure_alloc(|| {});
        assert!(s.is_quiet());
    }
}
