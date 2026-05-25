//! `hedge-core`
//!
//! Foundational primitives shared by every Hot_Path crate in PROJECT HEDGE.
//!
//! This crate is task **2.1** of the implementation plan and provides:
//!
//! * **Identifiers** — `CorrelationId` (u128 ULID), `SymbolId` (u32),
//!   `SessionId` (u64), `Qty` (u64).
//! * **Price arithmetic** — [`Px`], a fixed-decimal `i64` newtype expressed
//!   in **paise** with non-allocating `+`, `-`, scalar `*`, and scalar `/`.
//!   No floating point; no allocation.
//! * **Enums** — `Side`, `Regime`, `BrokerId`, `Priority`, all `#[repr(u8)]`
//!   for FlatBuffers compatibility (R1.5).
//! * **Clock helpers** — a `quanta::Instant` based monotonic clock and two
//!   RAII `LatencyTimer` flavours (callback and atomic).
//! * **Ring buffers** — bounded MPMC and unbounded MPSC wrappers around
//!   `crossbeam` queues plus a const-generic `MpscRing<T, N>`.
//! * **Inline windows** — [`window::RingWindow`], a no-alloc inline-array
//!   incremental feature buffer (R3.4).
//! * **Bounded payloads** — [`payload::BoundedEvents`], a `SmallVec`-backed
//!   collection with explicit `try_push` overflow handling (R2.6).
//! * **Allocation harness** — [`alloc_harness::assert_no_alloc`] for tests
//!   that gate hot-loop code on the `stats_alloc` global allocator.
//!
//! Prohibitions enforced by downstream CI (task **8.1**, R30):
//!
//! * No dependency on `pyo3`, `numpy`, `pandas`, or any Python runtime.
//! * No blocking external HTTP on the per-tick path.
//! * No LLM inference on the per-tick path.

#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

pub mod alloc_harness;
pub mod clock;
pub mod enums;
pub mod ids;
pub mod payload;
pub mod price;
pub mod ring;
pub mod window;

// Public API re-exports — every Hot_Path crate imports from `hedge_core::*`.
pub use clock::{now_ns, AtomicLatencyTimer, CallbackLatencyTimer, LatencyTimer};
pub use enums::{BrokerId, Priority, Regime, Side};
pub use ids::{CorrelationId, Qty, SessionId, SymbolId};
pub use payload::{BoundedEvents, BoundedPushError};
pub use price::Px;
pub use ring::{MpmcRing, MpscRing, UnboundedRing};
pub use window::RingWindow;

// Convenience re-exports of the inline-storage primitives that downstream
// Hot_Path crates compose on top of `hedge-core`. Re-exporting here pins
// every crate to the same `arrayvec` / `smallvec` versions selected by the
// workspace and lets call sites write `use hedge_core::ArrayVec;` instead
// of pulling the crates in transitively.
pub use arrayvec::ArrayVec;
pub use smallvec::SmallVec;
