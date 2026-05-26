//! Identifier newtypes used across the Hot_Path.
//!
//! The design (Common Types section) declares these as raw type aliases, but
//! we wrap them in `Copy` newtypes to gain type-level distinction between a
//! `CorrelationId` and a `SessionId`. The `#[repr(transparent)]` attribute
//! preserves the FlatBuffers layout so the on-wire representation is byte
//! identical to the design's `[ubyte:16]` and `uint`/`u64` declarations.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Globally unique 128-bit ULID used to correlate every event spawned from a
/// single tick all the way through to the broker fill (R27.4, design "Latency
/// Budget Allocation" — every stage stamps the same `correlation_id`).
///
/// Stored as the underlying `u128` for cache-line friendliness and so it can
/// be packed into FlatBuffers `[ubyte:16]` slots without endian conversion.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CorrelationId(pub u128);

impl CorrelationId {
    /// Mints a fresh ULID-derived `CorrelationId`.
    ///
    /// `ulid::Ulid::new()` reads the system clock and a per-process RNG.
    /// It performs no heap allocation and is safe to call on the hot path.
    #[inline]
    pub fn new() -> Self {
        Self(Ulid::new().0)
    }

    /// Construct from a pre-existing `Ulid` (used by replay code paths that
    /// must reproduce IDs deterministically).
    #[inline]
    pub const fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid.0)
    }

    /// Borrow the inner 128-bit value (e.g. for FlatBuffers serialization).
    #[inline]
    pub const fn as_u128(self) -> u128 {
        self.0
    }

    /// Sentinel "nil" ID, equivalent to `Ulid::nil()`. Useful as a default
    /// for fields that have not yet been populated.
    pub const NIL: Self = Self(0);
}

impl Default for CorrelationId {
    /// Returns [`CorrelationId::NIL`]. Real IDs must be minted via
    /// [`CorrelationId::new`] — this default exists only so the type can be
    /// embedded in `#[derive(Default)]` structs.
    #[inline]
    fn default() -> Self {
        Self::NIL
    }
}

impl From<Ulid> for CorrelationId {
    #[inline]
    fn from(value: Ulid) -> Self {
        Self::from_ulid(value)
    }
}

/// 32-bit interned symbol identifier. The string symbol (e.g. "RELIANCE") is
/// resolved once at startup and afterwards every Hot_Path event carries only
/// this u32 — keeping FlatBuffers payloads cache-friendly.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SymbolId(pub u32);

impl SymbolId {
    /// Construct from a raw u32 produced by the symbol interner.
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// Recover the raw u32 (for FlatBuffers serialization).
    #[inline]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

/// 64-bit Trading_Session identifier — usually a date-derived monotonic
/// counter (e.g. `YYYYMMDD` packed into u64). Used by the Replay_Engine and
/// the Memory_RAG_Layer to scope per-session records.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SessionId(pub u64);

impl SessionId {
    /// Construct from a raw u64.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Recover the raw u64.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// 64-bit unsigned quantity — number of shares / contracts in an order or
/// fill. Wrapped in a newtype so a `Qty` cannot be accidentally added to a
/// price (`Px`) or to a session id. Matches the design's
/// `Tick_v1.ltq`, `OrderIntent_v1.quantity`, and `OrderState_v1.filled_qty`
/// fields, which are all FlatBuffers `ulong`.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Qty(pub u64);

impl Qty {
    /// Zero quantity.
    pub const ZERO: Self = Self(0);

    /// Construct from a raw u64.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Recover the raw u64.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Saturating addition. Use on the Risk_Engine path where overflow
    /// must never panic.
    #[inline]
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating subtraction.
    #[inline]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    /// Checked addition.
    #[inline]
    pub const fn checked_add(self, rhs: Self) -> Option<Self> {
        match self.0.checked_add(rhs.0) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn correlation_id_new_is_unique_across_1000_calls() {
        // Property R27.4: every event in a tick → trade chain must carry the
        // same correlation_id. A precondition is that fresh IDs are unique.
        let mut seen = HashSet::with_capacity(1024);
        for _ in 0..1000 {
            let id = CorrelationId::new();
            assert!(seen.insert(id), "ULID collision detected at id={:032x}", id.0);
        }
        assert_eq!(seen.len(), 1000);
    }

    #[test]
    fn correlation_id_default_is_nil() {
        assert_eq!(CorrelationId::default(), CorrelationId::NIL);
        assert_eq!(CorrelationId::NIL.as_u128(), 0u128);
    }

    #[test]
    fn correlation_id_from_ulid_round_trip() {
        let ulid = Ulid::new();
        let cid: CorrelationId = ulid.into();
        assert_eq!(cid.as_u128(), ulid.0);
    }

    #[test]
    fn symbol_id_round_trip() {
        let s = SymbolId::new(42);
        assert_eq!(s.raw(), 42);
        assert_eq!(SymbolId::new(0).raw(), 0);
    }

    #[test]
    fn session_id_round_trip() {
        let s = SessionId::new(20251130);
        assert_eq!(s.raw(), 20251130);
    }

    #[test]
    fn qty_round_trip_and_arithmetic() {
        let a = Qty::new(100);
        let b = Qty::new(40);
        assert_eq!(a.raw(), 100);
        assert_eq!(a.saturating_add(b).raw(), 140);
        assert_eq!(a.saturating_sub(b).raw(), 60);
        // Saturating sub clamps at zero rather than underflowing.
        assert_eq!(b.saturating_sub(a).raw(), 0);
        assert_eq!(Qty::ZERO.raw(), 0);
        assert_eq!(Qty::default(), Qty::ZERO);
    }

    #[test]
    fn qty_checked_add_detects_overflow() {
        let big = Qty::new(u64::MAX);
        assert!(big.checked_add(Qty::new(1)).is_none());
        assert_eq!(Qty::new(10).checked_add(Qty::new(5)).unwrap().raw(), 15);
    }

    #[test]
    fn ids_are_copy_and_hashable() {
        // Compile-time enforcement; the assertions below just exercise the
        // traits at runtime so a regression in the `#[derive]` list shows up
        // as a test failure instead of a silent ABI shift.
        fn assert_copy<T: Copy>() {}
        fn assert_hash<T: std::hash::Hash>() {}
        assert_copy::<CorrelationId>();
        assert_copy::<SymbolId>();
        assert_copy::<SessionId>();
        assert_copy::<Qty>();
        assert_hash::<CorrelationId>();
        assert_hash::<SymbolId>();
        assert_hash::<SessionId>();
        assert_hash::<Qty>();
    }

    #[test]
    fn correlation_id_repr_is_transparent_u128() {
        // Guarantees zero-cost FlatBuffers transmutation.
        assert_eq!(
            std::mem::size_of::<CorrelationId>(),
            std::mem::size_of::<u128>()
        );
    }
}
