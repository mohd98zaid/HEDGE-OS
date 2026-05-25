//! Symbol interner.
//!
//! The Hot_Path FlatBuffers payloads (`Tick_v1.symbol`, `Signal_v1.symbol`,
//! ...) carry a 32-bit [`SymbolId`] rather than the heap-allocated string
//! ticker. The interner is the single source of truth that maps an exchange
//! ticker symbol (`"RELIANCE"`) to its stable `SymbolId(42)` for the
//! duration of the process.
//!
//! ### Concurrency
//!
//! Backed by a [`dashmap::DashMap`] so concurrent ingestion tasks (one per
//! upstream feed) can intern in parallel without serialising on a single
//! lock. The id-allocation counter is an [`AtomicU32`] for the same
//! reason. The [`SymbolInterner::intern`] method uses `entry().or_insert_with`
//! so a symbol intern races safely — only one task observes the
//! `or_insert_with` callback for any given symbol, and every other concurrent
//! call observes the same allocated id.

use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;
use hedge_core::SymbolId;

/// Concurrent symbol → [`SymbolId`] interner.
///
/// The interner is `Send + Sync` and intended to live behind an `Arc` so
/// every adapter task and the engine itself share the same id space.
#[derive(Debug)]
pub struct SymbolInterner {
    map: DashMap<String, SymbolId>,
    next: AtomicU32,
}

impl SymbolInterner {
    /// Construct an empty interner.
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            // We deliberately start at 1 so a `SymbolId(0)` value can be
            // used as a sentinel by downstream Hot_Path code (e.g. an
            // unfilled FlatBuffers field).
            next: AtomicU32::new(1),
        }
    }

    /// Resolve `sym` to a stable [`SymbolId`].
    ///
    /// * If `sym` has been seen before, the previously allocated id is
    ///   returned (idempotent).
    /// * Otherwise a fresh id is allocated atomically and inserted.
    ///
    /// The lookup is O(1) average. Allocation only happens once per unique
    /// symbol, so the steady-state path (every Hot_Path tick after warm-up)
    /// is allocation-free for already-known symbols.
    pub fn intern(&self, sym: &str) -> SymbolId {
        if let Some(existing) = self.map.get(sym) {
            return *existing;
        }
        // Slow path: allocate a candidate id and insert via `entry()` so a
        // concurrent intern for the same symbol observes a single id.
        let candidate = SymbolId::new(self.next.fetch_add(1, Ordering::Relaxed));
        let entry = self.map.entry(sym.to_string()).or_insert(candidate);
        *entry
    }

    /// Returns the [`SymbolId`] for `sym` if it has been interned before,
    /// without allocating a new id.
    pub fn get(&self, sym: &str) -> Option<SymbolId> {
        self.map.get(sym).map(|v| *v)
    }

    /// Number of distinct symbols currently interned.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the interner has yet allocated any ids.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for SymbolInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn intern_returns_same_id_for_same_string() {
        let i = SymbolInterner::new();
        let a = i.intern("RELIANCE");
        let b = i.intern("RELIANCE");
        assert_eq!(a, b);
        assert_eq!(i.len(), 1);
    }

    #[test]
    fn distinct_symbols_get_distinct_ids() {
        let i = SymbolInterner::new();
        let r = i.intern("RELIANCE");
        let t = i.intern("TCS");
        let h = i.intern("HDFCBANK");
        assert_ne!(r, t);
        assert_ne!(t, h);
        assert_ne!(r, h);
        assert_eq!(i.len(), 3);
    }

    #[test]
    fn first_id_is_one_so_zero_remains_sentinel() {
        let i = SymbolInterner::new();
        let id = i.intern("RELIANCE");
        // SymbolId(0) is reserved; the first allocation is 1.
        assert_eq!(id.raw(), 1);
    }

    #[test]
    fn get_returns_none_for_unknown_symbol() {
        let i = SymbolInterner::new();
        assert!(i.get("UNKNOWN").is_none());
        i.intern("KNOWN");
        assert!(i.get("UNKNOWN").is_none());
        assert!(i.get("KNOWN").is_some());
    }

    #[test]
    fn concurrent_intern_of_same_symbol_yields_one_id() {
        // Property: a race between two threads interning the same symbol
        // never produces two distinct ids.
        let interner = Arc::new(SymbolInterner::new());
        let mut handles = Vec::new();
        for _ in 0..16 {
            let i = Arc::clone(&interner);
            handles.push(thread::spawn(move || i.intern("RELIANCE")));
        }
        let ids: Vec<SymbolId> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let first = ids[0];
        for id in &ids {
            assert_eq!(*id, first, "concurrent intern produced different ids");
        }
        assert_eq!(interner.len(), 1);
    }
}
