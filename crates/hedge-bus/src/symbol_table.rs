//! Cross-process symbol → SymbolId interning.
//!
//! Phase B of the full-cockpit-data spec needs `upstox-feed` and the
//! Hot_Path engines to agree on a small integer id per trading symbol so
//! the binary `Tick_v1` wire format can carry just `u32` instead of a
//! variable-width string.
//!
//! ### V1: static table
//!
//! For the 5–50 symbol baskets we trade today, a static `match` is the
//! lowest-overhead, lowest-risk option. Adding a symbol means editing
//! this file and rebuilding every consumer; that's acceptable while the
//! basket is small.
//!
//! ### V2 (deferred): Redis-backed dynamic table
//!
//! When the basket grows past ~50 symbols the static table will swap to
//! a Redis SETNX-based id generator with an in-process LRU cache. The
//! API in this module won't change.

/// Map a trading symbol to its stable `u32` id. Returns `0` for unknown
/// symbols — engines should treat id 0 as "drop the tick".
#[inline]
pub fn symbol_id_for(sym: &str) -> u32 {
    match sym {
        "RELIANCE" => 1,
        "INFY" => 2,
        "SBIN" => 3,
        "HDFCBANK" => 4,
        "ICICIBANK" => 5,
        // Add more here. Keep ids stable across releases.
        _ => 0,
    }
}

/// Inverse mapping. Returns `None` for unknown ids.
#[inline]
pub fn symbol_for_id(id: u32) -> Option<&'static str> {
    match id {
        1 => Some("RELIANCE"),
        2 => Some("INFY"),
        3 => Some("SBIN"),
        4 => Some("HDFCBANK"),
        5 => Some("ICICIBANK"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_known_symbols() {
        for sym in ["RELIANCE", "INFY", "SBIN", "HDFCBANK", "ICICIBANK"] {
            let id = symbol_id_for(sym);
            assert_ne!(id, 0, "{} should have a non-zero id", sym);
            assert_eq!(symbol_for_id(id), Some(sym));
        }
    }

    #[test]
    fn unknown_symbol_returns_zero() {
        assert_eq!(symbol_id_for("DOESNOTEXIST"), 0);
        assert_eq!(symbol_id_for(""), 0);
    }

    #[test]
    fn unknown_id_returns_none() {
        assert_eq!(symbol_for_id(0), None);
        assert_eq!(symbol_for_id(999), None);
    }

    #[test]
    fn ids_are_stable_and_dense() {
        // The first 5 ids must be 1..=5; tests guard against accidental
        // re-numbering when a new symbol is added at the wrong position.
        assert_eq!(symbol_id_for("RELIANCE"), 1);
        assert_eq!(symbol_id_for("ICICIBANK"), 5);
    }
}
