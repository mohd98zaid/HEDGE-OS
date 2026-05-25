//! Per-symbol realized-volatility tracker (R5.10).
//!
//! Consumes `feat.update.<sym>` `realized_vol` values published by the
//! Feature_Extraction_Engine and maintains the most recent reading per
//! symbol. The Risk_Engine consults this table during `evaluate` and
//! rejects with [`RejectionReason::VolatilityBlock`] when
//! `realized_vol > config.volatility_block_threshold`.
//!
//! ### Storage
//!
//! `BTreeMap<SymbolId, f32>` rather than `DashMap` because:
//!
//! 1. The Risk_Engine is single-threaded on the evaluator path; no
//!    concurrent writers exist on this state.
//! 2. The deterministic iteration order surfaces in replay assertions.
//!
//! ### Edge-triggered emissions (Property 8)
//!
//! [`update`](VolatilityTable::update) returns a [`VolatilityTransition`]
//! that distinguishes between transitions (false→true and true→false in
//! "above the threshold") and steady state, so the Risk_Engine can emit
//! per-symbol block / unblock events on the edge only. The emission is
//! the engine's responsibility — this module just reports the transition.

use std::collections::BTreeMap;

use hedge_core::SymbolId;
use serde::{Deserialize, Serialize};

/// Outcome of a volatility update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolatilityTransition {
    /// No tracked transition: either both old and new are below the
    /// threshold, or both are above.
    Steady,
    /// `false → true` — the symbol just crossed above the threshold.
    Blocked,
    /// `true → false` — the symbol just dropped back below the threshold.
    Unblocked,
}

/// Per-symbol realized-volatility table.
///
/// Stores the most recent `realized_vol` reading per `SymbolId`. Lookups
/// are `O(log N)` in the number of tracked symbols.
#[derive(Debug, Default)]
pub struct VolatilityTable {
    inner: BTreeMap<SymbolId, f32>,
}

impl VolatilityTable {
    /// Construct an empty table.
    pub const fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Update the realized volatility for `symbol`.
    ///
    /// Returns the transition relative to `threshold` so the Risk_Engine
    /// can emit edge-triggered block/unblock events.
    pub fn update(&mut self, symbol: SymbolId, vol: f32, threshold: f32) -> VolatilityTransition {
        let prior = self.inner.insert(symbol, vol);
        let was_blocked = prior.map(|p| p > threshold).unwrap_or(false);
        let now_blocked = vol > threshold;
        match (was_blocked, now_blocked) {
            (false, true) => VolatilityTransition::Blocked,
            (true, false) => VolatilityTransition::Unblocked,
            _ => VolatilityTransition::Steady,
        }
    }

    /// Returns `true` when `symbol`'s most recent reading is above the
    /// configured threshold. Symbols with no recorded reading are
    /// considered unblocked.
    #[inline]
    pub fn is_blocked(&self, symbol: SymbolId, threshold: f32) -> bool {
        match self.inner.get(&symbol) {
            Some(v) => *v > threshold,
            None => false,
        }
    }

    /// Borrow the most recent reading for `symbol`, if any.
    #[inline]
    pub fn realized_vol(&self, symbol: SymbolId) -> Option<f32> {
        self.inner.get(&symbol).copied()
    }

    /// Number of tracked symbols.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when no symbols have been tracked yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u32) -> SymbolId {
        SymbolId::new(n)
    }

    #[test]
    fn fresh_table_reports_unblocked() {
        let t = VolatilityTable::new();
        assert!(!t.is_blocked(s(1), 0.05));
        assert_eq!(t.realized_vol(s(1)), None);
    }

    #[test]
    fn first_update_above_threshold_returns_blocked() {
        let mut t = VolatilityTable::new();
        let tr = t.update(s(1), 0.10, 0.05);
        assert_eq!(tr, VolatilityTransition::Blocked);
        assert!(t.is_blocked(s(1), 0.05));
    }

    #[test]
    fn first_update_below_threshold_returns_steady() {
        let mut t = VolatilityTable::new();
        let tr = t.update(s(1), 0.01, 0.05);
        assert_eq!(tr, VolatilityTransition::Steady);
        assert!(!t.is_blocked(s(1), 0.05));
    }

    #[test]
    fn drop_below_threshold_returns_unblocked() {
        let mut t = VolatilityTable::new();
        t.update(s(1), 0.10, 0.05);
        let tr = t.update(s(1), 0.01, 0.05);
        assert_eq!(tr, VolatilityTransition::Unblocked);
        assert!(!t.is_blocked(s(1), 0.05));
    }

    #[test]
    fn repeated_above_returns_steady() {
        let mut t = VolatilityTable::new();
        t.update(s(1), 0.10, 0.05);
        let tr = t.update(s(1), 0.20, 0.05);
        assert_eq!(tr, VolatilityTransition::Steady);
    }

    #[test]
    fn realized_vol_round_trip() {
        let mut t = VolatilityTable::new();
        t.update(s(1), 0.07, 0.05);
        assert_eq!(t.realized_vol(s(1)), Some(0.07));
    }

    #[test]
    fn at_threshold_is_not_blocked_strict_gt() {
        // Strictly greater than — equality is not blocked.
        let mut t = VolatilityTable::new();
        let tr = t.update(s(1), 0.05, 0.05);
        assert_eq!(tr, VolatilityTransition::Steady);
        assert!(!t.is_blocked(s(1), 0.05));
    }
}
