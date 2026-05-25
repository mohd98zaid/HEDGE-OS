//! Per-evaluation context plumbed through every strategy.
//!
//! The Signal_Engine assembles a fresh [`StrategyContext`] on each feature
//! update before invoking [`crate::Strategy::evaluate`]. The context
//! borrows from longer-lived state held by the engine so individual
//! strategies remain allocation-free in the steady state (R4.4 forbids
//! polling, R30.8 forbids hot-path allocations).

use std::collections::BTreeMap;

use hedge_core::{Regime, SmallVec, SymbolId};
use hedge_schemas::strategy_id::StrategyId;

/// Trader-controlled per-strategy enable/disable map (R4.5).
///
/// `BTreeMap` is used rather than `HashMap` so iteration order is stable
/// across runs, which makes the engine's strategy ordering deterministic
/// (Property 7 expects "same input → same signal sequence"). Toggle entries
/// not present in the map default to **enabled** so a fresh deployment
/// without any explicit toggles still emits signals.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StrategyToggles {
    /// `id → enabled?`. Missing entries default to `true`.
    pub enabled: BTreeMap<StrategyId, bool>,
}

impl StrategyToggles {
    /// Construct a `StrategyToggles` with every concrete strategy
    /// explicitly enabled. Useful for tests and for the engine's
    /// "no-config" startup path.
    pub fn all_enabled() -> Self {
        let mut m = BTreeMap::new();
        for id in [
            StrategyId::OpeningRangeBreakout,
            StrategyId::VwapPullback,
            StrategyId::MomentumBreakout,
            StrategyId::LiquiditySweepReversal,
            StrategyId::OptionsOiExpansionBreakout,
            StrategyId::VolatilityCompressionBreakout,
        ] {
            m.insert(id, true);
        }
        Self { enabled: m }
    }

    /// Disable a single strategy. Returns the modified `Self` for
    /// builder-style construction in tests.
    pub fn with_disabled(mut self, id: StrategyId) -> Self {
        self.enabled.insert(id, false);
        self
    }

    /// Returns `true` when the strategy is enabled (or absent — which
    /// defaults to enabled).
    pub fn is_enabled(&self, id: StrategyId) -> bool {
        self.enabled.get(&id).copied().unwrap_or(true)
    }
}

/// 4-byte sector identifier carried through `ai.news.impact.*` events.
///
/// We use a numeric id rather than the symbol's sector string to keep the
/// gating data structure cache-friendly and `Copy`. The mapping
/// `SectorId ↔ string` lives in `hedge-config` and is resolved at
/// startup; the Signal_Engine only needs the numeric tag.
#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SectorId(pub u32);

impl SectorId {
    /// Construct from a raw u32.
    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }
    /// Recover the raw u32.
    #[inline]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

/// News-gating state sourced from `ai.news.impact.*` events (R12.6).
///
/// Concrete sets:
///
/// * `blocked_sectors` — sectors whose news impact magnitude is above the
///   configured threshold (e.g. an RBI repo-rate decision blocks the
///   `Banking` sector).
/// * `blocked_symbols` — specific symbols singled out by news-driven
///   risk (e.g. a single stock under a regulatory action).
///
/// `SmallVec` is used for both because typical day has < 8 sectors
/// blocked and < 16 symbols blocked simultaneously; the inline storage
/// keeps the gate check cache-friendly.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NewsGates {
    /// Sectors whose strategies are temporarily suppressed.
    pub blocked_sectors: SmallVec<[SectorId; 8]>,
    /// Symbols whose strategies are temporarily suppressed.
    pub blocked_symbols: SmallVec<[SymbolId; 16]>,
}

impl NewsGates {
    /// Empty gate set — the no-news path. Every symbol passes.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns `true` when `sym` is on the blocked-symbol list.
    #[inline]
    pub fn is_symbol_blocked(&self, sym: SymbolId) -> bool {
        self.blocked_symbols.iter().any(|s| *s == sym)
    }

    /// Returns `true` when `sector` is on the blocked-sector list.
    #[inline]
    pub fn is_sector_blocked(&self, sector: SectorId) -> bool {
        self.blocked_sectors.iter().any(|s| *s == sector)
    }
}

/// Previous-day structural memory exposed by the
/// [`hedge_schemas::json_schemas::MEM_PREV_DAY_SCHEMA`] subject (R15.2).
///
/// The Signal_Engine consumes this as a borrowed view in
/// [`StrategyContext::previous_day`]. A `None` value means the
/// Previous_Day_Memory_Engine has not yet published for the symbol.
///
/// Fields mirror the JSON schema's `mem.prev_day.<symbol>` payload but
/// keep prices in **paise** as `i64` for non-allocating arithmetic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PreviousDayMemory {
    /// Symbol the record refers to.
    pub symbol: SymbolId,
    /// Prior session open (paise).
    pub open_paise: i64,
    /// Prior session high (paise).
    pub high_paise: i64,
    /// Prior session low (paise).
    pub low_paise: i64,
    /// Prior session close (paise).
    pub close_paise: i64,
    /// Prior session VWAP (paise).
    pub vwap_paise: i64,
}

/// Per-evaluation context borrowed by every strategy.
///
/// All fields are borrowed references with the `'a` lifetime tying the
/// context's validity to the engine's evaluation window. Strategies do
/// not retain the context past the `evaluate` call.
#[derive(Debug)]
pub struct StrategyContext<'a> {
    /// Current market regime (R4.6, R13.1).
    pub regime: Regime,
    /// Per-strategy enable/disable map (R4.5).
    pub trader_config: &'a StrategyToggles,
    /// Whether Market_Open_War_Mode is active (R26.2).
    pub war_mode: bool,
    /// Minimum signal confidence accepted while war mode is active (R26.3).
    pub war_mode_min_confidence: f32,
    /// Previous-day structural data, when available.
    pub previous_day: Option<&'a PreviousDayMemory>,
    /// News-driven sector and symbol gates (R12.6).
    pub news_gates: &'a NewsGates,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toggles_default_to_enabled_when_missing() {
        let t = StrategyToggles::default();
        assert!(t.is_enabled(StrategyId::OpeningRangeBreakout));
        assert!(t.is_enabled(StrategyId::VwapPullback));
    }

    #[test]
    fn all_enabled_explicitly_lists_every_strategy() {
        let t = StrategyToggles::all_enabled();
        for id in [
            StrategyId::OpeningRangeBreakout,
            StrategyId::VwapPullback,
            StrategyId::MomentumBreakout,
            StrategyId::LiquiditySweepReversal,
            StrategyId::OptionsOiExpansionBreakout,
            StrategyId::VolatilityCompressionBreakout,
        ] {
            assert!(t.is_enabled(id));
            assert_eq!(t.enabled.get(&id), Some(&true));
        }
    }

    #[test]
    fn with_disabled_flips_one_entry_only() {
        let t = StrategyToggles::all_enabled().with_disabled(StrategyId::VwapPullback);
        assert!(!t.is_enabled(StrategyId::VwapPullback));
        assert!(t.is_enabled(StrategyId::OpeningRangeBreakout));
    }

    #[test]
    fn news_gates_default_is_permissive() {
        let g = NewsGates::default();
        assert!(!g.is_symbol_blocked(SymbolId::new(7)));
        assert!(!g.is_sector_blocked(SectorId::new(1)));
    }

    #[test]
    fn news_gates_block_specific_symbol() {
        let mut g = NewsGates::empty();
        g.blocked_symbols.push(SymbolId::new(42));
        assert!(g.is_symbol_blocked(SymbolId::new(42)));
        assert!(!g.is_symbol_blocked(SymbolId::new(7)));
    }

    #[test]
    fn news_gates_block_specific_sector() {
        let mut g = NewsGates::empty();
        g.blocked_sectors.push(SectorId::new(3));
        assert!(g.is_sector_blocked(SectorId::new(3)));
        assert!(!g.is_sector_blocked(SectorId::new(2)));
    }
}
