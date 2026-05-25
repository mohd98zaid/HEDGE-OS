//! Per-symbol cooldown registry (R5.8).
//!
//! When a fill exceeds the slippage threshold, the Risk_Engine puts the
//! affected symbol in a cooldown for `slippage_cooldown_ms` (R5.8). While
//! the symbol is cooling, every new entry on that symbol is rejected with
//! `RejectionReason::SlippageCooldown`.
//!
//! ### Storage
//!
//! Cooldowns are stored in a `BTreeMap<SymbolId, u64>` (expiry timestamp
//! in nanoseconds). The brief calls for "a small `BTreeMap`" — we honour
//! that. Lookups are `O(log N)` where N is the number of *active* cooled
//! symbols, which is rarely more than a handful.
//!
//! ### Edge-triggered emissions (Property 8)
//!
//! [`engage`](CooldownRegistry::engage) returns a [`CooldownTransition`]
//! that tells the caller whether this engage was a fresh false→true
//! transition, so the Risk_Engine can publish `risk.cooldown.<sym>` on
//! the engage edge only.
//!
//! Expiry detection is handled by [`prune`](CooldownRegistry::prune):
//! every entry whose timestamp is in the past is removed and the symbols
//! that just expired are returned to the caller, again so the Risk_Engine
//! can emit a release event on the `true → false` edge.

use std::collections::BTreeMap;

use hedge_core::SymbolId;
use serde::{Deserialize, Serialize};

/// Categorical reason a symbol is in cooldown — embedded in
/// `risk.cooldown.<sym>` payloads. Reserved for future expansion; the
/// only reason the Risk_Engine sets today is slippage (R5.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum CooldownReason {
    /// Recent fill exceeded the slippage threshold (R5.8).
    Slippage = 0,
    /// Trader_Psychology cooldown (R16.5).
    PsychologyCooldown = 1,
}

impl CooldownReason {
    /// Stable canonical short string for logs and metrics.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Slippage => "slippage",
            Self::PsychologyCooldown => "psychology_cooldown",
        }
    }
}

/// Outcome of an `engage` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CooldownTransition {
    /// The symbol was not in cooldown before; this engage was a fresh
    /// false→true transition. The caller is responsible for the
    /// `risk.cooldown.<sym>` emission.
    Engaged,
    /// The symbol was already in cooldown. The expiry was extended to
    /// the later of (existing, new). No emission required.
    Extended,
}

/// Registry of active per-symbol cooldowns.
///
/// `BTreeMap` is preferred over `HashMap` for two reasons:
///
/// 1. Deterministic iteration order — useful in tests and replay.
/// 2. Cache-friendly for the typically-small N of active cooldowns.
#[derive(Debug, Default)]
pub struct CooldownRegistry {
    /// `symbol -> (expiry_ns, reason)`.
    inner: BTreeMap<SymbolId, (u64, CooldownReason)>,
}

impl CooldownRegistry {
    /// Construct an empty registry.
    pub const fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Returns `true` when `symbol` is in cooldown at `now_ns`.
    ///
    /// This call does not prune. The Risk_Engine calls
    /// [`prune`](Self::prune) on a slower cadence (e.g. once per
    /// `evaluate`) so the per-tick path stays cheap.
    #[inline]
    pub fn is_cooling(&self, symbol: SymbolId, now_ns: u64) -> bool {
        match self.inner.get(&symbol) {
            Some((expiry, _)) => *expiry > now_ns,
            None => false,
        }
    }

    /// Engage `symbol` until `expiry_ns`.
    ///
    /// If the symbol is already in cooldown the expiry is extended to
    /// `max(existing, expiry_ns)` — never shortened.
    pub fn engage(
        &mut self,
        symbol: SymbolId,
        expiry_ns: u64,
        reason: CooldownReason,
    ) -> CooldownTransition {
        match self.inner.get_mut(&symbol) {
            Some(entry) => {
                if expiry_ns > entry.0 {
                    entry.0 = expiry_ns;
                    entry.1 = reason;
                }
                CooldownTransition::Extended
            }
            None => {
                self.inner.insert(symbol, (expiry_ns, reason));
                CooldownTransition::Engaged
            }
        }
    }

    /// Remove every cooldown whose expiry has passed.
    ///
    /// Returns the symbols that just expired so the Risk_Engine can
    /// emit a release event on the `true → false` edge.
    pub fn prune(&mut self, now_ns: u64) -> Vec<SymbolId> {
        // Two-pass: collect, then remove. `BTreeMap::retain` would also
        // work but does not surface which keys were removed.
        let expired: Vec<SymbolId> = self
            .inner
            .iter()
            .filter_map(|(sym, (exp, _))| if *exp <= now_ns { Some(*sym) } else { None })
            .collect();
        for sym in &expired {
            self.inner.remove(sym);
        }
        expired
    }

    /// Number of active cooldowns. Useful for metrics and tests.
    #[inline]
    pub fn active_count(&self) -> usize {
        self.inner.len()
    }

    /// Borrow the recorded reason for `symbol`, if any.
    #[inline]
    pub fn reason_for(&self, symbol: SymbolId) -> Option<CooldownReason> {
        self.inner.get(&symbol).map(|(_, r)| *r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u32) -> SymbolId {
        SymbolId::new(n)
    }

    #[test]
    fn fresh_registry_reports_no_cooling() {
        let r = CooldownRegistry::new();
        assert!(!r.is_cooling(s(1), 0));
        assert_eq!(r.active_count(), 0);
    }

    #[test]
    fn engage_returns_engaged_on_first_engage_then_extended() {
        let mut r = CooldownRegistry::new();
        let t = r.engage(s(1), 100, CooldownReason::Slippage);
        assert_eq!(t, CooldownTransition::Engaged);
        // Re-engage with a longer window — extended.
        let t = r.engage(s(1), 200, CooldownReason::Slippage);
        assert_eq!(t, CooldownTransition::Extended);
        // Re-engage with a shorter window — still extended (no-op shortening).
        let t = r.engage(s(1), 150, CooldownReason::Slippage);
        assert_eq!(t, CooldownTransition::Extended);
    }

    #[test]
    fn is_cooling_respects_expiry() {
        let mut r = CooldownRegistry::new();
        r.engage(s(1), 100, CooldownReason::Slippage);
        assert!(r.is_cooling(s(1), 50));
        // At expiry, no longer cooling (`>` semantics).
        assert!(!r.is_cooling(s(1), 100));
        assert!(!r.is_cooling(s(1), 101));
    }

    #[test]
    fn prune_returns_expired_symbols_and_removes_them() {
        let mut r = CooldownRegistry::new();
        r.engage(s(1), 100, CooldownReason::Slippage);
        r.engage(s(2), 200, CooldownReason::Slippage);
        r.engage(s(3), 300, CooldownReason::Slippage);
        let expired = r.prune(200);
        // Symbols 1 and 2 expire at or before 200; symbol 3 stays.
        assert!(expired.contains(&s(1)));
        assert!(expired.contains(&s(2)));
        assert_eq!(expired.len(), 2);
        assert_eq!(r.active_count(), 1);
        assert!(r.is_cooling(s(3), 200));
    }

    #[test]
    fn engage_does_not_shorten_existing_cooldown() {
        let mut r = CooldownRegistry::new();
        r.engage(s(1), 1_000, CooldownReason::Slippage);
        r.engage(s(1), 500, CooldownReason::Slippage);
        // Original 1_000 expiry preserved.
        assert!(r.is_cooling(s(1), 999));
    }

    #[test]
    fn cooldown_reason_str_is_stable() {
        assert_eq!(CooldownReason::Slippage.as_str(), "slippage");
        assert_eq!(CooldownReason::PsychologyCooldown.as_str(), "psychology_cooldown");
    }
}
