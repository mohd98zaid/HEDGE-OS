//! Strategy gating logic.
//!
//! Each gate is a **pure function** that maps a [`StrategyContext`] (plus
//! the candidate strategy or signal) to a [`GateOutcome`]. Composing the
//! gates inside [`check_gates`] produces a single, deterministic decision
//! per strategy invocation; tests can therefore drive each gate
//! independently to satisfy Property 7.
//!
//! Gating order (R4.5, R4.6, R12.6, R13.4, R26.2):
//!
//! 1. **Trader toggle** — strategy disabled by trader configuration.
//! 2. **Regime gate** — strategy disabled in the current market regime.
//! 3. **News gate** — symbol or its sector is news-blocked.
//!
//! War-mode confidence is enforced **after** evaluation in
//! [`check_war_mode`] because it depends on the candidate signal's
//! `confidence` field, which only exists once the strategy has emitted.
//!
//! All gates are infallible — they cannot return errors, only allow or
//! block decisions. This matches the design's fail-closed posture: if any
//! gate is uncertain, it blocks rather than allowing.

use hedge_core::SymbolId;
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::Signal;

use crate::context::{SectorId, StrategyContext};
use crate::strategy::Strategy;

/// Outcome of running every gate against a `(strategy, context, symbol)`
/// triple.
///
/// `Allowed` means the strategy may evaluate; `Blocked` carries the
/// canonical [`GateReason`] explaining the suppression. Reasons surface
/// in structured logs and metrics so the operator can see exactly which
/// gate stopped a strategy from emitting.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GateOutcome {
    /// Strategy may proceed to `evaluate`.
    Allowed,
    /// Strategy is suppressed; the [`GateReason`] is the cause.
    Blocked(GateReason),
}

impl GateOutcome {
    /// Returns `true` when the outcome allows the strategy to evaluate.
    #[inline]
    pub fn is_allowed(self) -> bool {
        matches!(self, GateOutcome::Allowed)
    }
}

/// Stable canonical reason a gate blocked a strategy.
///
/// Wire-stable: each variant maps to a `&'static str` identifier used in
/// metrics labels and structured logs. New reasons must be appended
/// rather than reordered.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum GateReason {
    /// Trader toggled the strategy off (R4.5).
    TraderToggle,
    /// Strategy is disabled in the current regime (R4.6, R13.4).
    RegimeBlocked,
    /// Sector or symbol is news-blocked (R12.6).
    NewsBlocked,
    /// War-mode confidence floor not met (R26.2, R26.3).
    WarModeConfidenceTooLow,
}

impl GateReason {
    /// Stable canonical short identifier.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TraderToggle => "trader_toggle",
            Self::RegimeBlocked => "regime_blocked",
            Self::NewsBlocked => "news_blocked",
            Self::WarModeConfidenceTooLow => "war_mode_confidence_too_low",
        }
    }
}

/// Pre-evaluation gate composition.
///
/// Returns the first blocking outcome encountered, in the order:
///
/// 1. trader toggle
/// 2. regime
/// 3. news
///
/// Returns [`GateOutcome::Allowed`] when every gate passes.
pub fn check_gates(
    strategy: &dyn Strategy,
    ctx: &StrategyContext,
    symbol: SymbolId,
    sector: Option<SectorId>,
) -> GateOutcome {
    if let GateOutcome::Blocked(r) = check_trader_toggle(strategy.id(), ctx) {
        return GateOutcome::Blocked(r);
    }
    if let GateOutcome::Blocked(r) = check_regime(strategy, ctx) {
        return GateOutcome::Blocked(r);
    }
    if let GateOutcome::Blocked(r) = check_news(symbol, sector, ctx) {
        return GateOutcome::Blocked(r);
    }
    GateOutcome::Allowed
}

/// Trader toggle gate (R4.5).
#[inline]
pub fn check_trader_toggle(id: StrategyId, ctx: &StrategyContext) -> GateOutcome {
    if ctx.trader_config.is_enabled(id) {
        GateOutcome::Allowed
    } else {
        GateOutcome::Blocked(GateReason::TraderToggle)
    }
}

/// Regime gate (R4.6, R13.4). Delegates to `Strategy::enabled_in`.
#[inline]
pub fn check_regime(strategy: &dyn Strategy, ctx: &StrategyContext) -> GateOutcome {
    if strategy.enabled_in(ctx.regime) {
        GateOutcome::Allowed
    } else {
        GateOutcome::Blocked(GateReason::RegimeBlocked)
    }
}

/// News gate (R12.6).
#[inline]
pub fn check_news(
    symbol: SymbolId,
    sector: Option<SectorId>,
    ctx: &StrategyContext,
) -> GateOutcome {
    if ctx.news_gates.is_symbol_blocked(symbol) {
        return GateOutcome::Blocked(GateReason::NewsBlocked);
    }
    if let Some(sec) = sector {
        if ctx.news_gates.is_sector_blocked(sec) {
            return GateOutcome::Blocked(GateReason::NewsBlocked);
        }
    }
    GateOutcome::Allowed
}

/// War-mode confidence gate (R26.2, R26.3).
///
/// Run **after** the strategy has produced a candidate signal — this
/// gate compares the signal's `confidence` against the configured war
/// mode floor. Returns [`GateOutcome::Allowed`] when war mode is
/// inactive.
#[inline]
pub fn check_war_mode(signal: &Signal, ctx: &StrategyContext) -> GateOutcome {
    if !ctx.war_mode {
        return GateOutcome::Allowed;
    }
    if signal.confidence + f32::EPSILON < ctx.war_mode_min_confidence {
        GateOutcome::Blocked(GateReason::WarModeConfidenceTooLow)
    } else {
        GateOutcome::Allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{NewsGates, StrategyToggles};
    use hedge_core::Regime;
    use hedge_schemas::{RiskProfile, Signal};

    /// Tiny test stub to exercise the gating module without depending on
    /// concrete strategy implementations (which add their own logic on top
    /// of the trait).
    struct StubStrategy {
        id: StrategyId,
        regime_enabled: bool,
    }

    impl Strategy for StubStrategy {
        fn id(&self) -> StrategyId {
            self.id
        }
        fn evaluate(
            &self,
            _snap: &hedge_schemas::FeatureSnapshot,
            _ctx: &StrategyContext,
        ) -> Option<Signal> {
            None
        }
        fn enabled_in(&self, _regime: Regime) -> bool {
            self.regime_enabled
        }
    }

    fn empty_ctx<'a>(toggles: &'a StrategyToggles, news: &'a NewsGates) -> StrategyContext<'a> {
        StrategyContext {
            regime: Regime::Trending,
            trader_config: toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: news,
        }
    }

    fn signal_with_confidence(c: f32) -> Signal {
        Signal {
            correlation_id: [0u8; 16],
            strategy: StrategyId::OpeningRangeBreakout.as_u8(),
            symbol: 1,
            side: 0,
            base_probability: 0.5,
            confidence: c,
            risk_profile: RiskProfile::default(),
            ts_ns: 0,
        }
    }

    #[test]
    fn allowed_when_all_gates_pass() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let ctx = empty_ctx(&toggles, &news);
        let s = StubStrategy {
            id: StrategyId::OpeningRangeBreakout,
            regime_enabled: true,
        };
        assert_eq!(
            check_gates(&s, &ctx, SymbolId::new(1), None),
            GateOutcome::Allowed
        );
    }

    #[test]
    fn blocked_on_trader_toggle() {
        let toggles =
            StrategyToggles::all_enabled().with_disabled(StrategyId::OpeningRangeBreakout);
        let news = NewsGates::empty();
        let ctx = empty_ctx(&toggles, &news);
        let s = StubStrategy {
            id: StrategyId::OpeningRangeBreakout,
            regime_enabled: true,
        };
        assert_eq!(
            check_gates(&s, &ctx, SymbolId::new(1), None),
            GateOutcome::Blocked(GateReason::TraderToggle)
        );
    }

    #[test]
    fn blocked_on_regime() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let ctx = empty_ctx(&toggles, &news);
        let s = StubStrategy {
            id: StrategyId::OpeningRangeBreakout,
            regime_enabled: false,
        };
        assert_eq!(
            check_gates(&s, &ctx, SymbolId::new(1), None),
            GateOutcome::Blocked(GateReason::RegimeBlocked)
        );
    }

    #[test]
    fn blocked_on_news_symbol() {
        let toggles = StrategyToggles::all_enabled();
        let mut news = NewsGates::empty();
        news.blocked_symbols.push(SymbolId::new(7));
        let ctx = empty_ctx(&toggles, &news);
        let s = StubStrategy {
            id: StrategyId::OpeningRangeBreakout,
            regime_enabled: true,
        };
        assert_eq!(
            check_gates(&s, &ctx, SymbolId::new(7), None),
            GateOutcome::Blocked(GateReason::NewsBlocked)
        );
    }

    #[test]
    fn blocked_on_news_sector() {
        let toggles = StrategyToggles::all_enabled();
        let mut news = NewsGates::empty();
        news.blocked_sectors.push(SectorId::new(2));
        let ctx = empty_ctx(&toggles, &news);
        let s = StubStrategy {
            id: StrategyId::OpeningRangeBreakout,
            regime_enabled: true,
        };
        assert_eq!(
            check_gates(&s, &ctx, SymbolId::new(7), Some(SectorId::new(2))),
            GateOutcome::Blocked(GateReason::NewsBlocked)
        );
    }

    #[test]
    fn news_gate_pass_with_unrelated_blocks() {
        let toggles = StrategyToggles::all_enabled();
        let mut news = NewsGates::empty();
        news.blocked_symbols.push(SymbolId::new(99));
        news.blocked_sectors.push(SectorId::new(99));
        let ctx = empty_ctx(&toggles, &news);
        let s = StubStrategy {
            id: StrategyId::OpeningRangeBreakout,
            regime_enabled: true,
        };
        assert_eq!(
            check_gates(&s, &ctx, SymbolId::new(7), Some(SectorId::new(2))),
            GateOutcome::Allowed
        );
    }

    #[test]
    fn war_mode_inactive_always_allowed() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let mut ctx = empty_ctx(&toggles, &news);
        ctx.war_mode = false;
        ctx.war_mode_min_confidence = 0.99;
        let sig = signal_with_confidence(0.0);
        assert_eq!(check_war_mode(&sig, &ctx), GateOutcome::Allowed);
    }

    #[test]
    fn war_mode_blocks_below_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let mut ctx = empty_ctx(&toggles, &news);
        ctx.war_mode = true;
        ctx.war_mode_min_confidence = 0.7;
        let sig = signal_with_confidence(0.5);
        assert_eq!(
            check_war_mode(&sig, &ctx),
            GateOutcome::Blocked(GateReason::WarModeConfidenceTooLow)
        );
    }

    #[test]
    fn war_mode_allows_at_or_above_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let mut ctx = empty_ctx(&toggles, &news);
        ctx.war_mode = true;
        ctx.war_mode_min_confidence = 0.7;
        // At floor.
        let sig = signal_with_confidence(0.7);
        assert_eq!(check_war_mode(&sig, &ctx), GateOutcome::Allowed);
        // Above floor.
        let sig = signal_with_confidence(0.9);
        assert_eq!(check_war_mode(&sig, &ctx), GateOutcome::Allowed);
    }

    #[test]
    fn gate_reason_canonical_strings_are_stable() {
        assert_eq!(GateReason::TraderToggle.as_str(), "trader_toggle");
        assert_eq!(GateReason::RegimeBlocked.as_str(), "regime_blocked");
        assert_eq!(GateReason::NewsBlocked.as_str(), "news_blocked");
        assert_eq!(
            GateReason::WarModeConfidenceTooLow.as_str(),
            "war_mode_confidence_too_low"
        );
    }
}
