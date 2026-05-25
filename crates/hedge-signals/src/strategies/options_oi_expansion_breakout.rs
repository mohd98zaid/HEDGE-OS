//! Options_OI_Expansion_Breakout strategy (skeleton).
//!
//! Detects an underlying breakout that is corroborated by a fresh
//! expansion in options open interest. The fully-wired implementation
//! requires the OI cache from `hedge-warmcache` and the `md.oi.<sym>`
//! NATS subject (task 24.1). Until then this strategy returns `None`
//! whenever the OI cache is unavailable, which is the safe behaviour
//! mandated by R30.6 ("Hot_Path must not depend on cloud-hosted
//! services for execution decisions" — and by extension, must not emit
//! a signal that depends on data it has not actually received).
//!
//! ### Skeleton behaviour
//!
//! * The strategy holds an `Option<Arc<dyn OiCache>>` (defaulting to
//!   `None`).
//! * When the cache is `None`, every `evaluate` call returns `None`.
//! * When the cache is populated and the expansion gate fires (cache
//!   reports a fresh OI buildup on the breakout side AND the
//!   breakout_pressure agrees), a signal is emitted.
//!
//! ### TODO: OI cache wiring (task 24.1)
//!
//! The full `OiCache` trait will land alongside the OI normaliser.
//! Today's stub trait below is intentionally minimal so callers can
//! plug in a mock for tests.

use std::sync::Arc;

use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::{build_signal, clamp01, risk_profile_for};
use crate::strategy::Strategy;

/// Minimum absolute `breakout_pressure` magnitude required to fire.
pub const OI_BREAKOUT_PRESSURE_MIN: f32 = 0.4;

/// Read-only access to the OI cache populated by the
/// Market_Data_Engine's `md.oi.<sym>` subscriber (task 24.1).
///
/// Returns `Some(true)` when the cache reports a fresh OI expansion
/// **on the breakout side** (call-side buildup for long breakouts,
/// put-side buildup for short breakouts). Returns `Some(false)` when
/// no fresh expansion is observed; returns `None` when the cache has
/// not yet been populated for the symbol — the strategy treats `None`
/// as "skip this evaluation, OI data unavailable".
pub trait OiCache: Send + Sync {
    /// Query whether the OI cache reports a fresh expansion in the
    /// `breakout_side` direction for `symbol`.
    fn fresh_expansion(&self, symbol: SymbolId, breakout_side: Side) -> Option<bool>;
}

/// Options_OI_Expansion_Breakout strategy.
///
/// Holds an optional `Arc<dyn OiCache>` that the engine binary populates
/// at startup. When `None`, every `evaluate` call returns `None`.
#[derive(Clone, Default)]
pub struct OptionsOiExpansionBreakout {
    cache: Option<Arc<dyn OiCache>>,
}

impl OptionsOiExpansionBreakout {
    /// Construct an instance without an OI cache. `evaluate` always
    /// returns `None` until [`with_cache`](Self::with_cache) is called.
    pub fn new() -> Self {
        Self { cache: None }
    }

    /// Replace the OI cache (used by the engine binary at startup and
    /// by tests with a mock cache).
    pub fn with_cache(mut self, cache: Arc<dyn OiCache>) -> Self {
        self.cache = Some(cache);
        self
    }
}

impl std::fmt::Debug for OptionsOiExpansionBreakout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptionsOiExpansionBreakout")
            .field("has_cache", &self.cache.is_some())
            .finish()
    }
}

impl Strategy for OptionsOiExpansionBreakout {
    fn id(&self) -> StrategyId {
        StrategyId::OptionsOiExpansionBreakout
    }

    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal> {
        let _ = ctx;

        // No OI cache wired yet — the safe path is `None`.
        let cache = self.cache.as_ref()?;

        // Determine candidate side from breakout pressure.
        let pressure = snap.breakout_pressure;
        if pressure.abs() < OI_BREAKOUT_PRESSURE_MIN {
            return None;
        }
        let side = if pressure > 0.0 { Side::Buy } else { Side::Sell };

        // Cache must report a fresh expansion in the breakout direction.
        let fresh = cache.fresh_expansion(SymbolId::new(snap.symbol), side)?;
        if !fresh {
            return None;
        }

        let confidence = clamp01(pressure.abs());
        let base_probability = clamp01(0.5 + 0.5 * pressure.abs().min(1.0));

        let entry = if snap.vwap > 0 { snap.vwap } else { snap.ema_fast };
        Some(build_signal(
            snap.correlation_id,
            self.id(),
            SymbolId::new(snap.symbol),
            side,
            base_probability,
            confidence,
            risk_profile_for(entry, side, snap.atr),
            snap.ts_ns,
        ))
    }

    fn enabled_in(&self, regime: Regime) -> bool {
        !matches!(
            regime,
            Regime::Sideways | Regime::LiquidityCrisis | Regime::LowParticipation
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{NewsGates, StrategyToggles};

    /// Mock OI cache used by the unit tests.
    struct StubCache {
        result: Option<bool>,
    }
    impl OiCache for StubCache {
        fn fresh_expansion(&self, _symbol: SymbolId, _side: Side) -> Option<bool> {
            self.result
        }
    }

    fn snap(breakout_pressure: f32) -> FeatureSnapshot {
        FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap: 100_00,
            atr: 100,
            ema_fast: 100_00,
            ema_slow: 100_00,
            ema_slope: 0.0,
            realized_vol: 0.0,
            momentum: 0.0,
            rolling_delta: 0,
            liquidity_imbalance: 0.0,
            orderflow_strength: 0.0,
            candle_structure: 0,
            breakout_pressure,
            compression_zone: 0.0,
            liquidity_sweep: 0.0,
            ts_ns: 0,
        }
    }

    fn ctx<'a>(toggles: &'a StrategyToggles, news: &'a NewsGates) -> StrategyContext<'a> {
        StrategyContext {
            regime: Regime::Trending,
            trader_config: toggles,
            war_mode: false,
            war_mode_min_confidence: 0.7,
            previous_day: None,
            news_gates: news,
        }
    }

    #[test]
    fn returns_none_when_oi_cache_unwired() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = OptionsOiExpansionBreakout::new();
        let snap_ = snap(0.8);
        assert!(s.evaluate(&snap_, &c).is_none());
    }

    #[test]
    fn returns_none_when_cache_returns_none() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = OptionsOiExpansionBreakout::new()
            .with_cache(Arc::new(StubCache { result: None }));
        assert!(s.evaluate(&snap(0.8), &c).is_none());
    }

    #[test]
    fn returns_none_when_cache_reports_no_expansion() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = OptionsOiExpansionBreakout::new()
            .with_cache(Arc::new(StubCache { result: Some(false) }));
        assert!(s.evaluate(&snap(0.8), &c).is_none());
    }

    #[test]
    fn fires_when_cache_reports_expansion_and_pressure_above_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = OptionsOiExpansionBreakout::new()
            .with_cache(Arc::new(StubCache { result: Some(true) }));
        let sig = s.evaluate(&snap(0.8), &c).expect("signal");
        assert_eq!(sig.side, Side::Buy.as_u8());
        assert_eq!(sig.strategy, StrategyId::OptionsOiExpansionBreakout.as_u8());
    }

    #[test]
    fn does_not_fire_when_pressure_below_floor() {
        let toggles = StrategyToggles::all_enabled();
        let news = NewsGates::empty();
        let c = ctx(&toggles, &news);
        let s = OptionsOiExpansionBreakout::new()
            .with_cache(Arc::new(StubCache { result: Some(true) }));
        assert!(s.evaluate(&snap(0.3), &c).is_none());
    }
}
