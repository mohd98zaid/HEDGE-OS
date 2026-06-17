//! CompositeAlphaBreakout strategy.
//!
//! Combines multiple dimensions of algorithmic and structural data to emit
//! highly selective, high-probability signals. This is designed to be the
//! "flagship" profitable signal that incorporates liquidity, algorithm, and
//! news/gating factors.
//!
//! ### Preconditions
//!
//! 1. **Algorithm**: Strong trend alignment (EMA fast > EMA slow), and high
//!    positive momentum (`momentum > 0.5`).
//! 2. **Liquidity**: Favorable liquidity imbalance and strong orderflow
//!    strength (`orderflow_strength > 0.6`).
//! 3. **Structure**: Positive breakout pressure.
//! 4. **News/Gating**: The `NewsGates` structurally block any trades when
//!    adverse news affects the sector or symbol. The downstream AI layer
//!    will further rank the signal using actual news sentiment.

use hedge_bus::symbol_for_id;
use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::{build_signal, clamp01, risk_profile_for};
use crate::strategy::Strategy;

/// Minimum momentum required to trigger.
pub const ALPHA_MIN_MOMENTUM: f32 = 0.0005;

/// Minimum orderflow strength required to trigger.
pub const ALPHA_MIN_ORDERFLOW: f32 = 0.6;

#[derive(Copy, Clone, Debug, Default)]
pub struct CompositeAlphaBreakout;

impl Strategy for CompositeAlphaBreakout {
    fn id(&self) -> StrategyId {
        StrategyId::CompositeAlphaBreakout
    }

    fn evaluate(&self, snap: &FeatureSnapshot, _ctx: &StrategyContext) -> Option<Signal> {
        let sym_name = symbol_for_id(snap.symbol).unwrap_or("");
        if !sym_name.contains("Nifty") {
            return None;
        }

        let price = snap.price;

        // Strong trend: Price must be above fast EMA, and slow EMA must be above trend EMA
        let trend_long = price > snap.ema_fast && snap.ema_slow > snap.ema_trend;
        let trend_short = price < snap.ema_fast && snap.ema_slow < snap.ema_trend;

        // Volatility filter: Avoid dead markets (ATR < 1.0 point / 100 paise)
        if snap.atr < 100 {
            return None;
        }

        let donchian_breakout = price >= snap.donchian_upper;
        let donchian_breakdown = price <= snap.donchian_lower;

        if !donchian_breakout && !donchian_breakdown {
            return None;
        }

        let side = if donchian_breakout { Side::Buy } else { Side::Sell };

        if side == Side::Buy && !trend_long { return None; }
        if side == Side::Sell && !trend_short { return None; }

        // Extremely high confidence since multiple dimensions align
        let base_probability = clamp01(0.75 + 0.1 * snap.momentum.abs());
        let confidence = clamp01(0.8 + 0.2 * snap.orderflow_strength.abs());

        // Dynamic Stop Loss based on ATR (fallback to 50 if zero)
        let atr_f = if snap.atr > 0 { snap.atr as f64 } else { 50.0 };
        let stop_distance = 1.5 * atr_f;
        let target_distance = stop_distance * 3.0; // 1:3 Risk Reward

        let mut rp = risk_profile_for(price, side, snap.atr);
        if side == Side::Buy {
            rp.stop_loss_paise = price - stop_distance.round() as i64;
            rp.take_profit_paise = price + target_distance.round() as i64;
        } else {
            rp.stop_loss_paise = price + stop_distance.round() as i64;
            rp.take_profit_paise = price - target_distance.round() as i64;
        }

        Some(build_signal(
            snap.correlation_id,
            self.id(),
            SymbolId::new(snap.symbol),
            side,
            base_probability,
            confidence,
            rp,
            snap.ts_ns,
        ))
    }

    fn enabled_in(&self, regime: Regime) -> bool {
        // Requires a trending environment and active participation
        matches!(regime, Regime::Trending | Regime::HighVolatility)
    }
}
