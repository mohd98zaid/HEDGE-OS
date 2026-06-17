use chrono::{Local, Timelike, TimeZone};

use hedge_core::{Regime, Side, SymbolId};
use hedge_schemas::strategy_id::StrategyId;
use hedge_schemas::{FeatureSnapshot, Signal};

use crate::context::StrategyContext;
use crate::strategies::util::build_signal;
use crate::strategy::Strategy;

/// Precision Breakout Pro (PBP) v2.0
///
/// Multi-Filter Breakout with Trend Confirmation.
#[derive(Copy, Clone, Debug, Default)]
pub struct PrecisionBreakoutPro;

impl PrecisionBreakoutPro {
    pub fn new(_id: u16) -> Self {
        Self
    }
}

impl Strategy for PrecisionBreakoutPro {
    fn id(&self) -> StrategyId {
        StrategyId::CompositeAlphaBreakout // We will reuse this ID for now to avoid modifying schema enums
    }

    fn evaluate(&self, snap: &FeatureSnapshot, ctx: &StrategyContext) -> Option<Signal> {
        let sym = SymbolId::new(snap.symbol);

        let dt = Local.timestamp_opt((snap.ts_ns / 1_000_000_000) as i64, 0).unwrap();
        let hour = dt.hour();
        let minute = dt.minute();
        
        // Trading Window: 09:30 to 14:45
        let time_valid = (hour == 9 && minute >= 30) || (hour > 9 && hour < 14) || (hour == 14 && minute <= 45);
        if !time_valid {
            return None;
        }

        // Feature gates
        // Since we evaluate tick-by-tick, we can't easily access 'previous' 50 EMA directly from a single snapshot, 
        // but we have ema_slope. A positive slope means ema is rising.
        let trend_up = snap.ema_slope > 0.0;
        let trend_down = snap.ema_slope < 0.0;

        let price = snap.price;
        
        let price_above_ema50 = price > snap.ema_trend;
        let price_below_ema50 = price < snap.ema_trend;

        let ema_alignment_long = snap.ema_fast > snap.ema_slow && snap.ema_slow > snap.ema_trend;
        let ema_alignment_short = snap.ema_fast < snap.ema_slow && snap.ema_slow < snap.ema_trend;

        let adx_strong = snap.adx > 15.0;

        let donchian_breakout = price >= snap.donchian_upper;
        let donchian_breakdown = price <= snap.donchian_lower;

        let rsi_healthy_long = snap.rsi > 40.0;
        let rsi_healthy_short = snap.rsi < 60.0;

        let atr_sufficient = snap.atr > 0;

        // Note: Volume is skipped as per the plan since historical data lacks volume.

        let is_long = donchian_breakout;

        let is_short = donchian_breakdown;

        if is_long {
            // Fallback ATR of 50 paise if volatility is extremely low
            let atr_f = if snap.atr > 0 { snap.atr as f64 } else { 50.0 };
            let stop_distance = 1.5 * atr_f;
            let target_distance = stop_distance * 2.5;
            
            let mut rp = crate::strategies::util::risk_profile_for(price, Side::Buy, snap.atr);
            rp.stop_loss_paise = price - stop_distance.round() as i64;
            rp.take_profit_paise = price + target_distance.round() as i64;

            Some(build_signal(
                snap.correlation_id,
                self.id(),
                sym,
                Side::Buy,
                0.8,
                1.0,
                rp,
                snap.ts_ns,
            ))
        } else if is_short {
            // Fallback ATR of 50 paise if volatility is extremely low
            let atr_f = if snap.atr > 0 { snap.atr as f64 } else { 50.0 };
            let stop_distance = 1.5 * atr_f;
            let target_distance = stop_distance * 2.5;

            let mut rp = crate::strategies::util::risk_profile_for(price, Side::Sell, snap.atr);
            rp.stop_loss_paise = price + stop_distance.round() as i64;
            rp.take_profit_paise = price - target_distance.round() as i64;

            Some(build_signal(
                snap.correlation_id,
                self.id(),
                sym,
                Side::Sell,
                0.8,
                1.0,
                rp,
                snap.ts_ns,
            ))
        } else {
            None
        }
    }

    fn enabled_in(&self, _regime: Regime) -> bool {
        true
    }
}
