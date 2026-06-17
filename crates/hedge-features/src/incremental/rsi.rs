use hedge_schemas::Tick;

use crate::state::{FeatureState, RSI_WINDOW};

/// Update RSI using Wilder's Smoothing Method
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    let price = tick.ltp_paise as f64;
    
    // If it's the first tick, we just store it (prev_ltp_paise will be set after this)
    if state.tick_count == 0 {
        state.rsi_value = 0.0;
        return;
    }

    let prev_price = state.prev_ltp_paise as f64;
    let change = price - prev_price;

    let mut current_gain = 0.0;
    let mut current_loss = 0.0;

    if change > 0.0 {
        current_gain = change;
    } else {
        current_loss = change.abs();
    }

    if state.tick_count < RSI_WINDOW as u64 {
        // Simple moving average for the first RSI_WINDOW ticks
        state.rsi_avg_gain = (state.rsi_avg_gain * (state.tick_count as f64 - 1.0) + current_gain) / state.tick_count as f64;
        state.rsi_avg_loss = (state.rsi_avg_loss * (state.tick_count as f64 - 1.0) + current_loss) / state.tick_count as f64;
    } else {
        // Wilder's Smoothing
        let window = RSI_WINDOW as f64;
        state.rsi_avg_gain = (state.rsi_avg_gain * (window - 1.0) + current_gain) / window;
        state.rsi_avg_loss = (state.rsi_avg_loss * (window - 1.0) + current_loss) / window;
    }

    if state.rsi_avg_loss == 0.0 {
        state.rsi_value = 100.0;
    } else {
        let rs = state.rsi_avg_gain / state.rsi_avg_loss;
        state.rsi_value = (100.0 - (100.0 / (1.0 + rs))) as f32;
    }
}

#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    state.rsi_value
}

#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.tick_count >= RSI_WINDOW as u64
}
