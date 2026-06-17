use hedge_schemas::Tick;

use crate::state::{FeatureState, ADX_WINDOW};

/// Update Average Directional Index (ADX)
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    if state.tick_count == 0 {
        // Handled after tick_count is incremented in engine.rs, so here tick_count is 0 if it's the very first tick evaluated *before* state increment.
        // Wait, in engine.rs, tick_count is advanced AFTER all updates. So tick_count == 0 means first tick.
        state.adx_value = 0.0;
        return;
    }

    let prev_high = state.bar_high_paise as f64; // Approximated since we use tick data
    let prev_low = state.bar_low_paise as f64;
    let prev_close = state.prev_ltp_paise as f64;
    
    // For tick data, High = Low = Close = ltp_paise.
    // So true range is just |ltp - prev_ltp|.
    let ltp = tick.ltp_paise as f64;
    let tr = (ltp - prev_close).abs();
    
    // Directional Movement is essentially 0 on pure tick data without synthetic bars,
    // but we can approximate it by comparing tick to prev tick.
    let up_move = ltp - prev_high;
    let down_move = prev_low - ltp;

    let mut pdm = 0.0;
    let mut ndm = 0.0;

    if up_move > down_move && up_move > 0.0 {
        pdm = up_move;
    }
    if down_move > up_move && down_move > 0.0 {
        ndm = down_move;
    }

    let window = ADX_WINDOW as f64;

    if state.tick_count < ADX_WINDOW as u64 {
        state.adx_smoothed_tr += tr;
        state.adx_smoothed_pdm += pdm;
        state.adx_smoothed_ndm += ndm;
    } else if state.tick_count == ADX_WINDOW as u64 {
        // Initial average
        state.adx_smoothed_tr = state.adx_smoothed_tr / window;
        state.adx_smoothed_pdm = state.adx_smoothed_pdm / window;
        state.adx_smoothed_ndm = state.adx_smoothed_ndm / window;
    } else {
        // Wilder's Smoothing
        state.adx_smoothed_tr = state.adx_smoothed_tr - (state.adx_smoothed_tr / window) + tr;
        state.adx_smoothed_pdm = state.adx_smoothed_pdm - (state.adx_smoothed_pdm / window) + pdm;
        state.adx_smoothed_ndm = state.adx_smoothed_ndm - (state.adx_smoothed_ndm / window) + ndm;

        let mut pdi = 0.0;
        let mut ndi = 0.0;

        if state.adx_smoothed_tr > 0.0 {
            pdi = 100.0 * (state.adx_smoothed_pdm / state.adx_smoothed_tr);
            ndi = 100.0 * (state.adx_smoothed_ndm / state.adx_smoothed_tr);
        }

        let dx = if (pdi + ndi) > 0.0 {
            100.0 * ((pdi - ndi).abs() / (pdi + ndi))
        } else {
            0.0
        };

        if state.tick_count <= (2 * ADX_WINDOW) as u64 {
            state.adx_smoothed_dx = dx; // Just use DX until we have enough to smooth ADX
        } else {
            state.adx_smoothed_dx = (state.adx_smoothed_dx * (window - 1.0) + dx) / window;
        }
        
        state.adx_value = state.adx_smoothed_dx as f32;
    }
}

#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    state.adx_value
}

#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.tick_count >= (2 * ADX_WINDOW) as u64
}
