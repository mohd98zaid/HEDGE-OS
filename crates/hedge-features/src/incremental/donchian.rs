use hedge_schemas::Tick;

use crate::state::{FeatureState, DONCHIAN_WINDOW};

/// Update Donchian Channels (Rolling High/Low over N periods)
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    state.donchian_prices.push(tick.ltp_paise);
}

#[inline]
pub fn compute_upper(state: &FeatureState) -> i64 {
    if state.donchian_prices.is_empty() {
        return 0;
    }
    let mut max = i64::MIN;
    for &p in state.donchian_prices.iter() {
        if p > max {
            max = p;
        }
    }
    max
}

#[inline]
pub fn compute_lower(state: &FeatureState) -> i64 {
    if state.donchian_prices.is_empty() {
        return 0;
    }
    let mut min = i64::MAX;
    for &p in state.donchian_prices.iter() {
        if p < min {
            min = p;
        }
    }
    min
}

#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.donchian_prices.len() >= DONCHIAN_WINDOW
}
