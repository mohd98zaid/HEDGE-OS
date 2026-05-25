//! Liquidity sweep detector.
//!
//! Source: design § Components § Feature_Extraction_Engine, R3.2.
//!
//! ```text
//! sweep_signal = +1.0  if a fresh high was tagged within the last 3 ticks
//!                       AND the latest LTP has reverted below the prior
//!                       high
//!              = -1.0  symmetric for low sweeps
//!              =  0.0  otherwise
//! ```
//!
//! The signal decays each tick — once 3 ticks pass since the breakout
//! sample, the corresponding break index is forgotten.

use hedge_schemas::Tick;

use crate::state::{FeatureState, SWEEP_LOOKAHEAD};

/// Update the high/low break tracking and the sweep signal.
///
/// Must be called **after** [`super::candle::update`] has folded the new
/// `last_ltp_paise` into state, otherwise the comparison reads stale data.
#[inline]
pub fn update(state: &mut FeatureState, tick: &Tick) {
    let idx = state.tick_count;
    let new_price = tick.ltp_paise;

    // Detect a new local extreme by comparing against `session_high_paise` /
    // `session_low_paise` BEFORE the current tick is folded. The candle
    // module updates the session high/low on each tick; we observe the
    // post-update value, so a "new high" is detected by comparing the
    // tick's LTP against the previous-stored break price.
    if state.session_high_paise > 0 && new_price >= state.session_high_paise && new_price > state.last_high_break_paise {
        state.last_high_break_idx = Some(idx);
        state.last_high_break_paise = new_price;
    }
    if state.session_low_paise > 0 && new_price <= state.session_low_paise && (state.last_low_break_paise == 0 || new_price < state.last_low_break_paise) {
        state.last_low_break_idx = Some(idx);
        state.last_low_break_paise = new_price;
    }

    // Compute the signal: a sweep up is a fresh-high break followed by a
    // reversal below the break price within `SWEEP_LOOKAHEAD` ticks.
    let mut signal = 0.0f32;
    if let Some(b_idx) = state.last_high_break_idx {
        let elapsed = idx.saturating_sub(b_idx);
        if elapsed > 0 && elapsed <= SWEEP_LOOKAHEAD as u64 && new_price < state.last_high_break_paise {
            signal = -1.0; // bullish sweep failed → bearish reversal
        } else if elapsed > SWEEP_LOOKAHEAD as u64 {
            // Decay the tracker.
            state.last_high_break_idx = None;
        }
    }
    if let Some(b_idx) = state.last_low_break_idx {
        let elapsed = idx.saturating_sub(b_idx);
        if elapsed > 0 && elapsed <= SWEEP_LOOKAHEAD as u64 && new_price > state.last_low_break_paise {
            signal = 1.0; // bearish sweep failed → bullish reversal
        } else if elapsed > SWEEP_LOOKAHEAD as u64 {
            state.last_low_break_idx = None;
        }
    }
    state.sweep_signal = signal;
}

/// Returns the cached sweep signal in `{-1.0, 0.0, +1.0}`.
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    state.sweep_signal
}

/// `true` once at least one break has been tagged.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.last_high_break_idx.is_some() || state.last_low_break_idx.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick_with_count;

    #[test]
    fn sweep_zero_before_any_break() {
        let s = FeatureState::default();
        assert_eq!(compute(&s), 0.0);
    }

    #[test]
    fn sweep_negative_one_when_high_break_then_reversal() {
        let mut s = FeatureState::default();
        // Establish a session high.
        s.session_high_paise = 110_00;
        s.session_low_paise = 100_00;
        // Tick t=0: tag a fresh high break.
        s.tick_count = 0;
        s.last_ltp_paise = 110_50;
        s.session_high_paise = 110_50;
        update(&mut s, &tick_with_count(110_50, 0));
        // Tick t=1: revert below the break price → sweep failed.
        s.tick_count = 1;
        s.last_ltp_paise = 108_00;
        update(&mut s, &tick_with_count(108_00, 1));
        assert_eq!(compute(&s), -1.0);
    }

    #[test]
    fn sweep_positive_one_when_low_break_then_reversal() {
        let mut s = FeatureState::default();
        s.session_high_paise = 110_00;
        s.session_low_paise = 100_00;
        s.tick_count = 0;
        s.last_ltp_paise = 99_00;
        s.session_low_paise = 99_00;
        update(&mut s, &tick_with_count(99_00, 0));
        s.tick_count = 1;
        s.last_ltp_paise = 102_00;
        update(&mut s, &tick_with_count(102_00, 1));
        assert_eq!(compute(&s), 1.0);
    }

    #[test]
    fn sweep_decays_after_lookahead_ticks() {
        let mut s = FeatureState::default();
        s.session_high_paise = 110_00;
        s.session_low_paise = 100_00;
        s.tick_count = 0;
        s.last_ltp_paise = 110_50;
        s.session_high_paise = 110_50;
        update(&mut s, &tick_with_count(110_50, 0));
        // 4 ticks later — beyond SWEEP_LOOKAHEAD (3).
        for tc in 1..=(SWEEP_LOOKAHEAD as u64 + 1) {
            s.tick_count = tc;
            s.last_ltp_paise = 110_50; // hold flat
            update(&mut s, &tick_with_count(110_50, tc));
        }
        assert!(s.last_high_break_idx.is_none());
        assert_eq!(compute(&s), 0.0);
    }
}
