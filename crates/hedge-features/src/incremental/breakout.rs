//! Breakout pressure — proximity to session high/low weighted by volume.
//!
//! Source: design § Components § Feature_Extraction_Engine, R3.2.
//!
//! Computed as:
//!
//! ```text
//! prox_high = (close - sess_low)  / (sess_high - sess_low)        in [0, 1]
//! prox_low  = (sess_high - close) / (sess_high - sess_low)        in [0, 1]
//! pressure  = prox_high - prox_low                                in [-1, 1]
//! pressure *= volume_ratio                                         scaled
//! ```
//!
//! `volume_ratio` is `min(rolling_delta_cached.abs() / 100, 1.0)`,
//! providing a soft "high-volume confirmation" amplifier without ever
//! pushing the result outside `[-1, 1]`. The Signal_Engine
//! Volatility_Compression_Breakout strategy reads the absolute value;
//! the sign disambiguates upper vs lower breaks.

use hedge_schemas::Tick;

use crate::state::FeatureState;

/// `update` is a no-op — breakout pressure is read directly from the
/// session high/low / rolling-delta state already maintained by the
/// candle and rolling-delta modules.
#[inline]
pub fn update(_state: &mut FeatureState, _tick: &Tick) {}

/// Returns breakout pressure in `[-1.0, 1.0]`.
#[inline]
pub fn compute(state: &FeatureState) -> f32 {
    let close = state.last_ltp_paise;
    let high = state.session_high_paise;
    let low = state.session_low_paise;
    if close == 0 || high == 0 || low == 0 {
        return 0.0;
    }
    let range = (high - low) as f64;
    if range <= 0.0 {
        return 0.0;
    }
    let prox_high = (close - low) as f64 / range;
    let prox_low = (high - close) as f64 / range;
    let raw = prox_high - prox_low; // in [-1, 1]
    let vol_ratio = (state.rolling_delta_cached.abs() as f64 / 100.0).min(1.0).max(0.0);
    (raw * vol_ratio).clamp(-1.0, 1.0) as f32
}

/// `true` once a high and a low are tagged.
#[inline]
pub fn is_ready(state: &FeatureState) -> bool {
    state.session_high_paise > 0 && state.session_low_paise > 0 && state.session_high_paise > state.session_low_paise
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn breakout_zero_before_warmup() {
        let s = FeatureState::default();
        assert_eq!(compute(&s), 0.0);
    }

    #[test]
    fn breakout_at_session_high_with_volume_yields_positive() {
        let mut s = FeatureState::default();
        s.session_high_paise = 110_00;
        s.session_low_paise = 100_00;
        s.last_ltp_paise = 110_00;
        s.rolling_delta_cached = 200; // saturates to 1.0 multiplier
        let v = compute(&s);
        assert!(v > 0.5, "got {}", v);
    }

    #[test]
    fn breakout_at_session_low_with_volume_yields_negative() {
        let mut s = FeatureState::default();
        s.session_high_paise = 110_00;
        s.session_low_paise = 100_00;
        s.last_ltp_paise = 100_00;
        s.rolling_delta_cached = -200;
        let v = compute(&s);
        assert!(v < -0.5, "got {}", v);
    }

    #[test]
    fn breakout_zero_volume_yields_zero() {
        let mut s = FeatureState::default();
        s.session_high_paise = 110_00;
        s.session_low_paise = 100_00;
        s.last_ltp_paise = 105_00;
        s.rolling_delta_cached = 0;
        assert_eq!(compute(&s), 0.0);
    }

    #[test]
    fn breakout_clamped_to_unit_range() {
        let mut s = FeatureState::default();
        s.session_high_paise = 200_00;
        s.session_low_paise = 100_00;
        s.last_ltp_paise = 200_00;
        s.rolling_delta_cached = 1_000_000;
        let v = compute(&s);
        assert!(v <= 1.0 && v >= -1.0);
    }
}
