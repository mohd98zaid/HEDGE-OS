//! High-volatility presentation mode (R20.4).
//!
//! When `md.breadth.volatility` exceeds `ui.high_vol_threshold`, the
//! cockpit must increase the refresh rate of critical panels (positions,
//! risk, exec, latency) and reduce secondary visual elements. The
//! gateway's role here is to:
//!
//! 1. Track the latest `md.breadth.volatility` reading (as a JSON
//!    payload — the FlatBuffers `md.breadth.volatility` event is decoded
//!    upstream and forwarded as JSON on the `/market` channel).
//! 2. Compare against the configured threshold from
//!    [`hedge_config::UiConfig::high_vol_threshold`].
//! 3. Emit a [`ServerMsg::Mode`](crate::protocol::ServerMsg::Mode)
//!    transition only when the boolean flips, so the cockpit does not
//!    receive a steady stream of redundant `mode` events.
//! 4. Adjust the gateway's per-channel batching cadence so critical
//!    channels are flushed more aggressively while in high-volatility
//!    mode (the [`RefreshCadence`] surface).
//!
//! ### Threshold semantics
//!
//! High-volatility mode is *strictly greater than* the configured
//! threshold (`>`, not `>=`) so a default threshold of `0.05` does not
//! latch on at exactly `0.05` (a common steady-state value). Hysteresis
//! is intentionally **not** introduced: the spec calls for a single
//! threshold per R20.4 / R32 § ui.high_vol_threshold, and the cockpit
//! debounces on its own side if the measurement chatters.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;

/// Per-channel refresh cadence applied by the gateway loop.
///
/// In **normal mode**, the gateway flushes its per-WebSocket send queue
/// every `normal` interval. In **high-volatility mode**, the gateway
/// flushes critical channels every `critical_high_vol` interval, leaving
/// secondary channels at `secondary_high_vol`. Defaults match the
/// design's "critical panels at 60 fps target, secondary at lower fps"
/// guidance (R20.1).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RefreshCadence {
    /// Default cadence applied to every channel when not in high-vol mode.
    pub normal: Duration,
    /// Cadence applied to *critical* channels (Risk, Exec, Latency,
    /// Alerts) while in high-vol mode.
    pub critical_high_vol: Duration,
    /// Cadence applied to *secondary* channels (News, Psych, Replay)
    /// while in high-vol mode.
    pub secondary_high_vol: Duration,
}

impl RefreshCadence {
    /// Default cadence: 33 ms normal (~30 fps), 16 ms critical-high-vol
    /// (~60 fps), 100 ms secondary-high-vol (10 fps).
    pub const DEFAULT: RefreshCadence = RefreshCadence {
        normal: Duration::from_millis(33),
        critical_high_vol: Duration::from_millis(16),
        secondary_high_vol: Duration::from_millis(100),
    };

    /// Returns the cadence that should apply to `channel` given whether
    /// the gateway is currently in high-volatility mode.
    ///
    /// "Critical" channels are: Risk, Exec, Latency, Alerts.
    /// Everything else is "secondary".
    pub fn for_channel(self, channel: crate::protocol::Channel, high_vol: bool) -> Duration {
        use crate::protocol::Channel as C;
        if !high_vol {
            return self.normal;
        }
        match channel {
            C::Risk | C::Exec | C::Latency | C::Alerts | C::Market | C::Orderflow => {
                self.critical_high_vol
            }
            _ => self.secondary_high_vol,
        }
    }
}

/// High-volatility mode tracker.
///
/// Edge-triggered: [`VolatilityTracker::observe`] returns
/// `Some(true)` only when the boolean flips from `false → true`, and
/// `Some(false)` only when it flips back. Any non-flipping update
/// returns `None` so the gateway does not flood the WebSocket with
/// redundant `mode` events.
pub struct VolatilityTracker {
    threshold: f32,
    /// Current state. `true` while the latest reading exceeded threshold.
    in_high_vol: AtomicBool,
}

impl VolatilityTracker {
    /// Construct a tracker with the given threshold.
    pub fn new(threshold: f32) -> Self {
        Self {
            threshold,
            in_high_vol: AtomicBool::new(false),
        }
    }

    /// Construct a tracker from `UiConfig`.
    pub fn from_config(cfg: &hedge_config::UiConfig) -> Self {
        Self::new(cfg.high_vol_threshold)
    }

    /// Configured threshold value.
    pub fn threshold(&self) -> f32 {
        self.threshold
    }

    /// Current high-volatility state.
    pub fn is_high_vol(&self) -> bool {
        self.in_high_vol.load(Ordering::Acquire)
    }

    /// Observe a new `md.breadth.volatility` payload and return a state
    /// transition if any.
    ///
    /// The payload is JSON; the `value` field is read as the volatility
    /// reading. Payloads without a numeric `value` field are ignored
    /// (the tracker's state does not change). NaN and negative
    /// readings are clamped to `0.0` so a misbehaving upstream cannot
    /// flap us into high-vol mode.
    pub fn observe(&self, payload: &Value) -> Option<bool> {
        let raw = payload.get("value").and_then(Value::as_f64)?;
        let v = if raw.is_nan() { 0.0 } else { raw.max(0.0) };
        let next = (v as f32) > self.threshold;
        self.set(next)
    }

    /// Set the tracker state directly. Returns `Some(next)` only on a
    /// flip. Used by tests and by the gateway when it has already
    /// computed the boolean elsewhere.
    pub fn set(&self, next: bool) -> Option<bool> {
        let prev = self.in_high_vol.swap(next, Ordering::AcqRel);
        if prev != next {
            Some(next)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Channel;
    use serde_json::json;

    #[test]
    fn refresh_cadence_critical_channels_use_high_vol_interval_when_high_vol() {
        let c = RefreshCadence::DEFAULT;
        assert_eq!(c.for_channel(Channel::Risk, true), c.critical_high_vol);
        assert_eq!(c.for_channel(Channel::Exec, true), c.critical_high_vol);
        assert_eq!(c.for_channel(Channel::Latency, true), c.critical_high_vol);
        assert_eq!(c.for_channel(Channel::Alerts, true), c.critical_high_vol);
        assert_eq!(c.for_channel(Channel::Market, true), c.critical_high_vol);
        assert_eq!(c.for_channel(Channel::Orderflow, true), c.critical_high_vol);
    }

    #[test]
    fn refresh_cadence_secondary_channels_use_secondary_interval_when_high_vol() {
        let c = RefreshCadence::DEFAULT;
        assert_eq!(c.for_channel(Channel::News, true), c.secondary_high_vol);
        assert_eq!(c.for_channel(Channel::Psych, true), c.secondary_high_vol);
        assert_eq!(c.for_channel(Channel::Replay, true), c.secondary_high_vol);
    }

    #[test]
    fn refresh_cadence_uses_normal_interval_when_not_high_vol() {
        let c = RefreshCadence::DEFAULT;
        for ch in Channel::ALL {
            assert_eq!(c.for_channel(ch, false), c.normal);
        }
    }

    #[test]
    fn observe_flips_to_high_vol_when_value_exceeds_threshold() {
        let t = VolatilityTracker::new(0.05);
        assert!(!t.is_high_vol());

        // exact threshold does not flip (strictly greater)
        let r = t.observe(&json!({"value": 0.05}));
        assert_eq!(r, None);
        assert!(!t.is_high_vol());

        // exceed threshold → flips on
        let r = t.observe(&json!({"value": 0.06}));
        assert_eq!(r, Some(true));
        assert!(t.is_high_vol());

        // staying high → no flip
        let r = t.observe(&json!({"value": 0.10}));
        assert_eq!(r, None);

        // back below threshold → flips off
        let r = t.observe(&json!({"value": 0.04}));
        assert_eq!(r, Some(false));
        assert!(!t.is_high_vol());
    }

    #[test]
    fn observe_ignores_payloads_without_numeric_value_field() {
        let t = VolatilityTracker::new(0.05);
        assert_eq!(t.observe(&json!({})), None);
        assert_eq!(t.observe(&json!({"foo": 1})), None);
        assert_eq!(t.observe(&json!({"value": "bad"})), None);
    }

    #[test]
    fn observe_clamps_nan_and_negative_to_zero() {
        let t = VolatilityTracker::new(0.05);
        assert_eq!(t.observe(&json!({"value": f64::NAN})), None);
        assert_eq!(t.observe(&json!({"value": -1.0})), None);
        assert!(!t.is_high_vol());
    }

    #[test]
    fn from_config_picks_up_threshold() {
        let cfg = hedge_config::UiConfig { high_vol_threshold: 0.07 };
        let t = VolatilityTracker::from_config(&cfg);
        assert!((t.threshold() - 0.07).abs() < 1e-6);
    }
}
