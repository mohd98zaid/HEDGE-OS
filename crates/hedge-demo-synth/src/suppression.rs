//! `SuppressionRegistry` — synth's "back off when a real publisher is alive"
//! mechanism (REQ-2.* of the full-cockpit-data spec).
//!
//! Each generator wraps every publish in `if registry.allow_publish(subject)`.
//! The registry watches a shared NATS subject pool (every subject the synth
//! itself publishes on) and remembers, per subject, when the most recent
//! non-`_synth` payload was seen. While a "real publisher seen" window is
//! active, `allow_publish` returns `false` and the generator skips that
//! tick.
//!
//! ### Echo detection
//!
//! Synth payloads carry `"_synth": true` at the top level. `record_message`
//! parses the payload as JSON and only updates state when the `_synth`
//! flag is absent or `false`. Synth's own publishes therefore never cause
//! self-suppression.
//!
//! ### Concurrency
//!
//! `DashMap` makes the registry cheap to share across many generator tasks
//! without explicit locking. `Instant` is used so wall-clock changes (e.g.
//! NTP step) don't break the suppression window.

use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde_json::Value;

/// Default suppression window: when a real publisher is observed on a
/// subject, synth defers for this duration.
pub const DEFAULT_SUPPRESSION_WINDOW: Duration = Duration::from_secs(5);

/// Per-subject suppression state.
#[derive(Copy, Clone, Debug, Default)]
struct SubjectState {
    last_real_seen_at: Option<Instant>,
    suppressed_until: Option<Instant>,
}

/// Tracks per-subject "real publisher detected" state.
///
/// Cheap to clone (internal `Arc<DashMap>`).
#[derive(Clone, Debug)]
pub struct SuppressionRegistry {
    inner: std::sync::Arc<SuppressionInner>,
}

#[derive(Debug)]
struct SuppressionInner {
    map: DashMap<String, SubjectState>,
    window: Duration,
}

impl SuppressionRegistry {
    /// Construct with the default 5-second window.
    pub fn new() -> Self {
        Self::with_window(DEFAULT_SUPPRESSION_WINDOW)
    }

    /// Construct with a custom suppression window (mainly for tests).
    pub fn with_window(window: Duration) -> Self {
        Self {
            inner: std::sync::Arc::new(SuppressionInner {
                map: DashMap::new(),
                window,
            }),
        }
    }

    /// Record an inbound NATS message on `subject`. If the message is a
    /// JSON object containing `"_synth": true`, this is a self-echo and
    /// the call is a no-op. Otherwise update the per-subject state so
    /// future `allow_publish` calls return `false` for the next
    /// `window` interval.
    pub fn record_message(&self, subject: &str, payload: &[u8]) {
        if Self::is_synth_echo(payload) {
            return;
        }
        let now = Instant::now();
        self.inner
            .map
            .entry(subject.to_string())
            .and_modify(|s| {
                s.last_real_seen_at = Some(now);
                s.suppressed_until = Some(now + self.inner.window);
            })
            .or_insert_with(|| SubjectState {
                last_real_seen_at: Some(now),
                suppressed_until: Some(now + self.inner.window),
            });
    }

    /// Returns `true` when the synth is allowed to publish on `subject`.
    pub fn allow_publish(&self, subject: &str) -> bool {
        match self.inner.map.get(subject) {
            None => true,
            Some(state) => match state.suppressed_until {
                None => true,
                Some(deadline) => Instant::now() >= deadline,
            },
        }
    }

    /// Number of subjects currently being suppressed. Useful in tests
    /// and `tracing` summaries.
    pub fn suppressed_count(&self) -> usize {
        let now = Instant::now();
        self.inner
            .map
            .iter()
            .filter(|kv| {
                kv.value()
                    .suppressed_until
                    .map(|t| now < t)
                    .unwrap_or(false)
            })
            .count()
    }

    /// Inspect the suppression deadline for a single subject. `None` when
    /// the subject has never been observed or is no longer suppressed.
    pub fn suppressed_until(&self, subject: &str) -> Option<Instant> {
        self.inner
            .map
            .get(subject)
            .and_then(|s| s.suppressed_until)
            .filter(|t| Instant::now() < *t)
    }

    /// Detect a synth-tagged payload. Treats anything except a JSON object
    /// with `"_synth": true` (boolean) as a real-publisher payload. We
    /// deliberately do not require valid JSON: binary frames (Phase B's
    /// `Tick_v1` for example) will fail to parse and be classified as
    /// real, which is exactly what we want — binary always means a real
    /// publisher.
    fn is_synth_echo(payload: &[u8]) -> bool {
        let v: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return false,
        };
        v.get("_synth")
            .and_then(|x| x.as_bool())
            .unwrap_or(false)
    }
}

impl Default for SuppressionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn unknown_subject_is_allowed() {
        let r = SuppressionRegistry::new();
        assert!(r.allow_publish("md.tick.RELIANCE"));
    }

    #[test]
    fn real_payload_suppresses_subject() {
        let r = SuppressionRegistry::with_window(Duration::from_millis(100));
        r.record_message("md.tick.RELIANCE", br#"{"kind":"tick","data":{}}"#);
        assert!(!r.allow_publish("md.tick.RELIANCE"));
    }

    #[test]
    fn synth_echo_does_not_suppress() {
        let r = SuppressionRegistry::with_window(Duration::from_millis(100));
        r.record_message(
            "md.tick.RELIANCE",
            br#"{"kind":"tick","data":{},"_synth":true}"#,
        );
        assert!(r.allow_publish("md.tick.RELIANCE"));
    }

    #[test]
    fn binary_payload_counts_as_real() {
        // 93 zero bytes is not valid JSON; treat as real publisher.
        let r = SuppressionRegistry::with_window(Duration::from_millis(100));
        let bin: Vec<u8> = vec![0u8; 93];
        r.record_message("md.tick.bin.RELIANCE", &bin);
        assert!(!r.allow_publish("md.tick.bin.RELIANCE"));
    }

    #[test]
    fn suppression_expires_after_window() {
        let r = SuppressionRegistry::with_window(Duration::from_millis(40));
        r.record_message("of.event.RELIANCE", br#"{"kind":"event"}"#);
        assert!(!r.allow_publish("of.event.RELIANCE"));
        sleep(Duration::from_millis(60));
        assert!(r.allow_publish("of.event.RELIANCE"));
    }

    #[test]
    fn suppressed_count_tracks_active_subjects() {
        let r = SuppressionRegistry::with_window(Duration::from_millis(100));
        assert_eq!(r.suppressed_count(), 0);
        r.record_message("a", br#"{"kind":"x"}"#);
        r.record_message("b", br#"{"kind":"x"}"#);
        assert_eq!(r.suppressed_count(), 2);
        r.record_message("a", br#"{"_synth":true}"#); // does nothing
        assert_eq!(r.suppressed_count(), 2);
    }

    #[test]
    fn suppressed_until_returns_some_only_during_window() {
        let r = SuppressionRegistry::with_window(Duration::from_millis(50));
        assert!(r.suppressed_until("x").is_none());
        r.record_message("x", br#"{"kind":"x"}"#);
        assert!(r.suppressed_until("x").is_some());
        sleep(Duration::from_millis(70));
        assert!(r.suppressed_until("x").is_none());
    }

    #[test]
    fn synth_flag_must_be_boolean_true() {
        // `"_synth": "true"` (string) does NOT count as a synth echo.
        let r = SuppressionRegistry::with_window(Duration::from_millis(100));
        r.record_message("y", br#"{"_synth":"true"}"#);
        assert!(!r.allow_publish("y"));
    }
}
