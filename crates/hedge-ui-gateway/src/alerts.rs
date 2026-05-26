//! `/alerts` channel: severity-sorted UI-formatted alert stream (R20.5).
//!
//! The cockpit's Alerts panel must surface critical alerts above
//! non-critical ones (R20.5). The gateway aggregates alerts from several
//! NATS sources into a single stream emitted on `/alerts`:
//!
//! | Source NATS subject              | Default severity                   |
//! |----------------------------------|------------------------------------|
//! | `risk.killswitch.activated`      | [`Severity::Critical`]             |
//! | `risk.target.reached`            | [`Severity::Info`]                 |
//! | `exec.broker.failover`           | [`Severity::Warning`]              |
//! | `obs.budget.breach.*`            | [`Severity::Warning`]              |
//! | `obs.error.*`                    | [`Severity::Error`]                |
//! | `ai.gov.action`                  | [`Severity::Warning`]              |
//! | `ai.psych.intervention`          | [`Severity::Warning`]              |
//! | `md.connection.*`                | [`Severity::Warning`]              |
//! | `ai.ollama.degraded`             | [`Severity::Warning`]              |
//!
//! ### Sorting
//!
//! Each delivered NATS event is rendered into a [`UiAlert`] and inserted
//! into a small bounded buffer. The buffer is sorted by `(severity DESC,
//! ts_ns DESC)` before emission so critical alerts always appear above
//! non-critical ones, and within a severity bucket the most recent alert
//! is shown first. The buffer is flushed on every insert so the cockpit
//! sees alerts as they happen.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::VecDeque;

use parking_lot::Mutex;

/// Alert severity levels, ordered Critical > Error > Warning > Info.
///
/// The numeric `Ord` derivation follows declaration order, which is
/// **inverted** from the user-facing ranking (Critical is the highest).
/// Use [`Severity::rank`] when sorting; see [`AlertBuffer::ordered`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Informational notice — kill-switch deactivation, profit-target reached.
    Info,
    /// Warning — broker failover, budget breach, news regime change.
    Warning,
    /// Error — observability error event, invalid token rejection.
    Error,
    /// Critical — kill-switch activation, max-loss breach, catastrophic
    /// component failure.
    Critical,
}

impl Severity {
    /// Numeric rank where higher = more severe. Used for sorting.
    #[inline]
    pub const fn rank(self) -> u8 {
        match self {
            Severity::Info => 0,
            Severity::Warning => 1,
            Severity::Error => 2,
            Severity::Critical => 3,
        }
    }
}

/// Best-guess severity classification for a NATS subject.
///
/// Returns `None` when the subject does not map to any known alert
/// source (no alert is emitted in that case). This is the canonical
/// gateway-side mapping; the React cockpit re-derives the same mapping
/// when rendering, so ordering matches across both sides.
pub fn severity_for_subject(subject: &str) -> Option<Severity> {
    if subject == "risk.killswitch.activated" {
        return Some(Severity::Critical);
    }
    if subject == "risk.target.reached" {
        return Some(Severity::Info);
    }
    if subject == "exec.broker.failover" || subject == "ai.ollama.degraded" {
        return Some(Severity::Warning);
    }
    if subject.starts_with("obs.budget.breach.") {
        return Some(Severity::Warning);
    }
    if subject.starts_with("obs.error.") {
        return Some(Severity::Error);
    }
    if subject == "ai.gov.action" || subject == "ai.psych.intervention" {
        return Some(Severity::Warning);
    }
    if subject.starts_with("md.connection.") {
        return Some(Severity::Warning);
    }
    None
}

/// UI-formatted alert payload emitted on `/alerts`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UiAlert {
    /// Severity bucket — drives ranking in the cockpit.
    pub severity: Severity,
    /// Source NATS subject (so the cockpit can deep-link to the raw event).
    pub source: String,
    /// Wall-clock nanoseconds since UNIX epoch at the moment the alert
    /// was constructed by the gateway.
    pub ts_ns: u128,
    /// Opaque JSON payload carried over from the source NATS event.
    pub payload: Value,
}

/// Bounded severity-sorted alert buffer.
///
/// Inserts cost `O(n log n)` because we re-sort on emission rather than
/// using a binary heap (the buffer is tiny — `capacity` defaults to 256
/// in production — and we want a single deterministic tie-break by
/// timestamp). On overflow the oldest alert (the lowest priority entry
/// after sort, which is the newest after FIFO insertion order) is
/// dropped.
pub struct AlertBuffer {
    inner: Mutex<VecDeque<UiAlert>>,
    capacity: usize,
}

impl AlertBuffer {
    /// Construct a buffer with capacity `cap`.
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(cap)),
            capacity: cap,
        }
    }

    /// Insert an alert, evicting the oldest if at capacity.
    pub fn push(&self, alert: UiAlert) {
        let mut g = self.inner.lock();
        if g.len() >= self.capacity {
            g.pop_front();
        }
        g.push_back(alert);
    }

    /// Number of buffered alerts.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// `true` when no alerts are buffered.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Drain the buffer into a severity-sorted vector.
    ///
    /// Sort key is `(severity DESC, ts_ns DESC)`: critical alerts first,
    /// most recent first within a severity bucket. The buffer is empty
    /// after this call.
    pub fn drain_ordered(&self) -> Vec<UiAlert> {
        let mut g = self.inner.lock();
        let mut v: Vec<UiAlert> = g.drain(..).collect();
        v.sort_by_key(|a| (Reverse(a.severity.rank()), Reverse(a.ts_ns)));
        v
    }

    /// Return a *copy* of the current buffer in severity-sorted order
    /// without draining. Used by tests and snapshot endpoints.
    pub fn ordered(&self) -> Vec<UiAlert> {
        let g = self.inner.lock();
        let mut v: Vec<UiAlert> = g.iter().cloned().collect();
        v.sort_by_key(|a| (Reverse(a.severity.rank()), Reverse(a.ts_ns)));
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn severity_rank_orders_critical_above_others() {
        assert!(Severity::Critical.rank() > Severity::Error.rank());
        assert!(Severity::Error.rank() > Severity::Warning.rank());
        assert!(Severity::Warning.rank() > Severity::Info.rank());
    }

    #[test]
    fn severity_for_subject_classifies_known_subjects() {
        assert_eq!(
            severity_for_subject("risk.killswitch.activated"),
            Some(Severity::Critical)
        );
        assert_eq!(
            severity_for_subject("risk.target.reached"),
            Some(Severity::Info)
        );
        assert_eq!(
            severity_for_subject("exec.broker.failover"),
            Some(Severity::Warning)
        );
        assert_eq!(
            severity_for_subject("obs.budget.breach.risk_check"),
            Some(Severity::Warning)
        );
        assert_eq!(
            severity_for_subject("obs.error.market_data"),
            Some(Severity::Error)
        );
        assert_eq!(
            severity_for_subject("ai.gov.action"),
            Some(Severity::Warning)
        );
        assert_eq!(
            severity_for_subject("ai.psych.intervention"),
            Some(Severity::Warning)
        );
        assert_eq!(
            severity_for_subject("md.connection.nse_l1"),
            Some(Severity::Warning)
        );
        assert_eq!(
            severity_for_subject("ai.ollama.degraded"),
            Some(Severity::Warning)
        );
    }

    #[test]
    fn severity_for_subject_returns_none_for_unknown() {
        assert_eq!(severity_for_subject("md.tick.42"), None);
        assert_eq!(severity_for_subject("sig.emitted"), None);
        assert_eq!(severity_for_subject(""), None);
    }

    #[test]
    fn drain_ordered_puts_critical_above_warning_above_info() {
        let buf = AlertBuffer::new(8);
        buf.push(UiAlert {
            severity: Severity::Info,
            source: "risk.target.reached".into(),
            ts_ns: 100,
            payload: json!({}),
        });
        buf.push(UiAlert {
            severity: Severity::Critical,
            source: "risk.killswitch.activated".into(),
            ts_ns: 50,
            payload: json!({}),
        });
        buf.push(UiAlert {
            severity: Severity::Warning,
            source: "exec.broker.failover".into(),
            ts_ns: 200,
            payload: json!({}),
        });

        let ordered = buf.drain_ordered();
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].severity, Severity::Critical);
        assert_eq!(ordered[1].severity, Severity::Warning);
        assert_eq!(ordered[2].severity, Severity::Info);
        assert!(buf.is_empty());
    }

    #[test]
    fn drain_ordered_within_severity_uses_latest_first() {
        let buf = AlertBuffer::new(8);
        buf.push(UiAlert {
            severity: Severity::Critical,
            source: "a".into(),
            ts_ns: 100,
            payload: json!({}),
        });
        buf.push(UiAlert {
            severity: Severity::Critical,
            source: "b".into(),
            ts_ns: 300,
            payload: json!({}),
        });
        buf.push(UiAlert {
            severity: Severity::Critical,
            source: "c".into(),
            ts_ns: 200,
            payload: json!({}),
        });

        let ordered = buf.drain_ordered();
        assert_eq!(ordered[0].source, "b");
        assert_eq!(ordered[1].source, "c");
        assert_eq!(ordered[2].source, "a");
    }

    #[test]
    fn capacity_evicts_oldest_first() {
        let buf = AlertBuffer::new(2);
        buf.push(UiAlert {
            severity: Severity::Info,
            source: "first".into(),
            ts_ns: 1,
            payload: json!({}),
        });
        buf.push(UiAlert {
            severity: Severity::Info,
            source: "second".into(),
            ts_ns: 2,
            payload: json!({}),
        });
        buf.push(UiAlert {
            severity: Severity::Info,
            source: "third".into(),
            ts_ns: 3,
            payload: json!({}),
        });
        let ordered = buf.drain_ordered();
        assert_eq!(ordered.len(), 2);
        // FIFO eviction → "first" gone; remaining are "third" and "second".
        let sources: Vec<&str> = ordered.iter().map(|a| a.source.as_str()).collect();
        assert!(sources.contains(&"second"));
        assert!(sources.contains(&"third"));
        assert!(!sources.contains(&"first"));
    }
}
