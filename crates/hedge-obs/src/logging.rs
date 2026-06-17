//! `tracing-subscriber` setup with JSON output and a Loki-shipping
//! fallback.
//!
//! Two layers compose into the global subscriber:
//!
//! 1. **`fmt::Layer` (JSON)** — emits structured records to stderr. The
//!    layer always installs; its `correlation_id`, `subject`, and `stage`
//!    fields are picked up via the standard `tracing::Span` recording API
//!    (see [`crate::correlation`]).
//! 2. **`LokiLayer`** — pushes high-severity records to the optional Loki
//!    HTTP endpoint via a non-blocking `tokio::sync::mpsc` channel. The
//!    actual HTTP shipping happens in [`crate::loki_shipper`] (gated by
//!    the `loki-shipper` feature) so this crate stays free of `reqwest`
//!    in its default build.
//!
//! ### Degraded behaviour
//!
//! When [`crate::degraded::loki_unavailable`] is true:
//! * Low-severity records are dropped at the source if the
//!   `degraded_drop_low_severity` flag was set in [`crate::ObsInit`].
//! * High-severity records are buffered into a [`BoundedRingLogBuffer<256>`]
//!   so the shipper task can drain them on reconnect.

use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::degraded::{self, BoundedRingLogBuffer};

/// Capacity of the high-severity backlog buffer used while Loki is down.
pub const LOKI_BACKLOG_CAPACITY: usize = 256;

/// Severity classification used for the drop-low-severity / buffer-high-
/// severity policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Severity {
    /// `TRACE`, `DEBUG`, `INFO`.
    Low,
    /// `WARN`, `ERROR`.
    High,
}

impl Severity {
    fn from_level(level: &Level) -> Self {
        if level <= &Level::WARN {
            Severity::High
        } else {
            Severity::Low
        }
    }
}

/// JSON-serialisable structured log record sent to the Loki shipper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEnvelope {
    /// Wallclock nanos since UNIX epoch (set by the caller).
    pub timestamp_ns: u128,
    /// Log level as the `tracing` short string (`"INFO"`, `"WARN"`, ...).
    pub level: String,
    /// Logger target (typically `module_path!()`).
    pub target: String,
    /// The structured message body.
    pub message: String,
    /// Severity bucket used by the degraded-mode policy.
    pub severity: Severity,
}

/// Configuration for [`LokiLayer`]. Independent of how the actual HTTP push
/// is implemented — the layer emits onto an `mpsc` channel; the consumer
/// (the Loki shipper task) is the component that talks HTTP.
pub struct LokiLayerConfig {
    /// Drop low-severity records when the Loki endpoint is unreachable.
    pub drop_low_severity_on_unavailable: bool,
}

/// Optional `tracing` layer that forwards records into the Loki shipper.
///
/// The layer holds:
/// * an `mpsc::Sender<LogEnvelope>` that pipes records to the shipper task,
/// * a `BoundedRingLogBuffer<256>` that retains high-severity records when
///   the shipper signals the endpoint is unreachable.
pub struct LokiLayer {
    sender: Option<mpsc::Sender<LogEnvelope>>,
    backlog: Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>>,
    config: RwLock<LokiLayerConfig>,
}

impl LokiLayer {
    /// Construct a layer that ships through `sender` and buffers into the
    /// shared `backlog` while Loki is unreachable.
    pub fn new(
        sender: mpsc::Sender<LogEnvelope>,
        backlog: Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>>,
        config: LokiLayerConfig,
    ) -> Self {
        Self {
            sender: Some(sender),
            backlog,
            config: RwLock::new(config),
        }
    }

    /// Construct a layer with no shipping channel — every event is buffered
    /// into the backlog. Used in unit tests.
    pub fn buffered_only(
        backlog: Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>>,
        config: LokiLayerConfig,
    ) -> Self {
        Self {
            sender: None,
            backlog,
            config: RwLock::new(config),
        }
    }

    /// Borrow the shared backlog for tests and the shipper task.
    pub fn backlog(&self) -> Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>> {
        Arc::clone(&self.backlog)
    }

    fn handle_envelope(&self, env: LogEnvelope) {
        if degraded::loki_unavailable() {
            // Loki is down — buffer high-severity, drop low-severity per
            // configured policy.
            match env.severity {
                Severity::High => {
                    self.backlog.push(env);
                }
                Severity::Low => {
                    if !self.config.read().drop_low_severity_on_unavailable {
                        self.backlog.push(env);
                    }
                    // else: dropped silently.
                }
            }
            return;
        }
        // Loki is reachable — try the channel; if it is full, fall back to
        // buffering. `try_send` is non-blocking; the Hot_Path Drop site does
        // not need to await.
        if let Some(sender) = &self.sender {
            if sender.try_send(env.clone()).is_err() {
                self.backlog.push(env);
            }
        } else {
            // No shipper wired up; buffer.
            self.backlog.push(env);
        }
    }
}

impl<S> Layer<S> for LokiLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let severity = Severity::from_level(metadata.level());

        // Render the message body via a tracing visitor.
        struct Visitor {
            message: String,
        }
        impl tracing::field::Visit for Visitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "message" {
                    self.message = value.to_string();
                } else if !self.message.is_empty() {
                    use std::fmt::Write;
                    let _ = write!(self.message, " {}={}", field.name(), value);
                }
            }
            fn record_debug(
                &mut self,
                field: &tracing::field::Field,
                value: &dyn std::fmt::Debug,
            ) {
                if field.name() == "message" {
                    self.message = format!("{:?}", value);
                } else if !self.message.is_empty() {
                    use std::fmt::Write;
                    let _ = write!(self.message, " {}={:?}", field.name(), value);
                }
            }
        }
        let mut v = Visitor { message: String::new() };
        event.record(&mut v);

        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);

        let env = LogEnvelope {
            timestamp_ns,
            level: metadata.level().to_string(),
            target: metadata.target().to_string(),
            message: v.message,
            severity,
        };
        self.handle_envelope(env);
    }
}

/// Build the JSON `fmt::Layer` shared by every binary. The layer writes to
/// `stderr` so `stdout` remains free for the `/metrics` HTTP handler when
/// embedded into a CLI.
///
/// Returns an opaque [`Layer`] so callers do not have to spell out the
/// `tracing_subscriber::fmt::Layer` type's full generic form, which is
/// large and changes across minor versions of `tracing-subscriber`.
pub fn json_fmt_layer<S>() -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_target(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::{layer::SubscriberExt, Registry};

    fn fresh_backlog(
    ) -> Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>> {
        Arc::new(BoundedRingLogBuffer::new())
    }

    #[test]
    fn severity_classification_matches_level() {
        assert_eq!(Severity::from_level(&Level::TRACE), Severity::Low);
        assert_eq!(Severity::from_level(&Level::DEBUG), Severity::Low);
        assert_eq!(Severity::from_level(&Level::INFO), Severity::Low);
        assert_eq!(Severity::from_level(&Level::WARN), Severity::High);
        assert_eq!(Severity::from_level(&Level::ERROR), Severity::High);
    }

    #[test]
    fn loki_layer_buffers_when_no_shipper_channel() {
        let _guard = crate::degraded::TEST_MUTEX.lock().unwrap();
        // Reset degraded flag.
        degraded::set_loki_unavailable(false);

        let backlog = fresh_backlog();
        let layer = LokiLayer::buffered_only(
            backlog.clone(),
            LokiLayerConfig {
                drop_low_severity_on_unavailable: true,
            },
        );
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::error!("ship this");
            tracing::info!("buffer this too — no shipper");
        });

        let drained = backlog.drain();
        // Both records buffer because no shipper channel is wired up.
        assert_eq!(drained.len(), 2);
        let levels: Vec<&str> = drained.iter().map(|e| e.level.as_str()).collect();
        assert!(levels.contains(&"ERROR"));
        assert!(levels.contains(&"INFO"));
    }

    #[test]
    fn loki_layer_drops_low_severity_when_unavailable_and_policy_says_drop() {
        let _guard = crate::degraded::TEST_MUTEX.lock().unwrap();
        degraded::set_loki_unavailable(true);
        let backlog = fresh_backlog();
        let layer = LokiLayer::buffered_only(
            backlog.clone(),
            LokiLayerConfig {
                drop_low_severity_on_unavailable: true,
            },
        );
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("low — dropped");
            tracing::error!("high — buffered");
        });

        let drained = backlog.drain();
        assert_eq!(drained.len(), 1, "low-severity dropped");
        assert_eq!(drained[0].level, "ERROR");
        assert_eq!(drained[0].severity, Severity::High);

        // Reset for other tests.
        degraded::set_loki_unavailable(false);
    }

    #[test]
    fn loki_layer_buffers_low_severity_when_policy_keeps_them() {
        let _guard = crate::degraded::TEST_MUTEX.lock().unwrap();
        degraded::set_loki_unavailable(true);
        let backlog = fresh_backlog();
        let layer = LokiLayer::buffered_only(
            backlog.clone(),
            LokiLayerConfig {
                drop_low_severity_on_unavailable: false,
            },
        );
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("low — buffered too");
            tracing::error!("high");
        });
        let drained = backlog.drain();
        assert_eq!(drained.len(), 2);
        degraded::set_loki_unavailable(false);
    }

    #[test]
    fn loki_layer_uses_channel_when_loki_is_available() {
        let _guard = crate::degraded::TEST_MUTEX.lock().unwrap();
        degraded::set_loki_unavailable(false);
        let backlog = fresh_backlog();
        let (tx, mut rx) = mpsc::channel::<LogEnvelope>(8);
        let layer = LokiLayer::new(
            tx,
            backlog.clone(),
            LokiLayerConfig {
                drop_low_severity_on_unavailable: true,
            },
        );
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::error!("via channel");
        });

        // Channel received the record; backlog stayed empty.
        let env = rx.try_recv().expect("channel delivered envelope");
        assert_eq!(env.level, "ERROR");
        assert!(backlog.is_empty(), "backlog unused while Loki is up");
    }
}
