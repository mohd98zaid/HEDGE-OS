//! `CorrelationId` propagation through `tracing::Span` fields.
//!
//! Every event spawned from a single tick — through orderflow, features,
//! signals, risk, execution, and broker submission — carries the same 128-bit
//! [`CorrelationId`]. The Latency Budget Allocation table in design.md notes
//! that "every stage stamps the same `correlation_id`"; this module is the
//! glue that surfaces the id into `tracing` spans so it lands in Loki and
//! Jaeger automatically.
//!
//! ### Convention
//!
//! Spans created inside Hot_Path crates should declare the `correlation_id`
//! field at construction:
//!
//! ```ignore
//! let span = tracing::info_span!("risk_check", correlation_id = %CorrelationIdHex::EMPTY);
//! let _g = span.enter();
//! correlation::set_correlation_id(cid);
//! ```
//!
//! `tracing::Span::record` then writes the canonical hex form into the field
//! without reallocating the span. The hex form mirrors the
//! [`AI_RANK`](hedge_bus::AI_RANK) subject's `<cid>` segment, so a single
//! `correlation_id` value is enough to grep across structured logs, traces,
//! and the `obs.latency.<stage>` payloads (which carry the raw `[u8; 16]`).
//!
//! ### Why a hex helper?
//!
//! `CorrelationId` is a thin newtype around `u128`. `tracing` would happily
//! format it with the `Display` impl, but `Display` would render the decimal
//! form — which is awkward to grep against the hex strings emitted by the
//! NATS subject helpers. [`CorrelationIdHex`] wraps the value and guarantees
//! a 32-character lowercase hex rendering.

use std::fmt;

use hedge_core::CorrelationId;
use tracing::Span;

/// Canonical name of the `correlation_id` span field. Use this constant so
/// callers cannot drift from the agreed key.
pub const FIELD_CORRELATION_ID: &str = "correlation_id";

/// Wrapper around `CorrelationId` that renders as 32-character lowercase
/// hex. Use when injecting into `tracing` field values.
///
/// ```ignore
/// span.record(FIELD_CORRELATION_ID, &tracing::field::display(CorrelationIdHex(cid)));
/// ```
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct CorrelationIdHex(pub CorrelationId);

impl CorrelationIdHex {
    /// Sentinel hex form for `CorrelationId::NIL`.
    pub const EMPTY: &'static str = "00000000000000000000000000000000";
}

impl fmt::Display for CorrelationIdHex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0.as_u128())
    }
}

impl From<CorrelationId> for CorrelationIdHex {
    #[inline]
    fn from(c: CorrelationId) -> Self {
        Self(c)
    }
}

/// Record `cid` on the current span under [`FIELD_CORRELATION_ID`].
///
/// The current span must have been created with `correlation_id` declared as
/// a field (e.g. `info_span!("...", correlation_id = tracing::field::Empty)`)
/// or `tracing::Span::record` becomes a no-op. The `tracing` library prints
/// a debug-time warning in that case; this function does not surface the
/// warning to keep the Hot_Path call site allocation-free.
#[inline]
pub fn set_correlation_id(cid: CorrelationId) {
    Span::current().record(
        FIELD_CORRELATION_ID,
        tracing::field::display(CorrelationIdHex(cid)),
    );
}

/// Like [`set_correlation_id`] but writes onto an arbitrary span rather than
/// the current one. Used by spawn-on-drop emitters that want to attribute
/// the recording span explicitly.
#[inline]
pub fn record_correlation_id(span: &Span, cid: CorrelationId) {
    span.record(
        FIELD_CORRELATION_ID,
        tracing::field::display(CorrelationIdHex(cid)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hex_constant_matches_default_correlation_id() {
        let nil = CorrelationIdHex(CorrelationId::NIL);
        assert_eq!(format!("{}", nil), CorrelationIdHex::EMPTY);
        assert_eq!(CorrelationIdHex::EMPTY.len(), 32);
    }

    #[test]
    fn hex_renders_full_32_chars_with_leading_zeros() {
        let cid = CorrelationId(1);
        let hex = format!("{}", CorrelationIdHex(cid));
        assert_eq!(hex.len(), 32);
        assert_eq!(hex, "00000000000000000000000000000001");
    }

    #[test]
    fn hex_renders_all_low_hex_digits() {
        let cid = CorrelationId(0xABCDEF1234567890_FEDCBA0987654321u128);
        let hex = format!("{}", CorrelationIdHex(cid));
        assert_eq!(hex, "abcdef1234567890fedcba0987654321");
        // Exhaustive check: only [0-9a-f] characters.
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn set_correlation_id_round_trips_through_a_span() {
        // Property: a value written via `set_correlation_id` is observable on
        // the span via the visitor API. We use a custom subscriber that
        // captures recorded fields to make the round-trip deterministic.
        use std::sync::{Arc, Mutex};
        use tracing::subscriber;
        use tracing_subscriber::{layer::SubscriberExt, Registry};
        use tracing_subscriber::Layer;

        #[derive(Default)]
        struct Capture {
            inner: Arc<Mutex<Vec<(String, String)>>>,
        }

        impl<S> Layer<S> for Capture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_record(
                &self,
                _id: &tracing::span::Id,
                values: &tracing::span::Record<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                struct V<'a>(&'a Arc<Mutex<Vec<(String, String)>>>);
                impl tracing::field::Visit for V<'_> {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        self.0
                            .lock()
                            .unwrap()
                            .push((field.name().to_string(), format!("{:?}", value)));
                    }
                    fn record_str(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &str,
                    ) {
                        self.0
                            .lock()
                            .unwrap()
                            .push((field.name().to_string(), value.to_string()));
                    }
                }
                values.record(&mut V(&self.inner));
            }
        }

        let captured: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let layer = Capture { inner: captured.clone() };
        let subscriber = Registry::default().with(layer);

        subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "test_stage",
                correlation_id = tracing::field::Empty
            );
            let _g = span.enter();
            set_correlation_id(CorrelationId(0x1234));
        });

        let captured = captured.lock().unwrap();
        let entry = captured
            .iter()
            .find(|(k, _)| k == FIELD_CORRELATION_ID)
            .expect("correlation_id field was recorded");
        assert!(entry.1.ends_with("00000000000000000000000000001234"), "got `{}`", entry.1);
    }
}
