//! Unified error type for the observability scaffolding.
//!
//! Every fallible operation in `hedge-obs` returns [`ObsError`]. The variants
//! map 1:1 to the failure modes called out in design § "Error Handling §
//! Degraded Telemetry":
//!
//! * **`PrometheusInit` / `PrometheusRegister`** — failures from
//!   `prometheus::register_*_with_registry!` and the once-only
//!   [`crate::metrics::registry`] handle.
//! * **`Otel`** — failures from the OTLP gRPC tracer pipeline. We render the
//!   underlying `opentelemetry::trace::TraceError` to a `String` because that
//!   type is not `Clone` and because callers route on the variant rather than
//!   on the inner cause.
//! * **`Logging`** — `tracing-subscriber` initialisation failures (a global
//!   subscriber was already set, an env-filter directive was malformed, ...).
//! * **`LokiShipper`** — surfaced by the shipper task in
//!   `loki_shipper.rs`; degraded behaviour is handled internally so this is
//!   only used for **terminal** failures the caller must observe.

use thiserror::Error;

/// Crate-level error type. Construct with the helper conversions where
/// available so call sites stay compact.
#[derive(Debug, Error)]
pub enum ObsError {
    /// The Prometheus registry could not be initialised. Currently only
    /// raised by [`crate::metrics::registry`] when a duplicate registration
    /// is attempted across two different runtimes (which would indicate a
    /// programming error — registries are process-global by design).
    #[error("prometheus init failed: {0}")]
    PrometheusInit(String),

    /// `prometheus::register_*_with_registry!` reported an error. The most
    /// common cause is a name/help/labels triple that has already been
    /// registered against the same registry.
    #[error("prometheus register failed: {0}")]
    PrometheusRegister(String),

    /// The OpenTelemetry OTLP exporter pipeline failed to install. The
    /// downstream binary should treat this as fatal at startup but **not**
    /// during a session — tracing failures must not take down the Hot_Path.
    #[error("otlp exporter init failed: {0}")]
    Otel(String),

    /// `tracing-subscriber` failed to install the global subscriber. This is
    /// almost always "subscriber already set" and is safe to log-and-continue
    /// in tests; production binaries should fail closed at startup.
    #[error("tracing subscriber init failed: {0}")]
    Logging(String),

    /// Terminal failure inside the optional Loki shipper task.
    #[error("loki shipper failure: {0}")]
    LokiShipper(String),

    /// Caller-supplied configuration is invalid (e.g. malformed URL).
    #[error("invalid observability config: {0}")]
    Config(String),
}

impl From<prometheus::Error> for ObsError {
    #[inline]
    fn from(e: prometheus::Error) -> Self {
        Self::PrometheusRegister(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_renders_each_variant() {
        let cases: &[(ObsError, &str)] = &[
            (ObsError::PrometheusInit("dup".into()), "prometheus init failed: dup"),
            (ObsError::PrometheusRegister("x".into()), "prometheus register failed: x"),
            (ObsError::Otel("y".into()), "otlp exporter init failed: y"),
            (ObsError::Logging("z".into()), "tracing subscriber init failed: z"),
            (ObsError::LokiShipper("net".into()), "loki shipper failure: net"),
            (ObsError::Config("url".into()), "invalid observability config: url"),
        ];
        for (err, expected) in cases {
            assert_eq!(format!("{}", err), *expected);
        }
    }

    #[test]
    fn prometheus_error_converts_via_from() {
        let p = prometheus::Error::AlreadyReg;
        let o: ObsError = p.into();
        match o {
            ObsError::PrometheusRegister(_) => {}
            other => panic!("expected PrometheusRegister, got {:?}", other),
        }
    }
}
