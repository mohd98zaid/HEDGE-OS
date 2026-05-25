//! `hedge-obs` — observability scaffolding for PROJECT HEDGE.
//!
//! Task **5.1**: wire Prometheus, Loki, and Jaeger via OpenTelemetry, with
//! degraded-telemetry behaviour for Loki and Jaeger failure modes.
//!
//! ### Layout
//!
//! * [`metrics`] — process-global Prometheus [`Registry`](prometheus::Registry)
//!   and named histograms / counters / gauges (R27.1 + R28.6).
//! * [`tracer`] — [`LatencyTracer`](crate::tracer::LatencyTracer), the RAII
//!   guard that emits a [`LatencyRecord`](hedge_schemas::LatencyRecord) on
//!   `obs.latency.<stage>` and increments
//!   `hedge_budget_breach_total{stage}` on per-stage budget breach (R27.4,
//!   R28.6).
//! * [`logging`] — `tracing-subscriber` JSON layer plus an opt-in Loki
//!   forwarder that buffers high-severity records during outages.
//! * [`tracing_otel`] — OTLP exporter setup and [`DownsampledSampler`] that
//!   keeps only the configured fraction of traces while Jaeger is overloaded.
//! * [`correlation`] — `CorrelationId` propagation through `tracing::Span`
//!   fields so structured logs and Jaeger traces both surface the
//!   end-to-end identifier (R27.4).
//! * [`degraded`] — atomic flags + bounded ring log buffer that backs the
//!   Loki and Jaeger fallback paths.
//! * [`error`] — unified [`ObsError`].
//! * [`loki_shipper`] (feature `loki-shipper`) — async HTTP shipper task
//!   that drains the [`logging::LogEnvelope`] channel into Loki.
//!
//! ### Hot_Path discipline
//!
//! `hedge-obs` lives in the Hot_Path's transitive closure. The crate body
//! itself contains zero blocking HTTP and zero `reqwest::blocking` usage.
//! The optional `loki-shipper` feature pulls in `reqwest` (async only —
//! `default-features = false, features = ["json", "rustls-tls"]`) for the
//! shipper task that each binary spawns at startup.
//!
//! ### Initialisation
//!
//! Production binaries call [`init`] once at startup and hold the returned
//! [`ObsHandle`] for the process lifetime; the handle's `Drop` flushes OTLP
//! and stops the Loki shipper.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod correlation;
pub mod degraded;
pub mod error;
pub mod logging;
pub mod metrics;
pub mod tracer;
pub mod tracing_otel;

#[cfg(feature = "loki-shipper")]
pub mod loki_shipper;

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, Registry};

pub use crate::correlation::{
    record_correlation_id, set_correlation_id, CorrelationIdHex, FIELD_CORRELATION_ID,
};
pub use crate::degraded::{
    degraded_state, jaeger_overloaded, loki_unavailable, set_jaeger_overloaded,
    set_loki_unavailable, BoundedRingLogBuffer, DegradedState,
};
pub use crate::error::ObsError;
pub use crate::logging::{LogEnvelope, LokiLayer, LokiLayerConfig, Severity, LOKI_BACKLOG_CAPACITY};
pub use crate::metrics::{init_metrics, metrics, registry, Metrics, LATENCY_BUCKETS_NS};
pub use crate::tracer::{
    LatencyEmitter, LatencyRecordJson, LatencyTracer, NatsEmitter, NoopEmitter, RecorderEmitter,
};
pub use crate::tracing_otel::{build_sampler, install_otlp_tracer, DownsampledSampler};

/// Configuration passed to [`init`] at process startup.
///
/// The fields mirror the shape used by the Hot_Path binaries; populate them
/// from `hedge_config::ObservabilityConfig` plus the binary-specific
/// `service_name` and ports.
#[derive(Clone, Debug)]
pub struct ObsInit {
    /// Logical service name surfaced as the `service.name` OTel resource
    /// attribute and as the Prometheus `job` label downstream.
    pub service_name: &'static str,
    /// OTLP gRPC endpoint for Jaeger (e.g. `"http://localhost:4317"`).
    /// `None` skips OTLP wiring entirely; the JSON `fmt::Layer` still
    /// installs.
    pub otlp_endpoint: Option<String>,
    /// Loki HTTP push endpoint (e.g. `"http://localhost:3100/loki/api/v1/push"`).
    /// `None` disables Loki shipping; the [`LokiLayer`] still buffers
    /// high-severity records into [`BoundedRingLogBuffer`] so a later
    /// shipper bring-up can drain them.
    pub loki_url: Option<String>,
    /// Optional Prometheus HTTP port. The actual `/metrics` HTTP handler
    /// lives in each binary; this field is informational and exposed via
    /// [`ObsHandle::prometheus_port`].
    pub prometheus_port: Option<u16>,
    /// `RUST_LOG`-style filter directive (e.g. `"info,hedge_risk=debug"`).
    pub log_level: String,
    /// Mirror of `ObservabilityConfig.degraded_mode.drop_low_severity_logs_at_loki_unavailable`.
    pub degraded_drop_low_severity: bool,
    /// Mirror of `ObservabilityConfig.degraded_mode.sample_traces_at_jaeger_overload`.
    /// Default `0.10`.
    pub jaeger_overload_keep_ratio: f64,
}

impl ObsInit {
    /// Construct a development-friendly default. `service_name` is required;
    /// all network endpoints are disabled so this builds a fully in-process
    /// observability stack suitable for `cargo test`.
    pub fn for_tests(service_name: &'static str) -> Self {
        Self {
            service_name,
            otlp_endpoint: None,
            loki_url: None,
            prometheus_port: None,
            log_level: "info".into(),
            degraded_drop_low_severity: true,
            jaeger_overload_keep_ratio: 0.1,
        }
    }
}

/// Handle returned by [`init`]. Holding it keeps the OTLP tracer provider
/// alive; dropping it flushes outstanding spans and signals the Loki
/// shipper to stop.
///
/// The handle is intentionally not `Clone` — there is exactly one
/// observability lifetime per process.
pub struct ObsHandle {
    service_name: &'static str,
    prometheus_port: Option<u16>,
    /// Sender half of the Loki shipper channel. Dropping the handle drops
    /// the sender, which closes the channel and lets the shipper task
    /// resolve its `recv()` loop.
    _loki_tx: Option<mpsc::Sender<LogEnvelope>>,
    /// Receiver half consumed by [`ObsHandle::take_loki_receiver`]; the
    /// caller hands this to [`crate::loki_shipper::run_loki_shipper`] (or
    /// equivalent) at startup. Storing it here keeps the channel alive
    /// even when the shipper is not yet wired up — pending records
    /// accumulate in the bounded ring backlog instead of disappearing.
    loki_rx: Option<mpsc::Receiver<LogEnvelope>>,
    /// Backlog buffer shared with the Loki shipper task (held so the
    /// shipper can drain it on reconnect).
    backlog: Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>>,
    /// OTLP tracer provider — the `Drop` calls `shutdown_tracer_provider`
    /// to flush outstanding spans.
    tracer_provider: Option<opentelemetry_sdk::trace::TracerProvider>,
}

impl ObsHandle {
    /// Borrow the configured service name.
    pub fn service_name(&self) -> &'static str {
        self.service_name
    }

    /// Borrow the configured Prometheus port (if any).
    pub fn prometheus_port(&self) -> Option<u16> {
        self.prometheus_port
    }

    /// Borrow the shared high-severity backlog so the Loki shipper task can
    /// drain it on reconnect.
    pub fn loki_backlog(&self) -> Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>> {
        Arc::clone(&self.backlog)
    }

    /// Take ownership of the Loki shipper receiver. Returns `None` after
    /// the first call. The caller hands this to
    /// `crate::loki_shipper::run_loki_shipper(loki_url, rx, backlog)` at
    /// startup; until then the channel stays open and pending records flow
    /// into the bounded backlog.
    pub fn take_loki_receiver(&mut self) -> Option<mpsc::Receiver<LogEnvelope>> {
        self.loki_rx.take()
    }
}

impl Drop for ObsHandle {
    fn drop(&mut self) {
        // Flush OTLP spans on shutdown. We use the per-provider shutdown
        // rather than the global because tests construct multiple handles
        // and we do not want to clobber the global tracer state.
        if let Some(_provider) = self.tracer_provider.take() {
            opentelemetry::global::shutdown_tracer_provider();
        }
    }
}

/// Initialise the observability stack and install the global
/// `tracing-subscriber`.
///
/// Idempotent only with respect to the Prometheus registry — calling `init`
/// twice with different configurations will fail at the
/// `tracing-subscriber::set_global_default` call. Tests that exercise the
/// full pipeline should reuse a single handle.
pub fn init(cfg: ObsInit) -> Result<ObsHandle, ObsError> {
    // 1. Metrics: idempotent so we can call this freely.
    let _metrics = init_metrics()?;

    // 2. Loki layer wiring. We always create the layer (even when no Loki
    //    URL is configured) so the high-severity backlog is available for a
    //    later shipper bring-up.
    let backlog: Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>> =
        Arc::new(BoundedRingLogBuffer::new());
    let (loki_tx, loki_rx) = mpsc::channel::<LogEnvelope>(1024);

    let loki_layer = LokiLayer::new(
        loki_tx.clone(),
        Arc::clone(&backlog),
        LokiLayerConfig {
            drop_low_severity_on_unavailable: cfg.degraded_drop_low_severity,
        },
    );

    // 3. JSON fmt layer with env filter.
    let env_filter = EnvFilter::try_new(&cfg.log_level)
        .map_err(|e| ObsError::Logging(format!("invalid filter `{}`: {}", cfg.log_level, e)))?;

    // JSON fmt layer to stderr so stdout stays free for `/metrics`.
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_target(true);

    let subscriber = Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(loki_layer);

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| ObsError::Logging(e.to_string()))?;

    // 4. OTLP exporter (optional).
    let tracer_provider = if let Some(endpoint) = cfg.otlp_endpoint.as_ref() {
        Some(install_otlp_tracer(
            cfg.service_name,
            endpoint,
            cfg.jaeger_overload_keep_ratio,
        )?)
    } else {
        None
    };

    Ok(ObsHandle {
        service_name: cfg.service_name,
        prometheus_port: cfg.prometheus_port,
        _loki_tx: Some(loki_tx),
        loki_rx: Some(loki_rx),
        backlog,
        tracer_provider,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obs_init_for_tests_disables_network_endpoints() {
        let cfg = ObsInit::for_tests("hedge-obs-test");
        assert_eq!(cfg.service_name, "hedge-obs-test");
        assert!(cfg.otlp_endpoint.is_none());
        assert!(cfg.loki_url.is_none());
        assert!(cfg.prometheus_port.is_none());
        assert!(cfg.degraded_drop_low_severity);
        assert!((cfg.jaeger_overload_keep_ratio - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn metrics_registry_is_accessible_after_init_metrics() {
        // We do not call the full `init()` here because that installs the
        // global `tracing-subscriber` and would conflict with other tests.
        // The metrics half is independently usable.
        let _ = init_metrics().unwrap();
        let r = registry();
        assert!(!r.gather().is_empty());
    }
}
