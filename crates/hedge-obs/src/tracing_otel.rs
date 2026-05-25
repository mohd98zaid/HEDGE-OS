//! OpenTelemetry OTLP exporter setup with Jaeger downsampling.
//!
//! Production binaries call [`install_otlp_tracer`] during startup to wire
//! `tracing` spans into Jaeger via the OTLP gRPC pipeline. The exporter is
//! optional — binaries running without an OTLP collector use the
//! [`DownsampledSampler`] alone (or skip OTel entirely) and rely on the JSON
//! `fmt::Layer` from [`crate::logging`].
//!
//! ### Downsampling under load
//!
//! When [`crate::degraded::jaeger_overloaded`] is true, a configurable
//! fraction of spans is dropped at sampling time. The default ratio is
//! `0.10` (per the design's `degraded_mode.sample_traces_at_jaeger_overload`)
//! — meaning ~10 % of spans are kept under load. The sampler implementation
//! uses a deterministic hash of the trace id so the same trace is either
//! kept or dropped consistently across all spans, preserving span/trace
//! consistency even under partial sampling.
//!
//! ### Compatibility
//!
//! Targets the `opentelemetry` 0.24 / `opentelemetry_sdk` 0.24 /
//! `opentelemetry-otlp` 0.17 surface. The custom sampler is wired via
//! [`opentelemetry_sdk::trace::Config::with_sampler`] because `Sampler` is
//! `#[non_exhaustive]` and exposes no `Custom` variant in 0.24 — instead
//! any type implementing
//! [`opentelemetry_sdk::trace::ShouldSample`] is accepted directly by
//! `Config::with_sampler<T: ShouldSample + 'static>`.

use opentelemetry::trace::{
    Link, SamplingDecision, SamplingResult, SpanKind, TraceContextExt, TraceId, TraceState,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::ShouldSample;

use crate::degraded;

/// Sampler that keeps every span at steady state and downsamples to a
/// configured fraction when Jaeger is overloaded.
///
/// Construction inputs:
/// * `overload_keep_ratio` — fraction of spans kept while overloaded
///   (e.g. `0.10`). Clamped to `[0.0, 1.0]`.
#[derive(Debug, Clone)]
pub struct DownsampledSampler {
    overload_keep_ratio: f64,
}

impl DownsampledSampler {
    /// Construct a sampler with the given keep ratio. Values outside
    /// `[0.0, 1.0]` are clamped; NaN folds to 0.
    pub fn new(overload_keep_ratio: f64) -> Self {
        let r = if overload_keep_ratio.is_nan() {
            0.0
        } else {
            overload_keep_ratio.clamp(0.0, 1.0)
        };
        Self { overload_keep_ratio: r }
    }

    /// Returns the configured overload-mode keep ratio.
    pub fn keep_ratio(&self) -> f64 {
        self.overload_keep_ratio
    }

    fn keep_decision(&self, trace_id: TraceId) -> bool {
        // Deterministic by trace id: take the lower 64 bits and compare
        // against `keep_ratio * u64::MAX`. Same trace id always produces the
        // same decision, so child spans of a kept trace stay kept.
        let bytes = trace_id.to_bytes();
        let mut tail = [0u8; 8];
        tail.copy_from_slice(&bytes[8..16]);
        let v = u64::from_be_bytes(tail);
        let threshold = (u64::MAX as f64 * self.overload_keep_ratio) as u64;
        v < threshold
    }
}

impl Default for DownsampledSampler {
    fn default() -> Self {
        // Match the design's `degraded_mode.sample_traces_at_jaeger_overload`
        // default of 0.1.
        Self::new(0.1)
    }
}

impl ShouldSample for DownsampledSampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        _name: &str,
        _span_kind: &SpanKind,
        _attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        let trace_state = parent_context
            .map(|cx| cx.span().span_context().trace_state().clone())
            .unwrap_or_else(TraceState::default);

        let decision = if degraded::jaeger_overloaded() {
            if self.keep_decision(trace_id) {
                SamplingDecision::RecordAndSample
            } else {
                SamplingDecision::Drop
            }
        } else {
            SamplingDecision::RecordAndSample
        };
        SamplingResult {
            decision,
            attributes: Vec::new(),
            trace_state,
        }
    }
}

/// Construct a [`DownsampledSampler`] with the given keep ratio. Exposed so
/// binaries that build their own `TracerProvider` can reuse the same
/// decision rule without re-implementing it.
pub fn build_sampler(overload_keep_ratio: f64) -> DownsampledSampler {
    DownsampledSampler::new(overload_keep_ratio)
}

/// Install the OTLP exporter pipeline with the given collector endpoint and
/// downsampling ratio.
///
/// Returns the constructed `TracerProvider` so the caller can hold it for
/// the process lifetime; dropping it triggers a flush.
///
/// Errors surface as [`crate::error::ObsError::Otel`].
///
/// Wires `opentelemetry-otlp` 0.17's tonic builder pipeline:
///
/// ```text
/// new_exporter().tonic().with_endpoint(endpoint).build_span_exporter()
/// ```
///
/// then attaches the resulting exporter to a `TracerProvider` whose
/// `Config` carries our [`DownsampledSampler`] and a `service.name`
/// resource attribute.
pub fn install_otlp_tracer(
    service_name: &'static str,
    endpoint: &str,
    overload_keep_ratio: f64,
) -> Result<opentelemetry_sdk::trace::TracerProvider, crate::error::ObsError> {
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint)
        .build_span_exporter()
        .map_err(|e| crate::error::ObsError::Otel(e.to_string()))?;

    let config = opentelemetry_sdk::trace::Config::default()
        .with_sampler(DownsampledSampler::new(overload_keep_ratio))
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            service_name,
        )]));

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_config(config)
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trace_id(low_64: u64) -> TraceId {
        let mut b = [0u8; 16];
        b[8..16].copy_from_slice(&low_64.to_be_bytes());
        TraceId::from_bytes(b)
    }

    #[test]
    fn keep_ratio_is_clamped() {
        assert_eq!(DownsampledSampler::new(-1.0).keep_ratio(), 0.0);
        assert_eq!(DownsampledSampler::new(2.5).keep_ratio(), 1.0);
        assert!((DownsampledSampler::new(0.42).keep_ratio() - 0.42).abs() < f64::EPSILON);
        // NaN folds to 0.0 (drop everything when configured incorrectly).
        assert_eq!(DownsampledSampler::new(f64::NAN).keep_ratio(), 0.0);
    }

    #[test]
    fn sampler_keeps_everything_when_not_overloaded() {
        degraded::set_jaeger_overloaded(false);
        let sampler = DownsampledSampler::new(0.10);
        for low in [1u64, 0, u64::MAX, 1234567] {
            let r = sampler.should_sample(
                None,
                make_trace_id(low),
                "name",
                &SpanKind::Internal,
                &[],
                &[],
            );
            assert_eq!(r.decision, SamplingDecision::RecordAndSample);
        }
    }

    #[test]
    fn sampler_drops_above_threshold_when_overloaded() {
        degraded::set_jaeger_overloaded(true);
        let sampler = DownsampledSampler::new(0.10);
        // Trace id with low_64 = 0 is far below the 10 % threshold => kept.
        let kept = sampler.should_sample(
            None,
            make_trace_id(0),
            "name",
            &SpanKind::Internal,
            &[],
            &[],
        );
        assert_eq!(kept.decision, SamplingDecision::RecordAndSample);

        // Trace id with low_64 = u64::MAX is above the threshold => dropped.
        let dropped = sampler.should_sample(
            None,
            make_trace_id(u64::MAX),
            "name",
            &SpanKind::Internal,
            &[],
            &[],
        );
        assert_eq!(dropped.decision, SamplingDecision::Drop);

        degraded::set_jaeger_overloaded(false);
    }

    #[test]
    fn sampler_decision_is_deterministic_per_trace_id() {
        // Property: same trace id ⇒ same decision under the same flag.
        degraded::set_jaeger_overloaded(true);
        let sampler = DownsampledSampler::new(0.5);
        let tid = make_trace_id(0xDEADBEEFCAFEBABE);
        let a = sampler.should_sample(None, tid, "n", &SpanKind::Internal, &[], &[]);
        let b = sampler.should_sample(None, tid, "n", &SpanKind::Internal, &[], &[]);
        assert_eq!(a.decision, b.decision);
        degraded::set_jaeger_overloaded(false);
    }

    #[test]
    fn keep_ratio_zero_drops_all_under_overload() {
        degraded::set_jaeger_overloaded(true);
        let sampler = DownsampledSampler::new(0.0);
        for low in [0u64, 1, u64::MAX / 2, u64::MAX] {
            let r = sampler.should_sample(
                None,
                make_trace_id(low),
                "n",
                &SpanKind::Internal,
                &[],
                &[],
            );
            assert_eq!(r.decision, SamplingDecision::Drop);
        }
        degraded::set_jaeger_overloaded(false);
    }
}
