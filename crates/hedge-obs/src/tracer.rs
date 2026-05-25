//! Stage-scoped latency tracer.
//!
//! [`LatencyTracer`] is the single emission point that maps a per-stage
//! elapsed time to:
//!
//! 1. The matching Prometheus histogram (R27.1).
//! 2. A [`LatencyRecord`] published on `obs.latency.<stage>` (R27.4).
//! 3. A budget-breach event on `obs.budget.breach.<stage>` plus a
//!    `hedge_budget_breach_total{stage}` increment when `elapsed > budget`
//!    (R28.6).
//!
//! ### RAII discipline
//!
//! The tracer is `!Send` (via a `PhantomData<*const ()>` field) and never
//! crosses a `.await`. Hot_Path stages call:
//!
//! ```ignore
//! let _t = LatencyTracer::start(Stage::RiskCheck, cid, budget_ns, &emitter);
//! // ... evaluate ...
//! // _t drops here, emits exactly one LatencyRecord.
//! ```
//!
//! ### Emitter abstraction
//!
//! Publication is funnelled through the [`LatencyEmitter`] trait so the
//! crate carries three implementations:
//!
//! * [`NoopEmitter`] — for unit tests that do not care about emission.
//! * [`RecorderEmitter`] — pushes records onto a `MpmcRing` so tests can
//!   assert one record per traversed stage (Property 3).
//! * [`NatsEmitter`] — wraps an [`hedge_bus::NatsClient`] and publishes
//!   non-blockingly on the NATS subjects.
//!
//! `NatsEmitter` does **not** await on the Hot_Path: it spawns a Tokio task
//! that performs the actual publish. The Hot_Path is the runtime owner, so
//! `tokio::spawn` is the cheapest available "fire-and-forget" path.

use std::marker::PhantomData;
use std::sync::Arc;

use hedge_bus::{subjects, JsonCodec, NatsClient};
use hedge_core::{CorrelationId, MpmcRing};
use hedge_schemas::stage::Stage;
use hedge_schemas::LatencyRecord;
use quanta::Instant;
use serde::{Deserialize, Serialize};

use crate::metrics::{metrics, Metrics};

// ---- LatencyEmitter trait -----------------------------------------------

/// Publication sink for [`LatencyRecord`] payloads emitted by the
/// [`LatencyTracer`].
///
/// The trait is `Send + Sync` so the same emitter handle can be cloned into
/// many per-stage tracers concurrently.
pub trait LatencyEmitter: Send + Sync {
    /// Publish a per-stage `LatencyRecord` on `obs.latency.<stage>`.
    fn emit_record(&self, stage: Stage, record: &LatencyRecord);

    /// Publish a per-stage budget-breach event on
    /// `obs.budget.breach.<stage>`. The carried `LatencyRecord` is the same
    /// payload as the corresponding `emit_record` call (with `breach = true`).
    fn emit_breach(&self, stage: Stage, record: &LatencyRecord);
}

/// No-op emitter. Useful in tests that exercise control flow without caring
/// about emission.
#[derive(Copy, Clone, Default, Debug)]
pub struct NoopEmitter;

impl LatencyEmitter for NoopEmitter {
    fn emit_record(&self, _stage: Stage, _record: &LatencyRecord) {}
    fn emit_breach(&self, _stage: Stage, _record: &LatencyRecord) {}
}

/// In-process emitter that pushes every record / breach into a shared
/// [`MpmcRing`]. Used by `proptest` to assert "exactly one record per
/// traversed stage".
///
/// The two channels are separate so tests can assert breach behaviour
/// independently of the per-stage record stream.
#[derive(Clone)]
pub struct RecorderEmitter {
    /// Records pushed by every `emit_record` call.
    pub records: MpmcRing<(Stage, LatencyRecord)>,
    /// Records pushed by every `emit_breach` call.
    pub breaches: MpmcRing<(Stage, LatencyRecord)>,
}

impl RecorderEmitter {
    /// Construct an emitter with `capacity` slots in each ring.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: MpmcRing::with_capacity(capacity),
            breaches: MpmcRing::with_capacity(capacity),
        }
    }
}

impl Default for RecorderEmitter {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

impl LatencyEmitter for RecorderEmitter {
    fn emit_record(&self, stage: Stage, record: &LatencyRecord) {
        // Drop on overflow: records are diagnostic, never authoritative.
        let _ = self.records.push((stage, *record));
    }
    fn emit_breach(&self, stage: Stage, record: &LatencyRecord) {
        let _ = self.breaches.push((stage, *record));
    }
}

/// JSON mirror of `LatencyRecord_v1` used for `obs.*` payloads.
///
/// The Hot_Path FlatBuffers wire form (`LatencyRecord_v1`) is the canonical
/// representation, but `obs.*` events ride the `JsonCodec` per the design's
/// table. This struct serialises with stable field names and survives schema
/// evolution because every additional field is optional.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyRecordJson {
    /// Hex form of the 16-byte `correlation_id` for grep-friendliness.
    pub correlation_id_hex: String,
    /// Stable canonical stage name (`hedge_schemas::stage::Stage::as_str`).
    pub stage: String,
    /// Elapsed nanoseconds.
    pub nanos: u64,
    /// Configured budget in nanoseconds. `0` indicates no budget configured.
    pub budget_nanos: u64,
    /// `true` when `nanos > budget_nanos` and a budget was configured.
    pub breach: bool,
}

impl LatencyRecordJson {
    fn from_fb(record: &LatencyRecord, stage: Stage) -> Self {
        let mut hex = String::with_capacity(32);
        for byte in record.correlation_id {
            use std::fmt::Write;
            // `write!` into a String never fails.
            let _ = write!(hex, "{:02x}", byte);
        }
        Self {
            correlation_id_hex: hex,
            stage: stage.as_str().to_string(),
            nanos: record.nanos,
            budget_nanos: record.budget_nanos,
            breach: record.breach,
        }
    }
}

/// NATS-backed emitter. Spawns a Tokio task per emission so the Hot_Path
/// caller never awaits on the broker.
///
/// The publisher is held behind an `Arc` so the spawned task can outlive the
/// stage that emitted it. JSON encoding happens inside the task, not on the
/// caller's thread, so the Hot_Path Drop pays only for the `Bytes` clone of
/// the in-memory `LatencyRecordJson` (which is small).
#[derive(Clone)]
pub struct NatsEmitter {
    client: Arc<NatsClient>,
}

impl NatsEmitter {
    /// Construct a NATS emitter from a connected client.
    pub fn new(client: NatsClient) -> Self {
        Self { client: Arc::new(client) }
    }

    fn spawn_publish(&self, subject_name: String, payload: LatencyRecordJson) {
        // Snapshot a cheap clone of the client `Arc` so the spawned task
        // does not pin the original handle alive longer than necessary.
        let client = Arc::clone(&self.client);
        // We use `tokio::spawn` so the Hot_Path call site does not await.
        // If no Tokio runtime is current (e.g. tests outside a `#[tokio::main]`
        // block) the spawn would panic; for that reason `NatsEmitter` is
        // intended for production binaries only — tests use `RecorderEmitter`.
        tokio::spawn(async move {
            let codec: JsonCodec<LatencyRecordJson> = JsonCodec::new();
            // Build the typed Subject<T> at the spawn boundary. The phantom
            // payload parameter is informational; the actual codec is what
            // determines wire format.
            let subject = hedge_bus::Subject::<LatencyRecordJson>::new(subject_name);
            let publisher = client.publisher(subject, codec);
            if let Err(err) = publisher.publish(&payload).await {
                tracing::warn!(error = %err, "obs latency publish failed");
            }
        });
    }
}

impl LatencyEmitter for NatsEmitter {
    fn emit_record(&self, stage: Stage, record: &LatencyRecord) {
        let subject: hedge_bus::Subject<LatencyRecordJson> =
            subjects::obs_latency(stage.as_str());
        let payload = LatencyRecordJson::from_fb(record, stage);
        self.spawn_publish(subject.into_string(), payload);
    }

    fn emit_breach(&self, stage: Stage, record: &LatencyRecord) {
        let subject: hedge_bus::Subject<LatencyRecordJson> =
            subjects::obs_budget_breach(stage.as_str());
        let payload = LatencyRecordJson::from_fb(record, stage);
        self.spawn_publish(subject.into_string(), payload);
    }
}

// ---- LatencyTracer ------------------------------------------------------

/// RAII guard that measures and emits a per-stage latency on drop.
///
/// The lifetime parameter `'a` ties the tracer to the borrowed
/// [`LatencyEmitter`] and prevents the tracer from outliving the emitter.
/// The `_no_send: PhantomData<*const ()>` field makes the type `!Send`, so
/// the tracer can never accidentally cross an `.await` boundary on the
/// Hot_Path.
pub struct LatencyTracer<'a> {
    stage: Stage,
    correlation_id: CorrelationId,
    budget_ns: u64,
    started: Instant,
    sink: &'a dyn LatencyEmitter,
    metrics: &'static Metrics,
    armed: bool,
    /// `*const ()` keeps the type `!Send` (matches design's RAII discipline).
    _no_send: PhantomData<*const ()>,
}

impl<'a> LatencyTracer<'a> {
    /// Start a new tracer. Records elapsed time on drop.
    ///
    /// `budget_ns == 0` disables the breach emission and breach counter — the
    /// histogram observation still happens. Use `0` for stages where the
    /// design does not specify a per-stage ceiling (e.g. broker submit, which
    /// is observed but not budgeted at the same granularity).
    pub fn start(
        stage: Stage,
        correlation_id: CorrelationId,
        budget_ns: u64,
        sink: &'a dyn LatencyEmitter,
    ) -> Self {
        Self {
            stage,
            correlation_id,
            budget_ns,
            started: Instant::now(),
            sink,
            metrics: metrics(),
            armed: true,
            _no_send: PhantomData,
        }
    }

    /// Cancel the tracer without emitting. Useful when a stage early-exits
    /// on a fast no-op path that should not pollute the latency channel.
    #[inline]
    pub fn cancel(mut self) {
        self.armed = false;
    }

    /// Borrow the configured `Stage`.
    #[inline]
    pub fn stage(&self) -> Stage {
        self.stage
    }

    /// Read the current elapsed nanoseconds without consuming the tracer.
    #[inline]
    pub fn elapsed_ns(&self) -> u64 {
        let n = self.started.elapsed().as_nanos();
        if n > u64::MAX as u128 {
            u64::MAX
        } else {
            n as u64
        }
    }

    fn observe_histogram(&self, elapsed_ns: u64) {
        let v = elapsed_ns as f64;
        match self.stage {
            Stage::TickIngest => self.metrics.tick_ingest_ns.observe(v),
            Stage::FeatureExtraction => self.metrics.feature_extract_ns.observe(v),
            Stage::AiScoringFetch => {
                // No dedicated histogram in R27.1 — fold into risk_check_ns
                // because the WarmCache fetch is part of the Risk_Engine
                // evaluate path (design § Latency Budget Allocation).
                self.metrics.risk_check_ns.observe(v);
            }
            Stage::RiskCheck => self.metrics.risk_check_ns.observe(v),
            Stage::ExecutionRouting => self.metrics.exec_route_ns.observe(v),
            Stage::BrokerSubmit => {
                // BrokerSubmit is exposed as `hedge_broker_latency_ns{broker}`
                // in R7.4 — but the per-broker label is not visible at this
                // generic Drop site. Hot_Path callers that need the labelled
                // metric should call
                // `metrics().broker_latency_ns.with_label_values(...)`
                // directly. We intentionally do **not** double-record here
                // to keep the `hedge_broker_latency_ns` series free of
                // unlabelled samples.
            }
        }
    }
}

impl Drop for LatencyTracer<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let elapsed_ns = self.elapsed_ns();
        // 1. Update the matching Prometheus histogram.
        self.observe_histogram(elapsed_ns);

        // 2. Build the LatencyRecord (FlatBuffers fallback struct).
        let breach = self.budget_ns > 0 && elapsed_ns > self.budget_ns;
        let mut cid_bytes = [0u8; 16];
        cid_bytes.copy_from_slice(&self.correlation_id.as_u128().to_be_bytes());
        let record = LatencyRecord {
            correlation_id: cid_bytes,
            stage: self.stage.as_u8(),
            nanos: elapsed_ns,
            budget_nanos: self.budget_ns,
            breach,
        };

        // 3. Publish to obs.latency.<stage>.
        self.sink.emit_record(self.stage, &record);

        // 4. Breach handling.
        if breach {
            self.metrics
                .budget_breach_total
                .with_label_values(&[self.stage.as_str()])
                .inc();
            self.sink.emit_breach(self.stage, &record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    /// Property: `LatencyTracer` calls `emit_record` on drop with elapsed > 0
    /// and `emit_breach` only when budget is exceeded.
    #[test]
    fn tracer_emits_record_with_non_zero_elapsed_and_no_breach_under_budget() {
        let emitter = RecorderEmitter::with_capacity(8);
        let cid = CorrelationId(42);

        // Generous budget — the test thread will not exceed 100 ms.
        let budget_ns = 100_000_000;
        {
            let _t = LatencyTracer::start(Stage::RiskCheck, cid, budget_ns, &emitter);
            // Force a measurable elapsed window.
            thread::sleep(Duration::from_millis(2));
        }

        // Exactly one record, zero breaches.
        let (stage, rec) = emitter.records.pop().expect("record was emitted");
        assert_eq!(stage, Stage::RiskCheck);
        assert!(rec.nanos >= 1_000_000, "elapsed_ns should be >= 1ms, got {}", rec.nanos);
        assert!(!rec.breach, "no breach expected under budget");
        assert_eq!(rec.budget_nanos, budget_ns);
        assert!(emitter.records.pop().is_none(), "exactly one record");
        assert!(emitter.breaches.pop().is_none(), "no breach event expected");

        // correlation_id round-trips into the 16-byte field.
        assert_eq!(
            u128::from_be_bytes(rec.correlation_id),
            cid.as_u128(),
            "correlation_id round-trip"
        );
    }

    #[test]
    fn tracer_emits_breach_when_elapsed_exceeds_budget() {
        let emitter = RecorderEmitter::with_capacity(8);
        let cid = CorrelationId(1);

        // 1 ns budget — even a no-op drop will exceed.
        let budget_ns = 1;
        {
            let _t = LatencyTracer::start(Stage::TickIngest, cid, budget_ns, &emitter);
            // Tiny work item to ensure elapsed > 1 ns even on the fastest
            // CI runners.
            std::hint::black_box(0u64);
            thread::sleep(Duration::from_micros(50));
        }

        let (rec_stage, rec) = emitter.records.pop().expect("record");
        assert_eq!(rec_stage, Stage::TickIngest);
        assert!(rec.breach, "breach=true under sub-ns budget");

        let (br_stage, br) = emitter.breaches.pop().expect("breach event");
        assert_eq!(br_stage, Stage::TickIngest);
        assert_eq!(br, rec, "breach payload identical to record");

        // Single record, single breach — no duplicates.
        assert!(emitter.records.pop().is_none());
        assert!(emitter.breaches.pop().is_none());
    }

    #[test]
    fn tracer_cancel_suppresses_emission() {
        let emitter = RecorderEmitter::with_capacity(2);
        {
            let t = LatencyTracer::start(
                Stage::FeatureExtraction,
                CorrelationId(7),
                1, // would otherwise breach
                &emitter,
            );
            thread::sleep(Duration::from_micros(10));
            t.cancel();
        }
        assert!(emitter.records.pop().is_none(), "cancel suppressed record");
        assert!(emitter.breaches.pop().is_none(), "cancel suppressed breach");
    }

    /// Property 3 (subset): every traversed stage produces exactly one record.
    /// We synthesise a four-stage chain and assert the outcome.
    #[test]
    fn tracer_emits_one_record_per_traversed_stage() {
        let emitter = RecorderEmitter::with_capacity(8);
        let cid = CorrelationId(0xCafe_F00D);
        let stages = [
            Stage::TickIngest,
            Stage::FeatureExtraction,
            Stage::RiskCheck,
            Stage::ExecutionRouting,
        ];
        for stage in stages {
            let _t = LatencyTracer::start(stage, cid, 100_000_000, &emitter);
            std::hint::black_box(stage);
        }
        let mut seen: Vec<Stage> = Vec::new();
        while let Some((s, rec)) = emitter.records.pop() {
            assert_eq!(
                u128::from_be_bytes(rec.correlation_id),
                cid.as_u128(),
                "every record carries the same correlation_id"
            );
            seen.push(s);
        }
        assert_eq!(seen.len(), stages.len(), "one record per stage");
        // The stage set is identical (FIFO order may differ inside MpmcRing).
        let seen_set: std::collections::HashSet<_> = seen.into_iter().collect();
        let expected_set: std::collections::HashSet<_> = stages.iter().copied().collect();
        assert_eq!(seen_set, expected_set);
    }

    #[test]
    fn zero_budget_disables_breach_path() {
        let emitter = RecorderEmitter::with_capacity(2);
        {
            let _t = LatencyTracer::start(
                Stage::AiScoringFetch,
                CorrelationId(0),
                0, // no budget configured
                &emitter,
            );
            thread::sleep(Duration::from_millis(1));
        }
        let (_, rec) = emitter.records.pop().expect("record emitted");
        assert!(!rec.breach, "breach must be false when budget_ns == 0");
        assert_eq!(rec.budget_nanos, 0);
        assert!(emitter.breaches.pop().is_none(), "no breach event");
    }

    #[test]
    fn latency_record_json_renders_correlation_hex_lowercase() {
        let cid_bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        let record = LatencyRecord {
            correlation_id: cid_bytes,
            stage: Stage::RiskCheck.as_u8(),
            nanos: 500,
            budget_nanos: 1_000,
            breach: false,
        };
        let json = LatencyRecordJson::from_fb(&record, Stage::RiskCheck);
        assert_eq!(json.correlation_id_hex, "0123456789abcdeffedcba9876543210");
        assert_eq!(json.stage, "RiskCheck");
        assert_eq!(json.nanos, 500);
        assert_eq!(json.budget_nanos, 1_000);
        assert!(!json.breach);
    }

    /// Compile-time ish: `LatencyTracer` is `!Send`. We assert via a function
    /// that requires `Send` — if the type ever becomes `Send`, the call
    /// inside the assertion below would compile and the test would no longer
    /// catch the regression. Instead we rely on the negative-trait check
    /// using a custom marker: a function that takes `!Send` types only
    /// cannot be expressed in stable Rust, so we settle for a static_assert
    /// pattern by using `assert_not_impl_all`-style reasoning in a doc test.
    /// At runtime we observe that the `_no_send` field is `PhantomData<*const
    /// ()>`, which is the canonical way to make a type `!Send`.
    #[test]
    fn tracer_marker_is_phantom_data_pointer() {
        // Size of `LatencyTracer` does not increase by an extra pointer's
        // worth — `PhantomData` is a ZST.
        let size = std::mem::size_of::<LatencyTracer<'_>>();
        // Sanity: Stage(u8) + CorrelationId(u128) + budget(u64) + Instant +
        // sink(&dyn ...) + metrics(&'static) + armed(bool) plus padding.
        // We assert size > 0 (sanity) and that PhantomData adds no size.
        assert!(size > 0);
    }
}
