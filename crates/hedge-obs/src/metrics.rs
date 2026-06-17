//! Prometheus registry and named metric handles.
//!
//! Every metric required by R27.1 + R28.6 is registered against a single
//! process-global [`Registry`]. The registration is idempotent: calling
//! [`init_metrics`] twice yields one underlying registry and one set of
//! collectors. Concrete handles are exposed through [`metrics()`] so call
//! sites can update counters and observe histograms without re-resolving the
//! collector.
//!
//! ### Buckets
//!
//! Latency histograms use the bucket set specified in task 5.1:
//!
//! ```text
//! [100, 250, 500, 1_000, 2_000, 3_000, 5_000, 10_000, 20_000, 50_000, 100_000]
//! ```
//!
//! Units are **nanoseconds**, so the bucket boundaries cover the design's
//! per-stage budgets (tick ingest 2 ms, feature extract 3 ms, risk check 2 ms,
//! execution route 5 ms) and the 50 ms end-to-end ceiling.
//!
//! `hedge_slippage_bps` shares a separate bucket set tuned to basis points.

use once_cell::sync::OnceCell;
use prometheus::{
    register_counter_vec_with_registry, register_gauge_vec_with_registry,
    register_gauge_with_registry, register_histogram_vec_with_registry,
    register_histogram_with_registry, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec,
    Registry,
};

use crate::error::ObsError;

/// Latency histogram bucket boundaries, in **nanoseconds**.
///
/// Covers the 100 ns floor up through the 100 ms ceiling. The 100 ns bucket
/// is small enough to detect inflation in the WarmCache atomic-load path
/// (design § Latency Budget Allocation: AI scoring fetch < 50 µs).
pub const LATENCY_BUCKETS_NS: &[f64] = &[
    100.0, 250.0, 500.0, 1_000.0, 2_000.0, 3_000.0, 5_000.0, 10_000.0, 20_000.0, 50_000.0,
    100_000.0,
];

/// Slippage histogram bucket boundaries, in **basis points**. The Risk_Engine
/// (R5.8) blocks new entries when slippage exceeds a configured threshold,
/// typically 25 bps; the bucket set here brackets that.
pub const SLIPPAGE_BUCKETS_BPS: &[f64] =
    &[0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0];

/// Bundle of every named metric the design names in R27.1 + R28.6.
///
/// All handles are cheap to clone (`Arc` underneath) — clone freely into
/// per-stage emitters. Latency histograms intentionally have no labels so
/// the per-stage routing is encoded in the metric *name*; this matches the
/// design's "per-stage Prometheus counter" wording and keeps the time-series
/// cardinality bounded.
#[derive(Clone)]
pub struct Metrics {
    /// `hedge_tick_ingest_ns` — Market_Data_Engine receive→publish (R28.1).
    pub tick_ingest_ns: Histogram,
    /// `hedge_feature_extract_ns` — Feature_Extraction_Engine compute (R28.2).
    pub feature_extract_ns: Histogram,
    /// `hedge_risk_check_ns` — Risk_Engine evaluate (R28.3 / R5.12).
    pub risk_check_ns: Histogram,
    /// `hedge_exec_route_ns` — Execution_Engine route (R28.4 / R6.1).
    pub exec_route_ns: Histogram,
    /// `hedge_broker_latency_ns{broker}` — broker submit round-trip (R7.4).
    pub broker_latency_ns: HistogramVec,
    /// `hedge_slippage_bps{symbol}` — observed slippage per fill (R5.8).
    pub slippage_bps: HistogramVec,
    /// `hedge_websocket_drops_total{source}` — websocket disconnect counter (R1.6).
    pub websocket_drops_total: CounterVec,
    /// `hedge_risk_anomaly_total{kind}` — risk-anomaly counter (R5.10, R5.11).
    pub risk_anomaly_total: CounterVec,
    /// `hedge_trader_emotional_risk` — Trader_Stability_Score derived risk gauge (R16).
    pub trader_emotional_risk: Gauge,
    /// `hedge_ai_drift{component}` — per-component AI drift gauge (R24.1).
    pub ai_drift: GaugeVec,
    /// `hedge_budget_breach_total{stage}` — per-stage breach counter (R28.6).
    pub budget_breach_total: CounterVec,
}

struct InitState {
    registry: Registry,
    metrics: Metrics,
}

/// Process-global lazy-initialised metric set.
///
/// We use `OnceCell::get_or_try_init` to handle the once-only initialisation
/// race-free without needing a separate `Mutex`. `OnceCell` itself ensures
/// that exactly one initialiser runs to completion and any concurrent caller
/// observes the same `InitState` afterwards.
static STATE: OnceCell<InitState> = OnceCell::new();

/// Initialise the registry and register every named metric, returning a
/// reference to the bundle.
///
/// **Idempotent** (R27.1 expectation): calling twice returns the same bundle
/// and does not re-register any collector. The returned `Metrics` is cheap
/// to clone — handles are reference-counted internally.
pub fn init_metrics() -> Result<&'static Metrics, ObsError> {
    let state = STATE.get_or_try_init(|| -> Result<InitState, ObsError> {
        let registry = Registry::new();
        let metrics = build_metrics(&registry)?;
        Ok(InitState { registry, metrics })
    })?;
    Ok(&state.metrics)
}

/// Borrow the previously-initialised metric set.
///
/// Calls [`init_metrics`] internally, so call sites can drop the explicit
/// init step if they only need read access.
pub fn metrics() -> &'static Metrics {
    init_metrics().expect("metrics init must succeed at process start")
}

/// Borrow the underlying [`prometheus::Registry`]. Used by each binary's
/// `/metrics` HTTP handler to render the exposition format.
pub fn registry() -> &'static Registry {
    init_metrics().expect("metrics init must succeed at process start");
    &STATE.get().expect("init succeeded").registry
}

fn build_metrics(reg: &Registry) -> Result<Metrics, ObsError> {
    // Latency histograms (no labels — stage is in the metric name).
    let tick_ingest_ns = register_histogram_with_registry!(
        prometheus::HistogramOpts::new(
            "hedge_tick_ingest_ns",
            "Tick ingest latency, nanoseconds (R28.1)",
        )
        .buckets(LATENCY_BUCKETS_NS.to_vec()),
        reg
    )?;
    let feature_extract_ns = register_histogram_with_registry!(
        prometheus::HistogramOpts::new(
            "hedge_feature_extract_ns",
            "Feature extraction latency, nanoseconds (R28.2)",
        )
        .buckets(LATENCY_BUCKETS_NS.to_vec()),
        reg
    )?;
    let risk_check_ns = register_histogram_with_registry!(
        prometheus::HistogramOpts::new(
            "hedge_risk_check_ns",
            "Risk_Engine evaluate latency, nanoseconds (R28.3, R5.12)",
        )
        .buckets(LATENCY_BUCKETS_NS.to_vec()),
        reg
    )?;
    let exec_route_ns = register_histogram_with_registry!(
        prometheus::HistogramOpts::new(
            "hedge_exec_route_ns",
            "Execution_Engine route latency, nanoseconds (R28.4, R6.1)",
        )
        .buckets(LATENCY_BUCKETS_NS.to_vec()),
        reg
    )?;

    // Labelled histograms.
    let broker_latency_ns = register_histogram_vec_with_registry!(
        prometheus::HistogramOpts::new(
            "hedge_broker_latency_ns",
            "Broker submit round-trip latency, nanoseconds (R7.4)",
        )
        .buckets(LATENCY_BUCKETS_NS.to_vec()),
        &["broker"],
        reg
    )?;
    let slippage_bps = register_histogram_vec_with_registry!(
        prometheus::HistogramOpts::new(
            "hedge_slippage_bps",
            "Observed slippage per fill, basis points (R5.8)",
        )
        .buckets(SLIPPAGE_BUCKETS_BPS.to_vec()),
        &["symbol"],
        reg
    )?;

    // Counters.
    let websocket_drops_total = register_counter_vec_with_registry!(
        "hedge_websocket_drops_total",
        "WebSocket disconnect count per source (R1.6)",
        &["source"],
        reg
    )?;
    let risk_anomaly_total = register_counter_vec_with_registry!(
        "hedge_risk_anomaly_total",
        "Risk anomaly count per kind (R5.10, R5.11)",
        &["kind"],
        reg
    )?;
    let budget_breach_total = register_counter_vec_with_registry!(
        "hedge_budget_breach_total",
        "Per-stage latency-budget breach count (R28.6)",
        &["stage"],
        reg
    )?;

    // Gauges.
    let trader_emotional_risk = register_gauge_with_registry!(
        "hedge_trader_emotional_risk",
        "Trader emotional-risk gauge derived from Trader_Stability_Score (R16)",
        reg
    )?;
    let ai_drift = register_gauge_vec_with_registry!(
        "hedge_ai_drift",
        "Per-component AI drift gauge (R24.1)",
        &["component"],
        reg
    )?;

    Ok(Metrics {
        tick_ingest_ns,
        feature_extract_ns,
        risk_check_ns,
        exec_route_ns,
        broker_latency_ns,
        slippage_bps,
        websocket_drops_total,
        risk_anomaly_total,
        trader_emotional_risk,
        ai_drift,
        budget_breach_total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `init_metrics()` is idempotent — call twice and observe the same
    /// underlying handles by registering a counter once and reading the same
    /// value back.
    #[test]
    fn init_metrics_is_idempotent() {
        let a = init_metrics().expect("first init");
        let b = init_metrics().expect("second init");
        // Fast pointer-equality check: `&'static Metrics` from both calls
        // resolves to the same allocation.
        assert!(std::ptr::eq(a as *const _, b as *const _));
    }

    /// Every named metric required by R27.1 + R28.6 appears in the registry's
    /// gather output.
    #[test]
    fn registry_contains_every_named_metric() {
        let m = init_metrics().unwrap();
        
        // Ensure vectors are instantiated so they appear in gather output
        m.broker_latency_ns.with_label_values(&["zerodha"]).observe(0.0);
        m.slippage_bps.with_label_values(&["RELIANCE"]).observe(0.0);
        m.websocket_drops_total.with_label_values(&["nse_l1"]).inc();
        m.risk_anomaly_total.with_label_values(&["volatility_block"]).inc();
        m.budget_breach_total.with_label_values(&["RiskCheck"]).inc();
        m.ai_drift.with_label_values(&["AI_Trade_Ranking"]).set(0.0);

        let families = registry().gather();
        let names: std::collections::HashSet<_> =
            families.iter().map(|f| f.get_name().to_string()).collect();

        for required in [
            "hedge_tick_ingest_ns",
            "hedge_feature_extract_ns",
            "hedge_risk_check_ns",
            "hedge_exec_route_ns",
            "hedge_broker_latency_ns",
            "hedge_slippage_bps",
            "hedge_websocket_drops_total",
            "hedge_risk_anomaly_total",
            "hedge_trader_emotional_risk",
            "hedge_ai_drift",
            "hedge_budget_breach_total",
        ] {
            assert!(names.contains(required), "missing metric `{}`", required);
        }
    }

    /// Histograms expose the requested bucket boundaries in nanoseconds.
    #[test]
    fn latency_histograms_use_documented_buckets() {
        let m = init_metrics().unwrap();
        m.tick_ingest_ns.observe(1_500.0);
        let families = registry().gather();
        let f = families
            .iter()
            .find(|f| f.get_name() == "hedge_tick_ingest_ns")
            .expect("tick_ingest_ns family present");
        let metric = f.get_metric().first().expect("at least one metric");
        let h = metric.get_histogram();
        let upper_bounds: Vec<f64> = h.get_bucket().iter().map(|b| b.get_upper_bound()).collect();
        assert_eq!(upper_bounds, LATENCY_BUCKETS_NS);
    }

    /// Labelled metrics accept the documented label names. We exercise each
    /// label set so a future name change shows up here as a compile or
    /// runtime error rather than as silent metric loss.
    #[test]
    fn labelled_metrics_accept_documented_labels() {
        let m = init_metrics().unwrap();
        m.broker_latency_ns
            .with_label_values(&["zerodha"])
            .observe(2_000.0);
        m.slippage_bps
            .with_label_values(&["RELIANCE"])
            .observe(3.0);
        m.websocket_drops_total.with_label_values(&["nse_l1"]).inc();
        m.risk_anomaly_total
            .with_label_values(&["volatility_block"])
            .inc();
        m.budget_breach_total
            .with_label_values(&["RiskCheck"])
            .inc();
        m.ai_drift.with_label_values(&["AI_Trade_Ranking"]).set(0.4);
        m.trader_emotional_risk.set(0.25);

        // Sanity: counter values increment.
        let v = m
            .websocket_drops_total
            .with_label_values(&["nse_l1"])
            .get();
        assert!(v >= 1.0, "expected >= 1, got {}", v);
    }

    #[test]
    fn registry_handle_is_stable_across_calls() {
        let r1 = registry();
        let r2 = registry();
        assert!(std::ptr::eq(r1 as *const _, r2 as *const _));
    }
}
