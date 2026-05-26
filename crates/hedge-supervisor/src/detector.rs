//! `Failure_Detector` — first stage of the Self_Healing_Supervisor.
//!
//! Subscribes to the failure-detection event surface mandated by
//! design § Self-Healing Flow and the supervisor account ACL in
//! `docker/nats/nats-server.conf`:
//!
//! ```text
//! md.connection.>     ConnectionStatus events from the Market_Data_Engine
//! cache.redis.>       degraded/reconnect events from the Memory_RAG_Layer
//! broker.metric.>     latency/error metrics from every Broker_Adapter
//! obs.latency.>       per-stage latency records (LatencyRecord_v1)
//! obs.error.>         typed error events from any Hot_Path stage
//! ai.ollama.degraded  Ollama_Infrastructure degraded-state event
//! ```
//!
//! Each inbound NATS message is decoded as `serde_json` into a small
//! per-subject mirror struct, classified into a [`FailureKind`], and
//! emitted on a Tokio MPSC channel as a [`FailureEvent`]. The
//! [`Recovery_Policy`](crate::policy::RecoveryPolicy) consumes that
//! channel.
//!
//! ### Hot_Path discipline
//!
//! The detector runs **off** the per-tick path — it lives in its own
//! process (`hedge-supervisor`) so that a Hot_Path crash never kills
//! the supervisor (R29.6). The detector never publishes; that is the
//! [`Recovery_Actuator`](crate::actuator::RecoveryActuator)'s job.
//!
//! ### Loop shape
//!
//! `run` uses one `tokio::select!` over all six subscriptions. There
//! is **no `tokio::time::interval` polling**: every awaited future is a
//! long-lived NATS subscription stream. The pattern mirrors
//! `crates/hedge-warmcache/src/updater.rs`.

use std::sync::Arc;

use bytes::Bytes;
use hedge_bus::{BusError, FlatBuffersCodec, NatsClient, RawBytes, Subject};
use hedge_config::HedgeConfig;
use hedge_core::BrokerId;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::event::{FailureEvent, FailureKind};

// ---------------------------------------------------------------------------
// Subscription wildcards ----------------------------------------------------
// ---------------------------------------------------------------------------

/// Wildcard for `md.connection.<source>` (R1.6).
const SUB_MD_CONNECTION: &str = "md.connection.>";
/// Wildcard for `cache.redis.<state>` (R25.2).
const SUB_CACHE_REDIS: &str = "cache.redis.>";
/// Wildcard for `broker.metric.<broker>` (R7.4).
const SUB_BROKER_METRIC: &str = "broker.metric.>";
/// Wildcard for `obs.latency.<stage>` (R27.4).
const SUB_OBS_LATENCY: &str = "obs.latency.>";
/// Wildcard for `obs.error.<source>` (R27.4).
const SUB_OBS_ERROR: &str = "obs.error.>";
/// Single subject `ai.ollama.degraded` (R10.9).
const SUB_AI_OLLAMA_DEGRADED: &str = "ai.ollama.degraded";

// ---------------------------------------------------------------------------
// Wire-payload mirrors ------------------------------------------------------
// ---------------------------------------------------------------------------
//
// We mirror only the fields the detector actually reads. The
// `#[serde(default)]` and absence of `#[serde(deny_unknown_fields)]`
// keep the detector forward-compatible with producers that add fields.

/// Mirror of `md.connection.<source>` payload published by
/// `crates/hedge-market-data/src/adapter.rs::ConnectionEvent`.
#[derive(Debug, Clone, Deserialize)]
struct MdConnectionEvent {
    source: String,
    /// `"disconnected"` or `"reconnected"`. Snake-case matches the
    /// producer's `#[serde(rename_all = "snake_case")]`.
    status: String,
    #[serde(default)]
    #[allow(dead_code)]
    attempt: u32,
}

/// Mirror of `cache.redis.<state>` payload. The Memory_RAG_Layer (R25.2)
/// publishes a JSON envelope with a `state` field that takes the values
/// `"degraded"` or `"healthy"`.
#[derive(Debug, Clone, Deserialize)]
struct CacheRedisEvent {
    /// `"degraded"`, `"unavailable"`, or `"healthy"`.
    state: String,
}

/// Mirror of `broker.metric.<broker>` payload published by
/// `crates/hedge-broker-api::BrokerMetric`. We only need the broker id,
/// the error flag, and the latency.
#[derive(Debug, Clone, Deserialize)]
struct BrokerMetricEvent {
    broker_id: BrokerId,
    error: bool,
    #[serde(default)]
    latency_ns: u64,
    #[serde(default)]
    #[allow(dead_code)]
    ts_ns: u64,
}

/// Mirror of `obs.latency.<stage>` payload (see
/// `crates/hedge-schemas/json_schemas/obs_latency.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct ObsLatencyEvent {
    stage: String,
    nanos: u64,
    #[serde(default)]
    breach: bool,
}

/// Mirror of `obs.error.<source>` payload (see
/// `crates/hedge-schemas/json_schemas/obs_error.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct ObsErrorEvent {
    source: String,
    #[serde(default)]
    severity: String,
    #[serde(default)]
    #[allow(dead_code)]
    code: String,
    #[serde(default)]
    #[allow(dead_code)]
    message: String,
}

/// Mirror of `ai.ollama.degraded` payload (see
/// `crates/hedge-schemas/json_schemas/ai_ollama_degraded.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct AiOllamaDegradedEvent {
    model: String,
    #[serde(default)]
    #[allow(dead_code)]
    fallback_model: String,
    #[serde(default)]
    #[allow(dead_code)]
    reason: String,
}

// ---------------------------------------------------------------------------
// Thresholds ----------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Broker-failover thresholds derived from [`hedge_config::BrokerConfig`].
/// Each metric event is classified independently — a single `error: true`
/// or a latency above `failover_latency_ns` causes a
/// [`FailureKind::BrokerErrorRateBreach`] to fire.
///
/// The supervisor *intentionally* errs on the side of emitting one
/// signal per breach: the [`crate::policy::RecoveryPolicy`] is the
/// place where rate-limiting / debouncing lives, not the detector.
/// This keeps the detector trivially testable.
#[derive(Copy, Clone, Debug)]
pub struct BrokerFailoverThresholds {
    /// Latency above which a single metric event is considered a breach.
    pub failover_latency_ns: u64,
}

impl BrokerFailoverThresholds {
    /// Read the thresholds from the workspace [`HedgeConfig`].
    pub fn from_config(cfg: &HedgeConfig) -> Self {
        Self {
            failover_latency_ns: u64::from(cfg.brokers.failover_latency_ms) * 1_000_000,
        }
    }
}

/// Latency threshold above which an [`ObsLatencyEvent`] on a non-Hot_Path
/// stage is treated as an external-API spike (R25.5). We deliberately
/// reuse the broker-latency block setting from `RiskConfig` so the
/// supervisor and Risk_Engine agree on the wall above which a remote
/// dependency is degraded; the supervisor applies it broadly to *any*
/// `obs.latency.<stage>` event whose `breach` flag is true.
#[derive(Copy, Clone, Debug)]
pub struct LatencySpikeThresholds {
    /// `nanos` budget above which any latency event counts as a spike.
    /// Pulled from `risk.broker_latency_block_ms × 1e6` so a single
    /// number governs both the per-tick gate and the supervisor.
    pub spike_threshold_ns: u64,
}

impl LatencySpikeThresholds {
    /// Read the thresholds from the workspace [`HedgeConfig`].
    pub fn from_config(cfg: &HedgeConfig) -> Self {
        Self {
            spike_threshold_ns: u64::from(cfg.risk.broker_latency_block_ms) * 1_000_000,
        }
    }
}

// ---------------------------------------------------------------------------
// FailureDetector -----------------------------------------------------------
// ---------------------------------------------------------------------------

/// Long-running task that drains the failure-detection event surface
/// into a typed [`FailureEvent`] channel.
///
/// Construct with [`FailureDetector::connect`] (or
/// [`FailureDetector::from_client`] in tests), then call
/// [`FailureDetector::run`] to start the loop.
pub struct FailureDetector {
    nats: NatsClient,
    broker_thresholds: BrokerFailoverThresholds,
    latency_thresholds: LatencySpikeThresholds,
}

impl FailureDetector {
    /// Connect to NATS at `nats_url` using the supervisor account
    /// credentials. The caller is responsible for placing the
    /// `*.creds` file at `creds_path`. Two-step connect is used so
    /// the binary can layer its own retry policy around connect
    /// without coupling it to the run loop.
    pub async fn connect(
        cfg: Arc<HedgeConfig>,
        nats_url: impl AsRef<str>,
        creds_path: Option<&std::path::Path>,
    ) -> Result<Self, BusError> {
        let nats = match creds_path {
            Some(p) => NatsClient::connect_with_creds(nats_url, p).await?,
            None => NatsClient::connect(nats_url).await?,
        };
        Ok(Self::from_client(cfg, nats))
    }

    /// Construct from an already-connected client.
    pub fn from_client(cfg: Arc<HedgeConfig>, nats: NatsClient) -> Self {
        Self {
            nats,
            broker_thresholds: BrokerFailoverThresholds::from_config(&cfg),
            latency_thresholds: LatencySpikeThresholds::from_config(&cfg),
        }
    }

    /// Read-only view of the broker thresholds. Test-visible.
    #[inline]
    pub fn broker_thresholds(&self) -> BrokerFailoverThresholds {
        self.broker_thresholds
    }

    /// Read-only view of the latency thresholds. Test-visible.
    #[inline]
    pub fn latency_thresholds(&self) -> LatencySpikeThresholds {
        self.latency_thresholds
    }

    /// Run the subscription loop. Sends every classified
    /// [`FailureEvent`] on `tx`. Returns `Ok(())` when the channel
    /// closes (receiver dropped) or any of the underlying
    /// subscriptions terminates; the supervisor's main loop is
    /// responsible for restarting on transient failures.
    pub async fn run(self, tx: mpsc::Sender<FailureEvent>) -> Result<(), BusError> {
        let nats = self.nats;
        let broker_th = self.broker_thresholds;
        let latency_th = self.latency_thresholds;

        let mut sub_md = nats
            .subscriber(Subject::<RawBytes>::new(SUB_MD_CONNECTION), FlatBuffersCodec)
            .await?;
        let mut sub_redis = nats
            .subscriber(Subject::<RawBytes>::new(SUB_CACHE_REDIS), FlatBuffersCodec)
            .await?;
        let mut sub_broker = nats
            .subscriber(Subject::<RawBytes>::new(SUB_BROKER_METRIC), FlatBuffersCodec)
            .await?;
        let mut sub_latency = nats
            .subscriber(Subject::<RawBytes>::new(SUB_OBS_LATENCY), FlatBuffersCodec)
            .await?;
        let mut sub_error = nats
            .subscriber(Subject::<RawBytes>::new(SUB_OBS_ERROR), FlatBuffersCodec)
            .await?;
        let mut sub_ollama = nats
            .subscriber(
                Subject::<RawBytes>::new(SUB_AI_OLLAMA_DEGRADED),
                FlatBuffersCodec,
            )
            .await?;

        loop {
            tokio::select! {
                msg = sub_md.recv_bytes() => match msg {
                    Ok(bytes) => {
                        if let Some(ev) = classify_md_connection(&bytes) {
                            if Self::send(&tx, ev).await.is_err() { return Ok(()); }
                        }
                    }
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => tracing::warn!(error = %other, "supervisor: md.connection.> recv failed"),
                },
                msg = sub_redis.recv_bytes() => match msg {
                    Ok(bytes) => {
                        if let Some(ev) = classify_cache_redis(&bytes) {
                            if Self::send(&tx, ev).await.is_err() { return Ok(()); }
                        }
                    }
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => tracing::warn!(error = %other, "supervisor: cache.redis.> recv failed"),
                },
                msg = sub_broker.recv_bytes() => match msg {
                    Ok(bytes) => {
                        if let Some(ev) = classify_broker_metric(&bytes, &broker_th) {
                            if Self::send(&tx, ev).await.is_err() { return Ok(()); }
                        }
                    }
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => tracing::warn!(error = %other, "supervisor: broker.metric.> recv failed"),
                },
                msg = sub_latency.recv_bytes() => match msg {
                    Ok(bytes) => {
                        if let Some(ev) = classify_obs_latency(&bytes, &latency_th) {
                            if Self::send(&tx, ev).await.is_err() { return Ok(()); }
                        }
                    }
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => tracing::warn!(error = %other, "supervisor: obs.latency.> recv failed"),
                },
                msg = sub_error.recv_bytes() => match msg {
                    Ok(bytes) => {
                        if let Some(ev) = classify_obs_error(&bytes) {
                            if Self::send(&tx, ev).await.is_err() { return Ok(()); }
                        }
                    }
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => tracing::warn!(error = %other, "supervisor: obs.error.> recv failed"),
                },
                msg = sub_ollama.recv_bytes() => match msg {
                    Ok(bytes) => {
                        if let Some(ev) = classify_ai_ollama_degraded(&bytes) {
                            if Self::send(&tx, ev).await.is_err() { return Ok(()); }
                        }
                    }
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => tracing::warn!(error = %other, "supervisor: ai.ollama.degraded recv failed"),
                },
            }
        }
    }

    /// Send a classified event on the channel. Returns `Err` once the
    /// receiver is gone — the caller treats that as a normal shutdown
    /// signal.
    #[inline]
    async fn send(
        tx: &mpsc::Sender<FailureEvent>,
        ev: FailureEvent,
    ) -> Result<(), mpsc::error::SendError<FailureEvent>> {
        tx.send(ev).await
    }
}

// ---------------------------------------------------------------------------
// Per-subject classifiers (pub(crate) so tests can drive them directly) ----
// ---------------------------------------------------------------------------

/// Classify a `md.connection.<source>` event. Returns `None` for a
/// `reconnected` status — the *successful* reconnect is signalled to
/// the policy layer through [`crate::policy::RecoveryPolicy::reset_backoff`]
/// out-of-band by the supervisor's main loop.
pub(crate) fn classify_md_connection(bytes: &Bytes) -> Option<FailureEvent> {
    match serde_json::from_slice::<MdConnectionEvent>(bytes) {
        Ok(ev) if ev.status == "disconnected" => {
            Some(FailureEvent::new(FailureKind::WsDisconnected { source: ev.source }))
        }
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(error = %e, "supervisor: md.connection decode failed");
            None
        }
    }
}

/// Classify a `cache.redis.<state>` event into a
/// [`FailureKind::RedisUnavailable`] when `state ∈ {"degraded","unavailable"}`.
/// Other states (`"healthy"`) are reset signals processed elsewhere.
pub(crate) fn classify_cache_redis(bytes: &Bytes) -> Option<FailureEvent> {
    match serde_json::from_slice::<CacheRedisEvent>(bytes) {
        Ok(ev) if ev.state == "degraded" || ev.state == "unavailable" => {
            Some(FailureEvent::new(FailureKind::RedisUnavailable))
        }
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(error = %e, "supervisor: cache.redis decode failed");
            None
        }
    }
}

/// Classify a `broker.metric.<broker>` event. Fires when the metric
/// reports an error or its latency exceeds the configured threshold.
pub(crate) fn classify_broker_metric(
    bytes: &Bytes,
    th: &BrokerFailoverThresholds,
) -> Option<FailureEvent> {
    match serde_json::from_slice::<BrokerMetricEvent>(bytes) {
        Ok(ev) => {
            if ev.error || ev.latency_ns > th.failover_latency_ns {
                Some(FailureEvent::new(FailureKind::BrokerErrorRateBreach {
                    broker: ev.broker_id,
                }))
            } else {
                None
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "supervisor: broker.metric decode failed");
            None
        }
    }
}

/// Classify an `obs.latency.<stage>` event. Treats any record with
/// `breach == true` (the canonical signal that the producer's own
/// budget gate fired) **or** a raw `nanos` above the supervisor's
/// `spike_threshold_ns` as an external-API latency spike. The stage
/// name is forwarded as the `source` so the policy table can pick a
/// per-stage mitigation.
pub(crate) fn classify_obs_latency(
    bytes: &Bytes,
    th: &LatencySpikeThresholds,
) -> Option<FailureEvent> {
    match serde_json::from_slice::<ObsLatencyEvent>(bytes) {
        Ok(ev) => {
            if ev.breach || ev.nanos > th.spike_threshold_ns {
                Some(FailureEvent::new(FailureKind::ExternalApiLatencySpike {
                    source: ev.stage,
                }))
            } else {
                None
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "supervisor: obs.latency decode failed");
            None
        }
    }
}

/// Classify an `obs.error.<source>` event. Many `obs.error.*` events
/// are routine (e.g. payload-overflow warnings) and not actionable;
/// the supervisor only reacts to:
///
/// * `severity ∈ {"error","critical"}` and `source` carrying `redis`
///   ⇒ Redis unavailable;
/// * `severity ∈ {"error","critical"}` and `source` carrying
///   `ollama` ⇒ Ollama unresponsive (with an unknown model name —
///   the policy table self-loops to the lightweight model);
/// * everything else ⇒ ignored (the originating subject already has a
///   typed channel: `cache.redis.*`, `ai.ollama.degraded`, etc.).
///
/// Routing this way keeps the detector's behaviour simple: we never
/// invent failure events the dedicated subjects haven't already
/// announced. We only forward `obs.error.*` for *aliased* sources.
pub(crate) fn classify_obs_error(bytes: &Bytes) -> Option<FailureEvent> {
    let ev = match serde_json::from_slice::<ObsErrorEvent>(bytes) {
        Ok(ev) => ev,
        Err(e) => {
            tracing::warn!(error = %e, "supervisor: obs.error decode failed");
            return None;
        }
    };
    if ev.severity != "error" && ev.severity != "critical" {
        return None;
    }
    let lc = ev.source.to_ascii_lowercase();
    if lc.contains("redis") {
        Some(FailureEvent::new(FailureKind::RedisUnavailable))
    } else if lc.contains("ollama") {
        Some(FailureEvent::new(FailureKind::OllamaUnresponsive {
            model: "unknown".into(),
        }))
    } else {
        None
    }
}

/// Classify an `ai.ollama.degraded` event.
pub(crate) fn classify_ai_ollama_degraded(bytes: &Bytes) -> Option<FailureEvent> {
    match serde_json::from_slice::<AiOllamaDegradedEvent>(bytes) {
        Ok(ev) => Some(FailureEvent::new(FailureKind::OllamaUnresponsive {
            model: ev.model,
        })),
        Err(e) => {
            tracing::warn!(error = %e, "supervisor: ai.ollama.degraded decode failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_config::defaults;

    fn cfg() -> Arc<HedgeConfig> {
        Arc::new(defaults::hedge_config())
    }

    #[test]
    fn md_connection_disconnected_emits_ws_disconnected() {
        let payload = br#"{"source":"nse_l1","status":"disconnected","reason":"reset","attempt":2,"at":"2024-01-01T00:00:00Z"}"#;
        let ev = classify_md_connection(&Bytes::from_static(payload)).unwrap();
        assert_eq!(
            ev.kind,
            FailureKind::WsDisconnected { source: "nse_l1".into() }
        );
    }

    #[test]
    fn md_connection_reconnected_is_ignored() {
        let payload = br#"{"source":"nse_l1","status":"reconnected","reason":null,"attempt":2,"at":"2024-01-01T00:00:00Z"}"#;
        assert!(classify_md_connection(&Bytes::from_static(payload)).is_none());
    }

    #[test]
    fn md_connection_decode_failure_returns_none() {
        assert!(classify_md_connection(&Bytes::from_static(b"{garbage")).is_none());
    }

    #[test]
    fn cache_redis_degraded_emits_redis_unavailable() {
        let payload = br#"{"state":"degraded","reason":"connection refused"}"#;
        let ev = classify_cache_redis(&Bytes::from_static(payload)).unwrap();
        assert_eq!(ev.kind, FailureKind::RedisUnavailable);
    }

    #[test]
    fn cache_redis_unavailable_emits_redis_unavailable() {
        let payload = br#"{"state":"unavailable"}"#;
        let ev = classify_cache_redis(&Bytes::from_static(payload)).unwrap();
        assert_eq!(ev.kind, FailureKind::RedisUnavailable);
    }

    #[test]
    fn cache_redis_healthy_is_ignored() {
        let payload = br#"{"state":"healthy"}"#;
        assert!(classify_cache_redis(&Bytes::from_static(payload)).is_none());
    }

    #[test]
    fn broker_metric_error_flag_emits_breach() {
        let th = BrokerFailoverThresholds::from_config(&cfg());
        let payload = br#"{"broker_id":"Zerodha","correlation_id":{"value":0},"op":"Submit","latency_ns":100,"error":true,"http_status":500,"ts_ns":1}"#;
        let ev = classify_broker_metric(&Bytes::from_static(payload), &th).unwrap();
        assert_eq!(
            ev.kind,
            FailureKind::BrokerErrorRateBreach { broker: BrokerId::Zerodha }
        );
    }

    #[test]
    fn broker_metric_latency_above_threshold_emits_breach() {
        let th = BrokerFailoverThresholds {
            failover_latency_ns: 1_000,
        };
        let payload = br#"{"broker_id":"Dhan","correlation_id":{"value":0},"op":"Submit","latency_ns":5000,"error":false,"ts_ns":1}"#;
        let ev = classify_broker_metric(&Bytes::from_static(payload), &th).unwrap();
        assert_eq!(
            ev.kind,
            FailureKind::BrokerErrorRateBreach { broker: BrokerId::Dhan }
        );
    }

    #[test]
    fn broker_metric_healthy_is_ignored() {
        let th = BrokerFailoverThresholds {
            failover_latency_ns: u64::MAX,
        };
        let payload = br#"{"broker_id":"Zerodha","correlation_id":{"value":0},"op":"Submit","latency_ns":100,"error":false,"ts_ns":1}"#;
        assert!(classify_broker_metric(&Bytes::from_static(payload), &th).is_none());
    }

    #[test]
    fn obs_latency_breach_flag_emits_external_spike() {
        let th = LatencySpikeThresholds {
            spike_threshold_ns: u64::MAX, // only `breach` field can fire
        };
        let payload = br#"{"correlation_id":"x","stage":"AiScoringFetch","nanos":1,"budget_nanos":10,"breach":true}"#;
        let ev = classify_obs_latency(&Bytes::from_static(payload), &th).unwrap();
        match ev.kind {
            FailureKind::ExternalApiLatencySpike { source } => {
                assert_eq!(source, "AiScoringFetch");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn obs_latency_above_threshold_emits_external_spike() {
        let th = LatencySpikeThresholds { spike_threshold_ns: 1_000 };
        let payload = br#"{"correlation_id":"x","stage":"BrokerSubmit","nanos":2000,"budget_nanos":10,"breach":false}"#;
        let ev = classify_obs_latency(&Bytes::from_static(payload), &th).unwrap();
        match ev.kind {
            FailureKind::ExternalApiLatencySpike { source } => {
                assert_eq!(source, "BrokerSubmit");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn obs_latency_under_threshold_is_ignored() {
        let th = LatencySpikeThresholds { spike_threshold_ns: u64::MAX };
        let payload = br#"{"correlation_id":"x","stage":"RiskCheck","nanos":1,"budget_nanos":10,"breach":false}"#;
        assert!(classify_obs_latency(&Bytes::from_static(payload), &th).is_none());
    }

    #[test]
    fn obs_error_redis_critical_emits_redis_unavailable() {
        let payload = br#"{"correlation_id":"x","source":"warmcache.redis","code":"connect","severity":"critical","message":"down","ts_ns":1}"#;
        let ev = classify_obs_error(&Bytes::from_static(payload)).unwrap();
        assert_eq!(ev.kind, FailureKind::RedisUnavailable);
    }

    #[test]
    fn obs_error_ollama_error_emits_ollama_unresponsive() {
        let payload = br#"{"correlation_id":"x","source":"ollama_service","code":"timeout","severity":"error","message":"down","ts_ns":1}"#;
        let ev = classify_obs_error(&Bytes::from_static(payload)).unwrap();
        match ev.kind {
            FailureKind::OllamaUnresponsive { model } => assert_eq!(model, "unknown"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn obs_error_low_severity_is_ignored() {
        let payload = br#"{"correlation_id":"x","source":"warmcache.redis","code":"slow","severity":"warn","message":"slow","ts_ns":1}"#;
        assert!(classify_obs_error(&Bytes::from_static(payload)).is_none());
    }

    #[test]
    fn obs_error_unrelated_source_is_ignored() {
        let payload = br#"{"correlation_id":"x","source":"feature_extract","code":"x","severity":"error","message":"y","ts_ns":1}"#;
        assert!(classify_obs_error(&Bytes::from_static(payload)).is_none());
    }

    #[test]
    fn ai_ollama_degraded_emits_with_model_name() {
        let payload = br#"{"model":"qwen2.5:14b","fallback_model":"mistral:7b","reason":"timeout","ts_ns":1}"#;
        let ev = classify_ai_ollama_degraded(&Bytes::from_static(payload)).unwrap();
        match ev.kind {
            FailureKind::OllamaUnresponsive { model } => assert_eq!(model, "qwen2.5:14b"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn broker_thresholds_compute_from_config() {
        let th = BrokerFailoverThresholds::from_config(&cfg());
        // default failover_latency_ms = 250 ⇒ 250 ms in ns.
        assert_eq!(th.failover_latency_ns, 250 * 1_000_000);
    }

    #[test]
    fn latency_thresholds_compute_from_config() {
        let th = LatencySpikeThresholds::from_config(&cfg());
        // default broker_latency_block_ms = 250 ⇒ 250 ms in ns.
        assert_eq!(th.spike_threshold_ns, 250 * 1_000_000);
    }
}
