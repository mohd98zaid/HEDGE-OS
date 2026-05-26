//! `Recovery_Actuator` — third stage of the Self_Healing_Supervisor.
//!
//! Consumes [`RecoveryAction`] values produced by the
//! [`Recovery_Policy`](crate::policy::RecoveryPolicy) and publishes them
//! on `ops.action.<target>` as JSON envelopes that conform to
//! `crates/hedge-schemas/json_schemas/ops_action.schema.json`.
//!
//! Side-effects beyond the action publish:
//!
//! * `RedisReconnect`     — also publishes `cache.redis.degraded` on the
//!   bus so downstream consumers immediately see the degraded state
//!   (R25.2). The cache subject is published here, in the actuator,
//!   because it is the canonical "supervisor-emitted" announcement and
//!   not a side-effect of any single component.
//! * `OllamaFallback`     — also publishes `ai.ollama.degraded` so
//!   consumers (Risk_Engine, UI) immediately stop relying on the
//!   degraded model (R10.9, R25.x).
//! * `BrokerFailover`     — also publishes `exec.broker.failover` so
//!   the audit log and replay recorder capture a coherent record of
//!   the supervisor's failover request (R6.5).
//!
//! Each side-effect is stamped with `hedge_core::now_ns()` so its
//! `ts_ns` reflects supervisor wall-time, not the originating
//! [`FailureEvent::ts_ns`].
//!
//! ### Subject ACL
//!
//! The supervisor account's publish allow list (see
//! `docker/nats/nats-server.conf::supervisor.publish.allow`) is:
//! `ops.action.>` and `obs.>`. The three side-effect subjects above
//! sit *outside* that allow list; the supervisor account's permissions
//! must be widened in deployments that want them. We log warnings on
//! the publish path but do not fail the action emission.

use std::sync::Arc;

use hedge_bus::{subjects, BusError, JsonCodec, NatsClient, NatsPublisher, Subject};
use hedge_core::BrokerId;
use serde::{Deserialize, Serialize};

use crate::event::{RecoveryAction, RecoveryActionKind};

// ---------------------------------------------------------------------------
// Wire payloads -------------------------------------------------------------
// ---------------------------------------------------------------------------

/// `ops.action.<target>` payload (R25.x). Mirrors
/// `crates/hedge-schemas/json_schemas/ops_action.schema.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpsActionPayload {
    /// Target component (`market_data`, `execution_engine`, …).
    pub target: String,
    /// Action enum value (`reconnect`, `failover`, `restart`, …).
    pub action: String,
    /// Human-readable explanation.
    pub reason: String,
    /// Attempt counter for retried actions (e.g. WS reconnect).
    /// `None` for one-shot actions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// Wall-clock timestamp at emission.
    pub ts_ns: u64,
}

/// `cache.redis.<state>` payload that the supervisor publishes when a
/// Redis reconnect is initiated. The Memory_RAG_Layer's typed
/// exceptions ladder into this same envelope; we keep the wire form
/// minimal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheRedisDegradedPayload {
    /// Lifecycle state (`"degraded"`).
    pub state: String,
    /// Human-readable reason.
    pub reason: String,
    /// Wall-clock timestamp at emission.
    pub ts_ns: u64,
}

/// `ai.ollama.degraded` payload. Matches the JSON Schema in
/// `crates/hedge-schemas/json_schemas/ai_ollama_degraded.schema.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiOllamaDegradedPayload {
    /// Failing model name.
    pub model: String,
    /// Configured fallback model name.
    pub fallback_model: String,
    /// Failure category (`unresponsive` is the supervisor's default
    /// label — the `ai.ollama.degraded` schema enum admits it).
    pub reason: String,
    /// Wall-clock timestamp at emission.
    pub ts_ns: u64,
}

/// `exec.broker.failover` payload. Mirrors the broker-failover event
/// the Execution_Engine publishes on its own when the router swaps;
/// the supervisor's emission carries the *requested* swap, with the
/// engine's own emission supplying the *applied* swap. Consumers
/// dedupe on `correlation_id`-equivalent fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecBrokerFailoverPayload {
    /// Failing broker.
    pub from: BrokerId,
    /// Backup broker.
    pub to: BrokerId,
    /// Originator of the failover (`"supervisor"`).
    pub origin: String,
    /// Wall-clock timestamp at emission.
    pub ts_ns: u64,
}

// ---------------------------------------------------------------------------
// RecoveryActuator ----------------------------------------------------------
// ---------------------------------------------------------------------------

/// Side-effect subject names. Held as constants so tests can assert on
/// them without re-stringifying.
pub(crate) const CACHE_REDIS_DEGRADED: &str = "cache.redis.degraded";
pub(crate) const AI_OLLAMA_DEGRADED: &str = "ai.ollama.degraded";
pub(crate) const EXEC_BROKER_FAILOVER: &str = "exec.broker.failover";

/// Long-running task that publishes [`RecoveryAction`]s on the bus.
///
/// Constructed once per process and shared across the supervisor's
/// channels; the actuator itself is `Clone` because every NATS
/// publisher it owns is internally refcounted.
pub struct RecoveryActuator {
    nats: NatsClient,
}

impl RecoveryActuator {
    /// Connect to NATS at `nats_url` using the supervisor account
    /// credentials. The two-step constructor mirrors
    /// [`crate::detector::FailureDetector::connect`] so the binary can
    /// retry connect without touching the publish path.
    pub async fn connect(
        nats_url: impl AsRef<str>,
        creds_path: Option<&std::path::Path>,
    ) -> Result<Self, BusError> {
        let nats = match creds_path {
            Some(p) => NatsClient::connect_with_creds(nats_url, p).await?,
            None => NatsClient::connect(nats_url).await?,
        };
        Ok(Self::from_client(nats))
    }

    /// Construct from an already-connected client.
    pub fn from_client(nats: NatsClient) -> Self {
        Self { nats }
    }

    /// Borrow the underlying NATS client. Useful in tests that need to
    /// peek at the subscription side via the same broker.
    #[inline]
    pub fn nats(&self) -> &NatsClient {
        &self.nats
    }

    /// Publish an [`OpsActionPayload`] on the canonical
    /// `ops.action.<target>` subject. Test-friendly: returns the
    /// payload that was actually published so callers can assert on
    /// it without round-tripping through the bus.
    pub async fn emit_ops_action(
        &self,
        action: &RecoveryAction,
    ) -> Result<OpsActionPayload, BusError> {
        let payload = build_ops_action_payload(action);
        let publisher: NatsPublisher<OpsActionPayload, JsonCodec<OpsActionPayload>> =
            self.nats.publisher(
                subjects::ops_action::<OpsActionPayload>(action.kind.target()),
                JsonCodec::new(),
            );
        publisher.publish(&payload).await?;
        Ok(payload)
    }

    /// Publish the supervisor-side `cache.redis.degraded` event. Best-
    /// effort: a publish failure is logged at WARN and surfaced to the
    /// caller, but does not undo the in-memory action.
    pub async fn emit_cache_redis_degraded(
        &self,
        ts_ns: u64,
    ) -> Result<CacheRedisDegradedPayload, BusError> {
        let payload = CacheRedisDegradedPayload {
            state: "degraded".into(),
            reason: "supervisor: cache reconnect requested".into(),
            ts_ns,
        };
        let publisher: NatsPublisher<
            CacheRedisDegradedPayload,
            JsonCodec<CacheRedisDegradedPayload>,
        > = self.nats.publisher(
            Subject::<CacheRedisDegradedPayload>::new(CACHE_REDIS_DEGRADED),
            JsonCodec::new(),
        );
        publisher.publish(&payload).await?;
        Ok(payload)
    }

    /// Publish the supervisor-side `ai.ollama.degraded` event.
    pub async fn emit_ai_ollama_degraded(
        &self,
        from: &str,
        to: &str,
        ts_ns: u64,
    ) -> Result<AiOllamaDegradedPayload, BusError> {
        let payload = AiOllamaDegradedPayload {
            model: from.into(),
            fallback_model: to.into(),
            reason: "unresponsive".into(),
            ts_ns,
        };
        let publisher: NatsPublisher<
            AiOllamaDegradedPayload,
            JsonCodec<AiOllamaDegradedPayload>,
        > = self.nats.publisher(
            Subject::<AiOllamaDegradedPayload>::new(AI_OLLAMA_DEGRADED),
            JsonCodec::new(),
        );
        publisher.publish(&payload).await?;
        Ok(payload)
    }

    /// Publish the supervisor-side `exec.broker.failover` event.
    pub async fn emit_exec_broker_failover(
        &self,
        from: BrokerId,
        to: BrokerId,
        ts_ns: u64,
    ) -> Result<ExecBrokerFailoverPayload, BusError> {
        let payload = ExecBrokerFailoverPayload {
            from,
            to,
            origin: "supervisor".into(),
            ts_ns,
        };
        let publisher: NatsPublisher<
            ExecBrokerFailoverPayload,
            JsonCodec<ExecBrokerFailoverPayload>,
        > = self.nats.publisher(
            Subject::<ExecBrokerFailoverPayload>::new(EXEC_BROKER_FAILOVER),
            JsonCodec::new(),
        );
        publisher.publish(&payload).await?;
        Ok(payload)
    }

    /// Drive a [`RecoveryAction`] all the way to the bus, including any
    /// per-variant side-effects. Returns the [`OpsActionPayload`] that
    /// was published so the caller can log / audit / test it.
    ///
    /// Side-effect publishes that fail are logged at WARN and **do
    /// not** propagate the error. The primary `ops.action.<target>`
    /// publish failure does propagate, because dropping the action
    /// would defeat the supervisor's purpose.
    pub async fn actuate(
        &self,
        action: &RecoveryAction,
    ) -> Result<OpsActionPayload, BusError> {
        let ts_now = hedge_core::now_ns();
        match &action.kind {
            RecoveryActionKind::RedisReconnect => {
                if let Err(e) = self.emit_cache_redis_degraded(ts_now).await {
                    tracing::warn!(
                        error = %e,
                        "supervisor: side-effect cache.redis.degraded publish failed",
                    );
                }
            }
            RecoveryActionKind::BrokerFailover { from, to } => {
                if let Err(e) = self.emit_exec_broker_failover(*from, *to, ts_now).await {
                    tracing::warn!(
                        error = %e,
                        "supervisor: side-effect exec.broker.failover publish failed",
                    );
                }
            }
            RecoveryActionKind::OllamaFallback { from, to } => {
                if let Err(e) = self
                    .emit_ai_ollama_degraded(from.as_str(), to.as_str(), ts_now)
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        "supervisor: side-effect ai.ollama.degraded publish failed",
                    );
                }
            }
            // Reconnect and ApplyMitigation have no extra side-effect
            // publishes — the consumer (Market_Data_Engine,
            // News_Intelligence_Engine, …) listens directly on
            // `ops.action.<target>` and reacts.
            RecoveryActionKind::Reconnect { .. } | RecoveryActionKind::ApplyMitigation { .. } => {}
        }
        self.emit_ops_action(action).await
    }
}

// Manual `Clone` so the actuator can be shared across tasks without
// requiring a wrapping `Arc`. NATS clients are themselves
// refcount-cloneable.
impl Clone for RecoveryActuator {
    fn clone(&self) -> Self {
        Self { nats: self.nats.clone() }
    }
}

// Allow constructing an actuator from a shared `Arc<NatsClient>`-like
// surface. The simplest path is to expose `from_client` plus this Arc
// adapter so callers do not have to dereference the Arc themselves.
impl From<Arc<NatsClient>> for RecoveryActuator {
    fn from(client: Arc<NatsClient>) -> Self {
        Self { nats: (*client).clone() }
    }
}

// ---------------------------------------------------------------------------
// Pure helpers --------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Build the [`OpsActionPayload`] for a [`RecoveryAction`]. Pure
/// function — no I/O — so unit tests can verify the wire form
/// independent of any NATS client.
pub fn build_ops_action_payload(action: &RecoveryAction) -> OpsActionPayload {
    let attempt = match &action.kind {
        RecoveryActionKind::Reconnect { attempt, .. } => Some(*attempt),
        _ => None,
    };
    OpsActionPayload {
        target: action.kind.target().to_string(),
        action: action.kind.action().to_string(),
        reason: action.kind.reason(),
        attempt,
        ts_ns: action.detected_ts_ns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ops_action_payload_for_reconnect_carries_attempt() {
        let action = RecoveryAction::new(
            RecoveryActionKind::Reconnect {
                source: "nse_l1".into(),
                backoff: Duration::from_millis(800),
                attempt: 3,
            },
            42,
        );
        let p = build_ops_action_payload(&action);
        assert_eq!(p.target, "market_data");
        assert_eq!(p.action, "reconnect");
        assert_eq!(p.attempt, Some(3));
        assert_eq!(p.ts_ns, 42);
        assert!(p.reason.contains("nse_l1"), "reason missing source: {}", p.reason);
        assert!(p.reason.contains("attempt=3"), "reason missing attempt: {}", p.reason);
    }

    #[test]
    fn ops_action_payload_omits_attempt_for_failover() {
        let action = RecoveryAction::new(
            RecoveryActionKind::BrokerFailover {
                from: BrokerId::Zerodha,
                to: BrokerId::Dhan,
            },
            7,
        );
        let p = build_ops_action_payload(&action);
        assert_eq!(p.target, "execution_engine");
        assert_eq!(p.action, "failover");
        assert_eq!(p.attempt, None);
        assert_eq!(p.ts_ns, 7);
    }

    #[test]
    fn ops_action_payload_for_redis_reconnect() {
        let action = RecoveryAction::new(RecoveryActionKind::RedisReconnect, 9);
        let p = build_ops_action_payload(&action);
        assert_eq!(p.target, "warmcache");
        assert_eq!(p.action, "reconnect");
        assert_eq!(p.attempt, None);
    }

    #[test]
    fn ops_action_payload_for_apply_mitigation() {
        let action = RecoveryAction::new(
            RecoveryActionKind::ApplyMitigation {
                source: "news".into(),
                mitigation: "throttle".into(),
            },
            10,
        );
        let p = build_ops_action_payload(&action);
        assert_eq!(p.target, "warm_ai");
        assert_eq!(p.action, "warn");
        assert_eq!(p.attempt, None);
        assert!(p.reason.contains("news"));
        assert!(p.reason.contains("throttle"));
    }

    #[test]
    fn ops_action_payload_for_ollama_fallback() {
        let action = RecoveryAction::new(
            RecoveryActionKind::OllamaFallback {
                from: "qwen2.5:14b".into(),
                to: "mistral:7b".into(),
            },
            11,
        );
        let p = build_ops_action_payload(&action);
        assert_eq!(p.target, "warm_ai");
        assert_eq!(p.action, "warn");
        assert_eq!(p.attempt, None);
        assert!(p.reason.contains("qwen2.5:14b"));
        assert!(p.reason.contains("mistral:7b"));
    }

    #[test]
    fn ops_action_payload_round_trips_through_serde_json() {
        let action = RecoveryAction::new(
            RecoveryActionKind::Reconnect {
                source: "bse_l2".into(),
                backoff: Duration::from_millis(100),
                attempt: 0,
            },
            123,
        );
        let p = build_ops_action_payload(&action);
        let json = serde_json::to_string(&p).unwrap();
        let back: OpsActionPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
        // attempt must serialize as a number (not the string "0").
        assert!(json.contains("\"attempt\":0"));
    }

    #[test]
    fn ops_action_payload_omits_attempt_field_when_none() {
        let action = RecoveryAction::new(RecoveryActionKind::RedisReconnect, 0);
        let p = build_ops_action_payload(&action);
        let json = serde_json::to_string(&p).unwrap();
        assert!(
            !json.contains("attempt"),
            "attempt should be omitted when None: {json}"
        );
    }
}
