//! Typed [`FailureEvent`] and [`RecoveryAction`] enums.
//!
//! These are the two narrow waists of the supervisor:
//!
//! ```text
//!   ┌──────────────┐    FailureEvent    ┌────────────────┐    RecoveryAction
//!   │  Failure_    ├───────────────────▶│  Recovery_     ├──────────────────▶
//!   │  Detector    │                    │  Policy        │
//!   └──────────────┘                    └────────────────┘
//!         ▲                                                     │
//!         │ obs.error.*                                         ▼
//!         │ md.connection.*                              ┌────────────────┐
//!         │ broker.metric.*                              │  Recovery_     │
//!         │ obs.latency.*                                │  Actuator      │
//!         │ ai.ollama.degraded                           └────────────────┘
//!         │ cache.redis.*                                       │
//!                                                               ▼
//!                                                        ops.action.<target>
//! ```
//!
//! The classification is intentionally coarse: every supervisor input
//! collapses into one of five [`FailureKind`] variants, and every
//! supervisor output collapses into one of five [`RecoveryActionKind`]
//! variants (which then serialize to the standardised
//! `ops.action.<target>` payload defined in
//! `crates/hedge-schemas/json_schemas/ops_action.schema.json`).

use std::time::Duration;

use hedge_core::BrokerId;

// ---------------------------------------------------------------------------
// FailureEvent --------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Coarse classification of a failure observed by [`Failure_Detector`].
///
/// The variant set is fixed by design § Self-Healing Flow (R25.1–R25.5). Each
/// variant carries the minimum identifying parameter the Recovery_Policy
/// needs to pick a remediation:
///
/// | Variant                    | Identifies            | Source subjects                  |
/// |----------------------------|-----------------------|----------------------------------|
/// | `WsDisconnected`           | named upstream feed   | `md.connection.<source>`         |
/// | `BrokerErrorRateBreach`    | a [`BrokerId`]        | `broker.metric.<broker>`         |
/// | `RedisUnavailable`         | (no parameter)        | `cache.redis.*`, `obs.error.*`   |
/// | `ExternalApiLatencySpike`  | named external source | `obs.latency.*`, `obs.error.*`   |
/// | `OllamaUnresponsive`       | named model           | `ai.ollama.degraded`             |
///
/// [`Failure_Detector`]: crate::detector::FailureDetector
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FailureKind {
    /// A WebSocket / streaming feed disconnected. `source` is the
    /// canonical short name of the feed (e.g. `"nse_l1"`, `"bse_l2"`).
    /// Mirrors design § Components — Market_Data_Engine § Outputs:
    /// `md.connection.<source>` (R1.6).
    WsDisconnected {
        /// Identifier of the upstream feed.
        source: String,
    },

    /// A Broker_Adapter reported persistent error or latency above the
    /// configured threshold (R5.11, R6.5). Mirrors
    /// `broker.metric.<broker>` (R7.4).
    BrokerErrorRateBreach {
        /// Which broker breached the threshold.
        broker: BrokerId,
    },

    /// Redis became unreachable (R25.2). The cache is global — there is
    /// no per-shard parameter at the supervisor layer.
    RedisUnavailable,

    /// An external API (news feed, broker REST, regulator endpoint, …)
    /// exhibits latency above the configured threshold (R25.5). The
    /// `source` string identifies which external API; the policy table
    /// looks up the matching mitigation.
    ExternalApiLatencySpike {
        /// Identifier of the external dependency.
        source: String,
    },

    /// The Ollama_Infrastructure became unresponsive (R10.9). The
    /// `model` field carries the degraded model's canonical name so the
    /// policy can pick a fallback.
    OllamaUnresponsive {
        /// Name of the degraded model.
        model: String,
    },
}

impl FailureKind {
    /// Stable short tag used as a metric label and as the `code` field of
    /// the optional `obs.error.supervisor.<tag>` event the supervisor can
    /// itself emit for tracing.
    #[inline]
    pub fn tag(&self) -> &'static str {
        match self {
            Self::WsDisconnected { .. } => "ws_disconnected",
            Self::BrokerErrorRateBreach { .. } => "broker_error_rate_breach",
            Self::RedisUnavailable => "redis_unavailable",
            Self::ExternalApiLatencySpike { .. } => "external_api_latency_spike",
            Self::OllamaUnresponsive { .. } => "ollama_unresponsive",
        }
    }
}

/// A single observed failure together with the wall-clock time at which
/// the detector classified it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureEvent {
    /// What happened.
    pub kind: FailureKind,
    /// Monotonic timestamp at classification (`hedge_core::now_ns()`).
    pub ts_ns: u64,
}

impl FailureEvent {
    /// Construct a new event stamped with `hedge_core::now_ns()`.
    #[inline]
    pub fn new(kind: FailureKind) -> Self {
        Self { kind, ts_ns: hedge_core::now_ns() }
    }

    /// Construct a new event with an explicit timestamp. Useful in tests
    /// and replay where the wall-clock value must be deterministic.
    #[inline]
    pub fn with_ts(kind: FailureKind, ts_ns: u64) -> Self {
        Self { kind, ts_ns }
    }
}

// ---------------------------------------------------------------------------
// RecoveryAction ------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Coarse classification of the action the [`Recovery_Actuator`] will
/// publish on `ops.action.<target>`.
///
/// Mapping to the `ops.action.<target>` JSON Schema (`action` enum):
///
/// | Variant                | `target`                            | `action`     |
/// |------------------------|-------------------------------------|--------------|
/// | `Reconnect`            | `"market_data"` / per-source target | `reconnect`  |
/// | `BrokerFailover`       | `"execution_engine"`                | `failover`   |
/// | `RedisReconnect`       | `"warmcache"`                       | `reconnect`  |
/// | `ApplyMitigation`      | per-source target                   | `warn`       |
/// | `OllamaFallback`       | `"warm_ai"`                         | `warn`       |
///
/// [`Recovery_Actuator`]: crate::actuator::RecoveryActuator
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryActionKind {
    /// Reconnect the named upstream feed using exponential backoff
    /// `t_n = min(max_delay, base × 2^n)` (R25.1).
    Reconnect {
        /// Which source to reconnect (`"nse_l1"`, `"bse_l2"`, …).
        source: String,
        /// Computed delay for this attempt. The actuator sleeps for this
        /// duration before publishing the action so the receiver does
        /// not need its own backoff state.
        backoff: Duration,
        /// Attempt counter (0-indexed). Reset to 0 on a successful
        /// reconnect (signalled out-of-band by the upstream component).
        attempt: u32,
    },

    /// Atomic active+backup swap inside the Execution_Engine (R6.5,
    /// R25.3). Both broker ids are carried so the receiver can reject
    /// stale failover requests if its current state has changed.
    BrokerFailover {
        /// Failing broker.
        from: BrokerId,
        /// Backup broker.
        to: BrokerId,
    },

    /// Reconnect Redis and emit `cache.redis.degraded` on the bus
    /// (R25.2). The actuator emits the `cache.redis.degraded` event
    /// directly; the `RedisReconnect` action carries the trigger.
    RedisReconnect,

    /// Apply the configured per-component mitigation for an external
    /// API latency spike (R25.5). The `mitigation` string is a
    /// short human-readable label drawn from the supervisor's
    /// mitigation table (e.g. `"throttle_news_fetch"`).
    ApplyMitigation {
        /// Identifier of the external dependency.
        source: String,
        /// Mitigation label looked up from the policy table.
        mitigation: String,
    },

    /// Switch the Warm_AI_Pipeline to the configured fallback Ollama
    /// model (R10.9, R25.x). The actuator emits `ai.ollama.degraded`
    /// directly so downstream consumers (Risk_Engine, UI) immediately
    /// reduce their reliance on AI scores; the `OllamaFallback` action
    /// carries the model swap.
    OllamaFallback {
        /// Failing model.
        from: String,
        /// Fallback model the policy table selected.
        to: String,
    },
}

impl RecoveryActionKind {
    /// Stable canonical `target` segment for the `ops.action.<target>`
    /// subject. Matches the table in [`RecoveryActionKind`]'s docs.
    pub fn target(&self) -> &'static str {
        match self {
            Self::Reconnect { .. } => "market_data",
            Self::BrokerFailover { .. } => "execution_engine",
            Self::RedisReconnect => "warmcache",
            Self::ApplyMitigation { .. } => "warm_ai",
            Self::OllamaFallback { .. } => "warm_ai",
        }
    }

    /// Stable canonical `action` enum value for the JSON Schema. Matches
    /// the `action` enum in `crates/hedge-schemas/json_schemas/ops_action.schema.json`.
    pub fn action(&self) -> &'static str {
        match self {
            Self::Reconnect { .. } => "reconnect",
            Self::BrokerFailover { .. } => "failover",
            Self::RedisReconnect => "reconnect",
            // `warn` is the most accurate enum value for "apply this mitigation"
            // and "use the fallback model" — neither requires a hard restart.
            Self::ApplyMitigation { .. } => "warn",
            Self::OllamaFallback { .. } => "warn",
        }
    }

    /// Human-readable reason string passed in the JSON payload.
    pub fn reason(&self) -> String {
        match self {
            Self::Reconnect { source, attempt, backoff } => format!(
                "ws disconnect source={source} attempt={attempt} backoff_ms={}",
                backoff.as_millis()
            ),
            Self::BrokerFailover { from, to } => {
                format!("broker error/latency breach: failing over from {from:?} to {to:?}")
            }
            Self::RedisReconnect => "redis unavailable; reconnecting and emitting degraded state".into(),
            Self::ApplyMitigation { source, mitigation } => {
                format!("external api latency spike on {source}; applying mitigation `{mitigation}`")
            }
            Self::OllamaFallback { from, to } => {
                format!("ollama model `{from}` unresponsive; falling back to `{to}`")
            }
        }
    }
}

/// A [`RecoveryActionKind`] paired with the originating [`FailureEvent`]
/// timestamp. The actuator stamps its own emission `ts_ns` separately
/// so the latency between detection and actuation is observable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryAction {
    /// What to do.
    pub kind: RecoveryActionKind,
    /// Detection timestamp (carried through from the [`FailureEvent`]).
    pub detected_ts_ns: u64,
}

impl RecoveryAction {
    /// Construct from a kind + detection timestamp.
    #[inline]
    pub fn new(kind: RecoveryActionKind, detected_ts_ns: u64) -> Self {
        Self { kind, detected_ts_ns }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_kind_tag_is_stable() {
        assert_eq!(
            FailureKind::WsDisconnected { source: "nse".into() }.tag(),
            "ws_disconnected"
        );
        assert_eq!(
            FailureKind::BrokerErrorRateBreach { broker: BrokerId::Zerodha }.tag(),
            "broker_error_rate_breach"
        );
        assert_eq!(FailureKind::RedisUnavailable.tag(), "redis_unavailable");
        assert_eq!(
            FailureKind::ExternalApiLatencySpike { source: "news".into() }.tag(),
            "external_api_latency_spike"
        );
        assert_eq!(
            FailureKind::OllamaUnresponsive { model: "qwen2.5".into() }.tag(),
            "ollama_unresponsive"
        );
    }

    #[test]
    fn recovery_action_target_and_action_match_schema_enum() {
        // The supervisor's schema (ops_action.schema.json) declares:
        //   action ∈ {"restart","failover","drain","isolate","reconnect","warn"}
        let valid: &[&str] = &["restart", "failover", "drain", "isolate", "reconnect", "warn"];
        for k in [
            RecoveryActionKind::Reconnect {
                source: "nse_l1".into(),
                backoff: Duration::from_millis(100),
                attempt: 0,
            },
            RecoveryActionKind::BrokerFailover {
                from: BrokerId::Zerodha,
                to: BrokerId::Dhan,
            },
            RecoveryActionKind::RedisReconnect,
            RecoveryActionKind::ApplyMitigation {
                source: "news".into(),
                mitigation: "throttle".into(),
            },
            RecoveryActionKind::OllamaFallback {
                from: "qwen2.5:14b".into(),
                to: "mistral:7b".into(),
            },
        ] {
            assert!(
                valid.contains(&k.action()),
                "{k:?} produced action `{}` not in schema enum",
                k.action()
            );
            assert!(!k.target().is_empty());
        }
    }

    #[test]
    fn reason_strings_carry_identifying_data() {
        let r = RecoveryActionKind::Reconnect {
            source: "bse_l2".into(),
            backoff: Duration::from_millis(800),
            attempt: 3,
        };
        let reason = r.reason();
        assert!(reason.contains("bse_l2"), "reason missing source: {reason}");
        assert!(reason.contains("attempt=3"), "reason missing attempt: {reason}");
        assert!(reason.contains("800"), "reason missing backoff: {reason}");
    }

    #[test]
    fn failure_event_explicit_ts_round_trip() {
        let ev = FailureEvent::with_ts(FailureKind::RedisUnavailable, 12345);
        assert_eq!(ev.kind, FailureKind::RedisUnavailable);
        assert_eq!(ev.ts_ns, 12345);
    }
}
