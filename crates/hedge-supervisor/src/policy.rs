//! Declarative [`RecoveryPolicy`] table.
//!
//! Maps every [`FailureKind`] variant to the matching
//! [`RecoveryActionKind`] using the rules from design § Self-Healing
//! Flow:
//!
//! | FailureKind                     | RecoveryAction                               | Reference |
//! |---------------------------------|----------------------------------------------|-----------|
//! | `WsDisconnected{source}`        | `Reconnect{source, backoff, attempt}`        | R25.1     |
//! | `BrokerErrorRateBreach{broker}` | `BrokerFailover{from, to}`                   | R25.3     |
//! | `RedisUnavailable`              | `RedisReconnect`                             | R25.2     |
//! | `ExternalApiLatencySpike{src}`  | `ApplyMitigation{source, mitigation}`        | R25.5     |
//! | `OllamaUnresponsive{model}`     | `OllamaFallback{from, to}`                   | R10.9     |
//!
//! ### Backoff
//!
//! For `WsDisconnected` the policy returns
//! `t_n = min(max_delay, base × 2^n)` (R25.1, design § Self-Healing
//! Flow). The attempt counter `n` is tracked **per source** inside the
//! policy itself: multiple disconnects in a row use increasing delays
//! and a successful reconnect (signalled by [`RecoveryPolicy::reset_backoff`])
//! returns the counter to 0.
//!
//! ### Mitigation table
//!
//! `ApplyMitigation` looks up a per-source mitigation label. The label
//! is a short string that downstream consumers interpret (e.g. the
//! News_Intelligence_Engine reads `"throttle_news_fetch"` and reduces
//! its polling cadence). The supervisor itself does not know the
//! semantics of any individual mitigation.
//!
//! ### Ollama fallback table
//!
//! `OllamaFallback` looks up the configured fallback for the failing
//! model. The policy seeds a sensible default chain
//! (primary → fast → lightweight) from the [`hedge_config::OllamaConfig`]
//! at construction time so that the supervisor's `from`/`to` decisions
//! are deterministic and offline-testable.

use std::collections::HashMap;
use std::time::Duration;

use hedge_config::{HedgeConfig, OllamaConfig, OllamaRole};
use parking_lot::Mutex;

use crate::event::{FailureEvent, FailureKind, RecoveryAction, RecoveryActionKind};

// ---------------------------------------------------------------------------
// Backoff -------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Exponential-backoff parameters for WebSocket reconnection (R25.1).
///
/// `t_n = min(max_delay, base × 2^n)`. The attempt counter `n` is
/// 0-indexed: `t_0 = base`, `t_1 = 2·base`, …, capped at `max_delay`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct BackoffParams {
    /// Base delay (= `t_0`).
    pub base: Duration,
    /// Upper bound on any single delay.
    pub max_delay: Duration,
}

impl BackoffParams {
    /// Reasonable defaults for the design's typical broker / WS feeds:
    /// 100 ms base, 30 s ceiling. Matches the values used by
    /// `crates/hedge-broker-zerodha`'s reconnect loop.
    pub const DEFAULT: Self = Self {
        base: Duration::from_millis(100),
        max_delay: Duration::from_secs(30),
    };

    /// Compute `t_n` for the given attempt.
    ///
    /// Saturating math: `2^n` is computed as `u32::MAX` when `n >= 32`
    /// so the formula never panics; the `min(max_delay, ...)` then
    /// clamps the result to the configured ceiling.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        // Compute base × 2^n in nanoseconds, saturating at u128::MAX so
        // overflow is impossible. We then convert back to Duration via
        // `Duration::from_nanos(u64)` after a saturating downcast.
        let base_ns: u128 = self.base.as_nanos();
        let factor: u128 = 1u128
            .checked_shl(attempt)
            .unwrap_or(u128::MAX);
        let computed_ns: u128 = base_ns.saturating_mul(factor);
        let max_ns: u128 = self.max_delay.as_nanos();
        let clamped_ns: u128 = computed_ns.min(max_ns);
        // Saturating downcast — we know clamped_ns ≤ max_ns ≤ u64::MAX
        // for any sensible `max_delay`, but be defensive.
        Duration::from_nanos(clamped_ns.min(u64::MAX as u128) as u64)
    }
}

impl Default for BackoffParams {
    fn default() -> Self {
        Self::DEFAULT
    }
}

// ---------------------------------------------------------------------------
// Policy --------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Declarative recovery-policy table.
///
/// Stateful across `WsDisconnected` events: each source's attempt
/// counter is tracked inside [`RecoveryPolicy`] so the next
/// `decide()` for that source returns the next backoff slot.
/// `reset_backoff(source)` clears the counter on a successful
/// reconnect.
///
/// All other variants are stateless: the same input produces the same
/// output every time. This keeps the property test in task 41.2
/// (`proptest`-driven) trivially deterministic.
pub struct RecoveryPolicy {
    backoff: BackoffParams,
    primary_broker: hedge_core::BrokerId,
    backup_broker: hedge_core::BrokerId,
    mitigations: HashMap<String, String>,
    ollama_fallbacks: HashMap<String, String>,
    attempts: Mutex<HashMap<String, u32>>,
}

impl RecoveryPolicy {
    /// Construct a policy from the workspace [`HedgeConfig`]. Pulls the
    /// primary/backup broker pair from `cfg.brokers` and seeds a default
    /// Ollama fallback chain from `cfg.ollama` (primary → fast →
    /// lightweight, with `lightweight` falling back to itself as a last
    /// resort so the chain always terminates).
    pub fn from_config(cfg: &HedgeConfig) -> Self {
        Self {
            backoff: BackoffParams::DEFAULT,
            primary_broker: cfg.brokers.primary,
            backup_broker: cfg.brokers.backup,
            mitigations: default_mitigation_table(),
            ollama_fallbacks: build_ollama_fallback_table(&cfg.ollama),
            attempts: Mutex::new(HashMap::new()),
        }
    }

    /// Override the default backoff parameters.
    pub fn with_backoff(mut self, backoff: BackoffParams) -> Self {
        self.backoff = backoff;
        self
    }

    /// Insert / override one mitigation entry.
    pub fn with_mitigation(mut self, source: impl Into<String>, mitigation: impl Into<String>) -> Self {
        self.mitigations.insert(source.into(), mitigation.into());
        self
    }

    /// Insert / override one Ollama fallback entry.
    pub fn with_ollama_fallback(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.ollama_fallbacks.insert(from.into(), to.into());
        self
    }

    /// Read-only view of the configured backoff parameters.
    #[inline]
    pub fn backoff(&self) -> BackoffParams {
        self.backoff
    }

    /// Read-only view of the configured broker pair.
    #[inline]
    pub fn brokers(&self) -> (hedge_core::BrokerId, hedge_core::BrokerId) {
        (self.primary_broker, self.backup_broker)
    }

    /// Reset the per-source backoff counter for `source`. Called by
    /// the supervisor after observing a successful reconnect on the
    /// affected component (R25.1).
    pub fn reset_backoff(&self, source: &str) {
        self.attempts.lock().remove(source);
    }

    /// Current attempt counter for `source`, 0 when no failure has been
    /// observed yet. Test-only — production code routes through
    /// [`decide`](Self::decide).
    #[cfg(test)]
    pub(crate) fn attempt_count(&self, source: &str) -> u32 {
        self.attempts.lock().get(source).copied().unwrap_or(0)
    }

    /// Decide which [`RecoveryAction`] to take for the given event.
    ///
    /// Returns `None` when no rule fires — currently every variant is
    /// covered, but `Option<...>` future-proofs the API for variants
    /// the supervisor explicitly chooses to ignore (e.g. an
    /// `OllamaUnresponsive` event for a model not present in the
    /// fallback table — in that case the policy logs and skips).
    pub fn decide(&self, event: &FailureEvent) -> Option<RecoveryAction> {
        let kind = match &event.kind {
            // ---- WebSocket disconnect → reconnect with exponential backoff ----
            FailureKind::WsDisconnected { source } => {
                // Increment per-source attempt counter, then compute t_n.
                let attempt = {
                    let mut guard = self.attempts.lock();
                    let entry = guard.entry(source.clone()).or_insert(0);
                    let n = *entry;
                    *entry = n.saturating_add(1);
                    n
                };
                let backoff = self.backoff.delay_for(attempt);
                RecoveryActionKind::Reconnect {
                    source: source.clone(),
                    backoff,
                    attempt,
                }
            }

            // ---- Broker error-rate / latency breach → failover ----
            FailureKind::BrokerErrorRateBreach { broker } => {
                // The router already swaps automatically on its own
                // sliding window (see hedge_exec::router); the
                // supervisor publishes the action regardless so any
                // downstream consumer (UI, audit log, replay
                // recorder) sees a consistent record. We compute the
                // (from, to) pair from the failing broker:
                //   - if it equals the configured primary, we go to
                //     backup;
                //   - otherwise we go back to primary;
                //   - and if the failing broker is neither, we still
                //     emit a failover from `broker` → backup as a
                //     safe default.
                let (from, to) = if *broker == self.primary_broker {
                    (self.primary_broker, self.backup_broker)
                } else if *broker == self.backup_broker {
                    (self.backup_broker, self.primary_broker)
                } else {
                    (*broker, self.backup_broker)
                };
                RecoveryActionKind::BrokerFailover { from, to }
            }

            // ---- Redis unavailable → reconnect + degraded event ----
            FailureKind::RedisUnavailable => RecoveryActionKind::RedisReconnect,

            // ---- External API latency spike → per-source mitigation ----
            FailureKind::ExternalApiLatencySpike { source } => {
                let mitigation = self
                    .mitigations
                    .get(source)
                    .cloned()
                    .unwrap_or_else(|| DEFAULT_MITIGATION.to_string());
                RecoveryActionKind::ApplyMitigation {
                    source: source.clone(),
                    mitigation,
                }
            }

            // ---- Ollama unresponsive → fallback model ----
            FailureKind::OllamaUnresponsive { model } => {
                let to = self
                    .ollama_fallbacks
                    .get(model)
                    .cloned()
                    .unwrap_or_else(|| model.clone());
                RecoveryActionKind::OllamaFallback {
                    from: model.clone(),
                    to,
                }
            }
        };

        Some(RecoveryAction::new(kind, event.ts_ns))
    }
}

/// Default mitigation when a source has no entry in the table. Chosen
/// conservatively: `"warn"` is the most permissive consumer-facing
/// label that still tells the consumer to back off.
const DEFAULT_MITIGATION: &str = "warn";

/// Default mitigation table. Keys are external-API source names; values
/// are short labels consumed by the corresponding component.
///
/// The set is small and additive; downstream tasks can extend it via
/// `with_mitigation`. Initial entries cover the dependencies named in
/// the design's data-flow diagrams (news, regulator, exchange clock).
fn default_mitigation_table() -> HashMap<String, String> {
    let mut t = HashMap::new();
    t.insert("news_provider".into(), "throttle_news_fetch".into());
    t.insert("regulator".into(), "skip_optional_compliance_calls".into());
    t.insert("clock".into(), "use_local_monotonic".into());
    t.insert("ollama".into(), "warn".into());
    t
}

/// Build a deterministic Ollama fallback chain from the configured
/// model registry. The order of preference is:
///
/// `Primary → Fast → Lightweight → Lightweight (self-loop)`
///
/// `Deep` falls back to `Primary` (it is a reasoning-focused model and
/// `Primary` is the closest in capability). All entries terminate so
/// the policy can never recurse infinitely.
fn build_ollama_fallback_table(cfg: &OllamaConfig) -> HashMap<String, String> {
    fn pick(cfg: &OllamaConfig, role: OllamaRole) -> Option<&str> {
        cfg.models.iter().find(|m| m.role == role).map(|m| m.name.as_str())
    }
    let primary = pick(cfg, OllamaRole::Primary);
    let fast = pick(cfg, OllamaRole::Fast);
    let lightweight = pick(cfg, OllamaRole::Lightweight);
    let deep = pick(cfg, OllamaRole::Deep);

    let mut table = HashMap::new();
    if let (Some(p), Some(f)) = (primary, fast) {
        table.insert(p.to_string(), f.to_string());
    }
    if let (Some(f), Some(l)) = (fast, lightweight) {
        table.insert(f.to_string(), l.to_string());
    }
    if let Some(l) = lightweight {
        // Self-loop terminates the chain. A consumer that gets back the
        // same model is expected to surface a hard error rather than
        // continue retrying.
        table.insert(l.to_string(), l.to_string());
    }
    if let (Some(d), Some(p)) = (deep, primary) {
        table.insert(d.to_string(), p.to_string());
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_config::defaults;
    use hedge_core::BrokerId;

    fn cfg() -> HedgeConfig {
        defaults::hedge_config()
    }

    #[test]
    fn backoff_formula_matches_spec() {
        // t_n ≤ min(max_delay, base × 2^n). Verified pointwise.
        let p = BackoffParams {
            base: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        };
        assert_eq!(p.delay_for(0), Duration::from_millis(100));
        assert_eq!(p.delay_for(1), Duration::from_millis(200));
        assert_eq!(p.delay_for(2), Duration::from_millis(400));
        assert_eq!(p.delay_for(3), Duration::from_millis(800));
        assert_eq!(p.delay_for(4), Duration::from_millis(1_600));
        // Eventually clamps to max_delay.
        assert_eq!(p.delay_for(20), Duration::from_secs(30));
        assert_eq!(p.delay_for(64), Duration::from_secs(30));
        // Never panics on extreme inputs.
        assert_eq!(p.delay_for(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn ws_disconnect_increments_attempt_per_source() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(
            FailureKind::WsDisconnected { source: "nse_l1".into() },
            1,
        );

        let a0 = p.decide(&ev).unwrap();
        let a1 = p.decide(&ev).unwrap();
        let a2 = p.decide(&ev).unwrap();

        // Verify attempts go 0, 1, 2 with strictly increasing backoffs.
        match (&a0.kind, &a1.kind, &a2.kind) {
            (
                RecoveryActionKind::Reconnect { attempt: a, backoff: b0, .. },
                RecoveryActionKind::Reconnect { attempt: b, backoff: b1, .. },
                RecoveryActionKind::Reconnect { attempt: c, backoff: b2, .. },
            ) => {
                assert_eq!((*a, *b, *c), (0, 1, 2));
                assert!(b0 < b1, "{b0:?} < {b1:?}");
                assert!(b1 < b2, "{b1:?} < {b2:?}");
            }
            other => panic!("unexpected actions: {other:?}"),
        }
    }

    #[test]
    fn ws_disconnect_per_source_counters_are_independent() {
        let p = RecoveryPolicy::from_config(&cfg());
        let nse = FailureEvent::with_ts(
            FailureKind::WsDisconnected { source: "nse_l1".into() },
            1,
        );
        let bse = FailureEvent::with_ts(
            FailureKind::WsDisconnected { source: "bse_l2".into() },
            2,
        );

        let _ = p.decide(&nse).unwrap();
        let _ = p.decide(&nse).unwrap();
        let bse0 = p.decide(&bse).unwrap();
        match bse0.kind {
            RecoveryActionKind::Reconnect { attempt, .. } => assert_eq!(attempt, 0),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn ws_disconnect_reset_returns_counter_to_zero() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(
            FailureKind::WsDisconnected { source: "nse_l1".into() },
            1,
        );
        let _ = p.decide(&ev).unwrap();
        let _ = p.decide(&ev).unwrap();
        assert_eq!(p.attempt_count("nse_l1"), 2);
        p.reset_backoff("nse_l1");
        assert_eq!(p.attempt_count("nse_l1"), 0);

        let next = p.decide(&ev).unwrap();
        match next.kind {
            RecoveryActionKind::Reconnect { attempt, .. } => assert_eq!(attempt, 0),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn broker_breach_returns_failover_to_backup_when_primary_fails() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(
            FailureKind::BrokerErrorRateBreach { broker: BrokerId::Zerodha },
            10,
        );
        let action = p.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::BrokerFailover { from, to } => {
                assert_eq!(from, BrokerId::Zerodha);
                assert_eq!(to, BrokerId::Dhan);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn broker_breach_returns_failover_to_primary_when_backup_fails() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(
            FailureKind::BrokerErrorRateBreach { broker: BrokerId::Dhan },
            10,
        );
        let action = p.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::BrokerFailover { from, to } => {
                assert_eq!(from, BrokerId::Dhan);
                assert_eq!(to, BrokerId::Zerodha);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn redis_unavailable_returns_redis_reconnect() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(FailureKind::RedisUnavailable, 1);
        let action = p.decide(&ev).unwrap();
        assert_eq!(action.kind, RecoveryActionKind::RedisReconnect);
    }

    #[test]
    fn external_api_latency_spike_uses_configured_mitigation() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(
            FailureKind::ExternalApiLatencySpike { source: "news_provider".into() },
            1,
        );
        let action = p.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::ApplyMitigation { source, mitigation } => {
                assert_eq!(source, "news_provider");
                assert_eq!(mitigation, "throttle_news_fetch");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn external_api_latency_spike_unknown_source_falls_back_to_default() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(
            FailureKind::ExternalApiLatencySpike { source: "mystery_api".into() },
            1,
        );
        let action = p.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::ApplyMitigation { source, mitigation } => {
                assert_eq!(source, "mystery_api");
                assert_eq!(mitigation, DEFAULT_MITIGATION);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn ollama_fallback_uses_chain_from_config() {
        let p = RecoveryPolicy::from_config(&cfg());
        // Default OllamaConfig uses qwen2.5:14b (primary) → mistral:7b (fast).
        let ev = FailureEvent::with_ts(
            FailureKind::OllamaUnresponsive { model: "qwen2.5:14b".into() },
            1,
        );
        let action = p.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::OllamaFallback { from, to } => {
                assert_eq!(from, "qwen2.5:14b");
                assert_eq!(to, "mistral:7b");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn ollama_fallback_unknown_model_self_loops() {
        let p = RecoveryPolicy::from_config(&cfg());
        let ev = FailureEvent::with_ts(
            FailureKind::OllamaUnresponsive { model: "unknown:1b".into() },
            1,
        );
        let action = p.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::OllamaFallback { from, to } => {
                assert_eq!(from, "unknown:1b");
                assert_eq!(to, "unknown:1b");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn with_overrides_take_precedence() {
        let p = RecoveryPolicy::from_config(&cfg())
            .with_mitigation("custom_api", "drain")
            .with_ollama_fallback("alpha", "omega")
            .with_backoff(BackoffParams {
                base: Duration::from_millis(50),
                max_delay: Duration::from_secs(1),
            });

        let ev = FailureEvent::with_ts(
            FailureKind::ExternalApiLatencySpike { source: "custom_api".into() },
            1,
        );
        match p.decide(&ev).unwrap().kind {
            RecoveryActionKind::ApplyMitigation { mitigation, .. } => {
                assert_eq!(mitigation, "drain");
            }
            other => panic!("unexpected: {other:?}"),
        }
        let ev = FailureEvent::with_ts(
            FailureKind::OllamaUnresponsive { model: "alpha".into() },
            1,
        );
        match p.decide(&ev).unwrap().kind {
            RecoveryActionKind::OllamaFallback { to, .. } => assert_eq!(to, "omega"),
            other => panic!("unexpected: {other:?}"),
        }
        let ev = FailureEvent::with_ts(
            FailureKind::WsDisconnected { source: "x".into() },
            1,
        );
        match p.decide(&ev).unwrap().kind {
            RecoveryActionKind::Reconnect { backoff, .. } => {
                assert_eq!(backoff, Duration::from_millis(50));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
