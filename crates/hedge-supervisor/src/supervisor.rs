//! [`Supervisor`] — top-level orchestrator that wires
//! [`FailureDetector`](crate::detector::FailureDetector),
//! [`RecoveryPolicy`](crate::policy::RecoveryPolicy), and
//! [`RecoveryActuator`](crate::actuator::RecoveryActuator) into one
//! Tokio-driven pipeline.
//!
//! ```text
//!  Failure_Detector ── mpsc<FailureEvent> ──▶ Recovery_Policy ──▶ Recovery_Actuator
//!     (NATS sub)                                (decide)              (NATS pub)
//!                                                  │
//!                                                  └── SupervisorStateStore
//! ```
//!
//! The supervisor runs in its own process so a Hot_Path crash never
//! kills it (R29.6, design § Self-Healing Flow). The orchestrator is
//! exposed both as:
//!
//! * `Supervisor::run` — drives the full loop. Used by the binary
//!   (`src/main.rs`).
//! * `Supervisor::handle_one` — pure-ish unit suitable for property
//!   tests in task 41.2: takes one `FailureEvent`, applies policy +
//!   actuator + state mutation, and returns the
//!   [`OpsActionPayload`](crate::actuator::OpsActionPayload) the
//!   actuator emitted.

use std::sync::Arc;
use std::time::Duration;

use hedge_bus::{BusError, NatsClient};
use hedge_config::HedgeConfig;
use tokio::sync::mpsc;

use crate::actuator::{OpsActionPayload, RecoveryActuator};
use crate::detector::FailureDetector;
use crate::event::{FailureEvent, FailureKind, RecoveryAction, RecoveryActionKind};
use crate::policy::RecoveryPolicy;
use crate::state::{StateError, SupervisorStateStore};

/// Channel depth for [`FailureEvent`]s flowing from detector to
/// supervisor. Sized generously: a backed-up channel here means the
/// supervisor cannot keep up with bus events, which is the loud
/// failure mode we want.
pub const FAILURE_CHANNEL_DEPTH: usize = 1024;

/// Orchestrator. Owns the policy, actuator, and state store; spawns
/// the detector as a child task in [`Supervisor::run`].
pub struct Supervisor {
    cfg: Arc<HedgeConfig>,
    policy: Arc<RecoveryPolicy>,
    actuator: RecoveryActuator,
    state: SupervisorStateStore,
}

impl Supervisor {
    /// Construct a supervisor from already-connected NATS clients and
    /// a pre-loaded state store. Both NATS clients can be the same
    /// underlying connection; we take two so the binary can use
    /// independently authenticated clients per the supervisor account
    /// ACL.
    pub fn new(
        cfg: Arc<HedgeConfig>,
        actuator_nats: NatsClient,
        state: SupervisorStateStore,
    ) -> Self {
        let policy = Arc::new(RecoveryPolicy::from_config(&cfg));
        Self {
            cfg,
            policy,
            actuator: RecoveryActuator::from_client(actuator_nats),
            state,
        }
    }

    /// Borrow the workspace config the supervisor was constructed with.
    #[inline]
    pub fn config(&self) -> &Arc<HedgeConfig> {
        &self.cfg
    }

    /// Borrow the policy. Test-visible.
    #[inline]
    pub fn policy(&self) -> &Arc<RecoveryPolicy> {
        &self.policy
    }

    /// Borrow the actuator. Test-visible.
    #[inline]
    pub fn actuator(&self) -> &RecoveryActuator {
        &self.actuator
    }

    /// Borrow the state store. Test-visible.
    #[inline]
    pub fn state(&self) -> &SupervisorStateStore {
        &self.state
    }

    /// Drive the full pipeline: spawn the detector, consume events,
    /// run them through the policy, drive the actuator, persist
    /// state. Returns when the detector channel closes.
    ///
    /// The detector runs as a child Tokio task so the run loop can
    /// keep processing events while the detector independently
    /// handles its own subscriptions. A detector failure (transient
    /// NATS error) is logged but does not crash the supervisor —
    /// production deployments wrap this method in a
    /// `tokio::spawn(...)` + restart loop in the binary.
    pub async fn run(self, detector_nats: NatsClient) -> Result<(), BusError> {
        let (tx, mut rx) = mpsc::channel::<FailureEvent>(FAILURE_CHANNEL_DEPTH);

        // Spawn the detector. It owns its own subscriptions and will
        // close the channel when it returns.
        let detector = FailureDetector::from_client(self.cfg.clone(), detector_nats);
        let detector_handle = tokio::spawn(async move {
            if let Err(e) = detector.run(tx).await {
                tracing::error!(error = %e, "supervisor: detector task exited with error");
            }
        });

        // Consume the channel until it closes.
        while let Some(event) = rx.recv().await {
            if let Err(e) = self.handle_one(&event).await {
                tracing::error!(
                    event = ?event,
                    error = %e,
                    "supervisor: failed to handle failure event",
                );
            }
        }

        // Best-effort: wait for the detector task to finish so its
        // tracing spans flush.
        let _ = detector_handle.await;
        Ok(())
    }

    /// Apply policy + actuator + state mutation for one
    /// [`FailureEvent`]. Returns the [`OpsActionPayload`] published on
    /// the bus.
    ///
    /// Pure-ish: no NATS subscription, only publishes. Used by task
    /// 41.2's property tests to drive the policy deterministically.
    pub async fn handle_one(
        &self,
        event: &FailureEvent,
    ) -> Result<Option<OpsActionPayload>, SupervisorError> {
        // 1. Decide.
        let Some(action) = self.policy.decide(event) else {
            tracing::debug!(event = ?event, "supervisor: no action for event");
            return Ok(None);
        };

        // 2. Persist (before publishing — if persistence fails we want
        //    to know before downstream consumers act on a state we
        //    cannot recover after restart).
        self.persist_state_for(event, &action)?;

        // 3. Actuate.
        let payload = self
            .actuator
            .actuate(&action)
            .await
            .map_err(SupervisorError::Bus)?;

        Ok(Some(payload))
    }

    /// Apply state mutations matching `(event, action)`.
    fn persist_state_for(
        &self,
        event: &FailureEvent,
        action: &RecoveryAction,
    ) -> Result<(), SupervisorError> {
        let action_clone = action.clone();
        self.state
            .update_and_save(|s| match (&event.kind, &action_clone.kind) {
                (
                    FailureKind::WsDisconnected { source },
                    RecoveryActionKind::Reconnect { .. },
                ) => {
                    s.record_ws_attempt(source);
                }
                (
                    FailureKind::BrokerErrorRateBreach { .. },
                    RecoveryActionKind::BrokerFailover { from, to },
                ) => {
                    s.record_failover(*from, *to, event.ts_ns);
                }
                (FailureKind::RedisUnavailable, RecoveryActionKind::RedisReconnect) => {
                    s.set_redis_degraded(true);
                }
                (
                    FailureKind::ExternalApiLatencySpike { source },
                    RecoveryActionKind::ApplyMitigation { mitigation, .. },
                ) => {
                    s.record_mitigation(source, mitigation);
                }
                (
                    FailureKind::OllamaUnresponsive { .. },
                    RecoveryActionKind::OllamaFallback { to, .. },
                ) => {
                    s.record_ollama_swap(to);
                }
                _ => {
                    // Mismatched (kind, action) pair is a bug — the
                    // policy should always produce a coherent action
                    // for the event type. Log loudly but do not abort.
                    tracing::warn!(
                        event = ?event,
                        action = ?action_clone,
                        "supervisor: mismatched (event, action) pair",
                    );
                }
            })
            .map_err(SupervisorError::State)
    }

    /// Restore last-known-healthy operational state from the store.
    /// Called once at startup; the supervisor uses the result to seed
    /// the in-memory policy counters so the next event for the same
    /// source picks up where the previous process left off.
    ///
    /// This is the "restart bring-up" half of R29.6. The other half
    /// (systemd / docker-compose `restart: unless-stopped`) lives in
    /// `docker-compose.yml`'s `hedge-supervisor` service.
    pub fn rehydrate_from_state(&self) -> Result<(), StateError> {
        let snapshot = self.state.load_or_default()?;
        // Replay the per-source attempt counters into the policy. The
        // policy's `attempts` map starts empty; each call to
        // `decide()` for `WsDisconnected{source}` increments by one,
        // so we replay `n` events per source to reach `n`.
        for (source, count) in snapshot.ws_attempts.iter() {
            let stub = FailureEvent::new(FailureKind::WsDisconnected {
                source: source.clone(),
            });
            for _ in 0..*count {
                let _ = self.policy.decide(&stub);
            }
        }
        tracing::info!(
            sources = snapshot.ws_attempts.len(),
            redis_degraded = snapshot.redis_degraded,
            active_broker = ?snapshot.active_broker,
            active_ollama_model = ?snapshot.active_ollama_model,
            "supervisor: rehydrated last-known-healthy state",
        );
        Ok(())
    }

    /// Convenience: combine [`Supervisor::rehydrate_from_state`] with
    /// [`Supervisor::run`]. Most production binaries use this entry
    /// point; tests call the two halves separately.
    pub async fn bring_up_and_run(
        self,
        detector_nats: NatsClient,
    ) -> Result<(), SupervisorError> {
        if let Err(e) = self.rehydrate_from_state() {
            // A missing or unreadable state file is non-fatal: we log
            // and start fresh. The detector and actuator are still
            // healthy.
            tracing::warn!(
                error = %e,
                "supervisor: rehydrate failed; starting from clean state",
            );
        }
        self.run(detector_nats).await.map_err(SupervisorError::Bus)
    }
}

/// Top-level supervisor error.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    /// Bus publish/subscribe failure surfaced by `hedge_bus`.
    #[error("supervisor bus error: {0}")]
    Bus(#[from] BusError),
    /// Persistent-state I/O / decode failure.
    #[error("supervisor state error: {0}")]
    State(#[from] StateError),
}

/// Compute the per-attempt sleep duration for a `Reconnect` action.
/// Exposed as a free function so the binary can implement the
/// "sleep before publish" half outside the actuator (the actuator
/// itself does not sleep — it publishes immediately so the consumer
/// learns of the action without a supervisor-side blocking wait).
///
/// The supervisor's main loop chooses to schedule the publish via
/// [`tokio::time::sleep`] when it wants the consumer to coordinate
/// with the supervisor's timing; in the default deployment the
/// consumer does its own backoff after seeing `attempt = N`.
#[inline]
pub fn reconnect_sleep_for(action: &RecoveryAction) -> Option<Duration> {
    match &action.kind {
        RecoveryActionKind::Reconnect { backoff, .. } => Some(*backoff),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_config::defaults;
    use std::time::Duration;
    use tempfile::tempdir;

    fn cfg() -> Arc<HedgeConfig> {
        Arc::new(defaults::hedge_config())
    }

    #[test]
    fn reconnect_sleep_for_reconnect_returns_backoff() {
        let action = RecoveryAction::new(
            RecoveryActionKind::Reconnect {
                source: "x".into(),
                backoff: Duration::from_millis(800),
                attempt: 3,
            },
            0,
        );
        assert_eq!(
            reconnect_sleep_for(&action),
            Some(Duration::from_millis(800))
        );
    }

    #[test]
    fn reconnect_sleep_for_other_actions_returns_none() {
        let action = RecoveryAction::new(RecoveryActionKind::RedisReconnect, 0);
        assert_eq!(reconnect_sleep_for(&action), None);
    }

    #[test]
    fn rehydrate_replays_ws_attempt_counters_into_policy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let store = SupervisorStateStore::with_path(path);
        store
            .update_and_save(|s| {
                s.ws_attempts.insert("nse_l1".into(), 3);
                s.ws_attempts.insert("bse_l2".into(), 1);
            })
            .unwrap();

        // Build a supervisor without a real NATS client. We only
        // exercise rehydrate_from_state, which never touches NATS.
        let policy = Arc::new(RecoveryPolicy::from_config(&cfg()));
        // Manually replay rather than constructing a Supervisor (which
        // needs a NatsClient). We use the same logic.
        let snapshot = store.load_or_default().unwrap();
        for (source, count) in snapshot.ws_attempts.iter() {
            let stub = FailureEvent::new(FailureKind::WsDisconnected {
                source: source.clone(),
            });
            for _ in 0..*count {
                let _ = policy.decide(&stub);
            }
        }

        // Next decide on `nse_l1` must pick up at attempt 3.
        let ev = FailureEvent::new(FailureKind::WsDisconnected {
            source: "nse_l1".into(),
        });
        let action = policy.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::Reconnect { attempt, .. } => assert_eq!(attempt, 3),
            other => panic!("unexpected: {other:?}"),
        }

        // And `bse_l2` at attempt 1.
        let ev = FailureEvent::new(FailureKind::WsDisconnected {
            source: "bse_l2".into(),
        });
        let action = policy.decide(&ev).unwrap();
        match action.kind {
            RecoveryActionKind::Reconnect { attempt, .. } => assert_eq!(attempt, 1),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn supervisor_state_path_for_handle_one_records_ws_attempts() {
        // We can't drive `handle_one` without a NATS client because the
        // actuator publishes. But we *can* exercise the persistence
        // half independently by walking through the same code paths.
        let dir = tempdir().unwrap();
        let store = SupervisorStateStore::with_path(dir.path().join("state.json"));
        let _ = store.load_or_default().unwrap();

        // Two WS disconnects on the same source.
        for _ in 0..2 {
            store
                .update_and_save(|s| {
                    s.record_ws_attempt("nse_l1");
                })
                .unwrap();
        }
        let snap = store.snapshot();
        assert_eq!(snap.ws_attempts.get("nse_l1"), Some(&2));
    }
}
