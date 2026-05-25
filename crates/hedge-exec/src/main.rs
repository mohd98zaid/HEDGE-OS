//! `hedge-exec` — Execution_Engine binary entry point (task 15.1).
//!
//! At runtime this binary:
//!
//! 1. Initialises a JSON `tracing-subscriber` (full `hedge-obs` wiring
//!    is centralised in the session manager, task 43.1).
//! 2. Loads `hedge_config::HedgeConfig` from `/etc/hedge/config.yaml`
//!    (falling back to defaults when standalone).
//! 3. Constructs a [`BrokerRouter`] from `config.brokers.primary` /
//!    `config.brokers.backup`. Until the per-broker crates expose
//!    their concrete adapters, both slots are bound to a placeholder
//!    [`PlaceholderAdapter`] that returns
//!    [`BrokerError::NotReady`] on every operation (R7.5 fail-closed
//!    posture).
//! 4. Wires the [`ExecutionEngine`] with the router, an
//!    [`ApprovalVerifier`] (placeholder until task 43.1 wires the
//!    shared key), the default retry policy, and the configured
//!    replay mode.
//! 5. Will spawn three tokio tasks once upstream producers are online:
//!    * **approvals consumer** — reads the `hedge.hot.approvals` Redis
//!      Stream consumer-group `execution_engine` and calls
//!      [`ExecutionEngine::submit`] for each entry.
//!    * **broker fill subscriber** — translates broker-side fills into
//!      [`ExecutionEngine::on_fill`] calls.
//!    * **fills producer** — drains [`EngineEvent::Fill`] events into
//!      the `hedge.hot.fills` Redis Stream.
//!
//! Today the binary boots the engine and idles.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hedge_broker_api::{
    BrokerAdapter, BrokerError, BrokerMetric, OrderIntent, OrderModification, OrderStatus,
    ReadyState, SubmitAck,
};
use hedge_config::{defaults, HedgeConfig};
use hedge_core::BrokerId;
use hedge_exec::{
    BrokerRouter, ExecutionEngine, FailoverThresholds, ReplayMode, RetryPolicy,
};
use hedge_obs::init_metrics;
use hedge_risk::ApprovalVerifier;
use tracing::{info, warn};

const SERVICE_NAME: &str = "hedge-exec";

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Bare-bones JSON logging.
    tracing_subscriber::fmt::fmt()
        .json()
        .with_target(true)
        .try_init()
        .ok();

    let _ = init_metrics()?;

    // 2. Configuration.
    let config: HedgeConfig = defaults::hedge_config();

    let replay_mode = if std::env::var("HEDGE_REPLAY_MODE")
        .ok()
        .map(|s| s == "on" || s == "1")
        .unwrap_or(false)
    {
        ReplayMode::On
    } else {
        ReplayMode::Off
    };

    info!(
        target: SERVICE_NAME,
        primary = ?config.brokers.primary,
        backup = ?config.brokers.backup,
        replay = ?replay_mode,
        "Execution_Engine starting"
    );

    // 3. Router + adapters. Until the broker crates expose their
    //    concrete adapters, bind two placeholder NotReady adapters so
    //    submission fails closed (R7.5).
    let (primary, backup) = match replay_mode {
        ReplayMode::Off => (
            placeholder_adapter(config.brokers.primary, "live broker adapter not yet wired"),
            placeholder_adapter(config.brokers.backup, "live broker adapter not yet wired"),
        ),
        ReplayMode::On => (
            placeholder_adapter(BrokerId::Simulated, "simulated adapter not yet wired"),
            placeholder_adapter(BrokerId::Simulated, "simulated adapter not yet wired"),
        ),
    };
    let thresholds = FailoverThresholds::from_broker_config(
        config.brokers.failover_error_rate,
        config.brokers.failover_latency_ms,
    );
    let router = Arc::new(BrokerRouter::new(primary, backup, thresholds));

    // 4. Engine. Until task 43.1 wires the shared HMAC key the
    //    verifier here is a placeholder — every approval token will
    //    therefore fail verification, which is the intended
    //    fail-closed posture for an unprovisioned binary.
    let placeholder_key = b"placeholder-exec-verifier-key!!!".to_vec();
    let verifier = ApprovalVerifier::from_key(placeholder_key);

    let engine = Arc::new(ExecutionEngine::new(
        Arc::clone(&router),
        verifier,
        RetryPolicy::default(),
        replay_mode,
    ));

    // 5. Subscriber wiring is deferred to task 43.1.
    warn!(
        target: SERVICE_NAME,
        "wire integration (Redis approvals consumer, broker fill subscriber, fills producer) deferred"
    );

    tokio::signal::ctrl_c().await.ok();
    info!(target: SERVICE_NAME, "Execution_Engine shutting down");
    let _ = engine; // suppress unused-variable lint until subscribers wire it.
    Ok(())
}

/// Construct a placeholder adapter that fails closed on every
/// operation.
fn placeholder_adapter(id: BrokerId, reason: &'static str) -> Arc<dyn BrokerAdapter> {
    Arc::new(PlaceholderAdapter { id, reason })
}

struct PlaceholderAdapter {
    id: BrokerId,
    reason: &'static str,
}

#[async_trait]
impl BrokerAdapter for PlaceholderAdapter {
    fn broker_id(&self) -> BrokerId {
        self.id
    }
    async fn submit(&self, _intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
        Err(BrokerError::NotReady(self.reason.to_string()))
    }
    async fn modify(&self, _m: &OrderModification) -> Result<(), BrokerError> {
        Err(BrokerError::NotReady(self.reason.to_string()))
    }
    async fn cancel(&self, _id: &str) -> Result<(), BrokerError> {
        Err(BrokerError::NotReady(self.reason.to_string()))
    }
    async fn status(&self, _id: &str) -> Result<OrderStatus, BrokerError> {
        Err(BrokerError::NotReady(self.reason.to_string()))
    }
    async fn metrics(&self) -> Vec<BrokerMetric> {
        Vec::new()
    }
    async fn ready(&self) -> ReadyState {
        ReadyState::ConfigError(self.reason.to_string())
    }
}
