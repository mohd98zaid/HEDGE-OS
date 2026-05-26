//! `hedge-supervisor` binary — Self_Healing_Supervisor process.
//!
//! Runs **outside** the Hot_Path so a Hot_Path crash never kills the
//! supervisor (R29.6, design § Self-Healing Flow). The binary's
//! responsibilities are deliberately narrow:
//!
//! 1. Load configuration (via `hedge-config`'s YAML loader).
//! 2. Load last-known-healthy state from
//!    `/var/lib/hedge/supervisor/state.json` (R29.6).
//! 3. Connect two NATS clients (one for the detector subscriptions,
//!    one for the actuator publishes — same broker, distinct
//!    sessions).
//! 4. Construct a [`hedge_supervisor::Supervisor`] and run its main
//!    pipeline. The detector → policy → actuator chain lives entirely
//!    inside the library; this binary is only the wiring.
//!
//! ### Environment variables
//!
//! | Variable               | Default                                     | Purpose                                  |
//! |------------------------|---------------------------------------------|------------------------------------------|
//! | `HEDGE_CONFIG_PATH`    | _unset_ ⇒ workspace defaults                | Path to YAML config                      |
//! | `HEDGE_NATS_URL`       | `nats://127.0.0.1:4222`                     | NATS endpoint                            |
//! | `HEDGE_NATS_CREDS`     | _unset_ ⇒ no credentials (dev only)         | Path to `*.creds`                        |
//! | `HEDGE_SUPERVISOR_STATE_PATH` | [`hedge_supervisor::DEFAULT_STATE_PATH`] | State-file location                |
//! | `RUST_LOG`             | `info`                                      | `tracing-subscriber` filter              |
//!
//! Container deployments (`docker-compose.yml::hedge-supervisor`) set
//! these via the `x-hedge-creds-supervisor` anchor. Systemd unit files
//! point to the same env keys.

use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use hedge_bus::NatsClient;
use hedge_config::{fail_closed, load_or_default, HedgeConfig};
use hedge_supervisor::{Supervisor, SupervisorStateStore, DEFAULT_STATE_PATH};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    install_tracing();

    // ---- 1. Configuration -------------------------------------------------
    let cfg_path = std::env::var("HEDGE_CONFIG_PATH").ok().map(PathBuf::from);
    let cfg: Arc<HedgeConfig> = match load_or_default(cfg_path.as_deref()) {
        Ok(c) => Arc::new(c),
        Err(e) => fail_closed(e),
    };

    // ---- 2. State store ---------------------------------------------------
    let state_path =
        std::env::var("HEDGE_SUPERVISOR_STATE_PATH").unwrap_or_else(|_| DEFAULT_STATE_PATH.into());
    let state = SupervisorStateStore::with_path(state_path);

    // ---- 3. NATS clients --------------------------------------------------
    let nats_url = std::env::var("HEDGE_NATS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let creds = std::env::var("HEDGE_NATS_CREDS")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);

    let actuator_nats = match connect_nats(&nats_url, creds.as_deref()).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, url = %nats_url, "supervisor: actuator nats connect failed");
            process::exit(2);
        }
    };
    let detector_nats = match connect_nats(&nats_url, creds.as_deref()).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, url = %nats_url, "supervisor: detector nats connect failed");
            process::exit(2);
        }
    };

    // ---- 4. Supervisor ----------------------------------------------------
    let supervisor = Supervisor::new(cfg, actuator_nats, state);

    tracing::info!(
        nats_url = %nats_url,
        creds_present = creds.is_some(),
        state_path = %supervisor.state().path().display(),
        "supervisor: bring-up complete; entering run loop",
    );

    if let Err(e) = supervisor.bring_up_and_run(detector_nats).await {
        tracing::error!(error = %e, "supervisor: run loop terminated with error");
        process::exit(1);
    }
}

async fn connect_nats(
    url: &str,
    creds: Option<&std::path::Path>,
) -> Result<NatsClient, hedge_bus::BusError> {
    match creds {
        Some(p) => NatsClient::connect_with_creds(url, p).await,
        None => NatsClient::connect(url).await,
    }
}

fn install_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .json()
        .try_init();
}
