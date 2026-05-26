//! `hedge-session` binary entry point.
//!
//! Spins up two cooperating IST-clock observers under a single tokio
//! runtime, both sharing one [`SystemWallClock`] and one
//! [`hedge_bus::NatsClient`]:
//!
//! * [`WarModeController`] — emits `ops.warmode.start` / `ops.warmode.end`
//!   over the `[09:15:00, 09:45:00]` IST window (task 42.1).
//! * [`SessionController`] — emits `ops.session.start` / `ops.session.end`
//!   over the `[09:15:00, 15:30:00]` IST window (task 43.1, R31.2,
//!   R31.3).
//!
//! Configuration is loaded once at startup via
//! [`hedge_config::load_default`] (which falls back to design defaults
//! when no YAML is mounted), so the binary can be smoke-tested in
//! development without `/etc/hedge/config.yaml`.
//!
//! ### Concurrency model
//!
//! Both controllers run as independent `tokio::spawn` tasks. Their
//! emission paths do not share mutable state — each owns its own
//! `Inactive`/`Active` enum and publishes through its own typed
//! publisher. The first controller whose `run` future resolves with an
//! error tears the process down via `tokio::select!`; the supervising
//! systemd unit (or the local `cargo run`) is responsible for restart.

use std::sync::Arc;

use anyhow::Context;
use hedge_bus::NatsClient;
use hedge_config::load_default;
use hedge_session::{
    NatsOpsEventPublisher, NatsSessionEventPublisher, SessionController, SystemWallClock,
    WarModeController,
};
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Minimal stderr tracing for development. Production wires
    // hedge-obs via the standard `ObsInit` flow when this binary is
    // started under systemd; full observability wiring lands with the
    // ObsHandle scaffolding (task 5.1) once it ships an in-process
    // initialiser shared by every Hot_Path binary.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .init();

    // `load_default()` returns the design-default `HedgeConfig`
    // directly (it cannot fail — it is in-memory). Production
    // deployments will swap to `loader::load_from_path("/etc/hedge/config.yaml")`
    // once config-file mounting lands on the systemd unit; until then
    // the defaults give us R26.1's 09:15–09:45 IST war-mode window
    // and R31.1's 09:15–15:30 IST session window out of the box.
    let cfg = load_default();

    let nats_url =
        std::env::var("HEDGE_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    info!(target: "hedge_session", %nats_url, "connecting to NATS");

    let client = NatsClient::connect(&nats_url)
        .await
        .with_context(|| format!("connecting to NATS at {nats_url}"))?;

    // Both controllers share one wall clock — tests already exercise
    // this through the `WallClock` trait (see the unit suites in
    // `controller.rs` and `session_controller.rs`).
    let clock = Arc::new(SystemWallClock);

    // War_Mode publisher and controller (task 42.1).
    let warmode_publisher = Arc::new(NatsOpsEventPublisher::new(&client));
    let warmode_controller = WarModeController::new(
        cfg.war_mode.clone(),
        Arc::clone(&clock),
        warmode_publisher,
    );

    info!(
        target: "hedge_session",
        start_ist = %cfg.war_mode.start_ist,
        end_ist = %cfg.war_mode.end_ist,
        min_confidence = cfg.war_mode.min_confidence,
        scan_multiplier = cfg.war_mode.scan_multiplier,
        "WarModeController starting"
    );

    // Trading_Session publisher and controller (task 43.1).
    let session_publisher = Arc::new(NatsSessionEventPublisher::new(&client));
    let session_controller = SessionController::new(
        cfg.session.clone(),
        Arc::clone(&clock),
        session_publisher,
    );

    info!(
        target: "hedge_session",
        start_ist = %cfg.session.start_ist,
        end_ist = %cfg.session.end_ist,
        "SessionController starting"
    );

    // Spawn both controllers and surface the first failure. `select!`
    // returns the moment any branch resolves, so an error in either
    // future propagates without leaking the other task — tokio drops
    // the spawned `JoinHandle` when this future drops, which aborts
    // the remaining task on the next cooperative yield.
    let warmode_handle = tokio::spawn(async move {
        warmode_controller
            .run()
            .await
            .context("WarModeController exited")
    });
    let session_handle = tokio::spawn(async move {
        session_controller
            .run()
            .await
            .context("SessionController exited")
    });

    tokio::select! {
        res = warmode_handle => match res {
            Ok(Ok(())) => {
                error!(target: "hedge_session", "WarModeController returned Ok(()) — unexpected");
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(join) => Err(anyhow::anyhow!("WarModeController task join failed: {join}")),
        },
        res = session_handle => match res {
            Ok(Ok(())) => {
                error!(target: "hedge_session", "SessionController returned Ok(()) — unexpected");
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(join) => Err(anyhow::anyhow!("SessionController task join failed: {join}")),
        },
    }
}
