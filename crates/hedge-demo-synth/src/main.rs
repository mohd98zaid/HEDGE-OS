//! `hedge-demo-synth` — deterministic synthetic publisher.
//!
//! Boots the coordinator that runs every per-subject generator. Each
//! generator wraps every publish in a SuppressionRegistry check so the
//! synth defers to real publishers automatically.

use std::env;

use anyhow::{Context, Result};
use tracing::info;

mod coordinator;
mod derive;
mod generators;
mod ltp_board;
mod rng;
mod signal_bus;
mod suppression;
mod symbols;

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    if let Ok(v) = env::var("HEDGE_DEMO_SYNTH") {
        let v_lower = v.trim().to_ascii_lowercase();
        if v_lower == "off" || v_lower == "false" || v_lower == "0" {
            info!("HEDGE_DEMO_SYNTH={} — exiting without publishing", v);
            return Ok(());
        }
    }

    let nats_url = env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());
    info!(nats_url = %nats_url, "demo-synth starting");

    let nats = hedge_bus::NatsClient::connect(&nats_url)
        .await
        .with_context(|| format!("connect to NATS at {}", nats_url))?;
    info!("demo-synth ready");

    coordinator::run(nats).await
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}
