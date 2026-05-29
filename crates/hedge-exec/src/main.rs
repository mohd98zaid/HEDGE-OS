//! `hedge-exec` — Execution_Engine binary entry point.
//!
//! Phase C wiring (full-cockpit-data spec, task C.3):
//!
//!   * Subscribes to `risk.decision.approved`.
//!   * For each approval, publishes `exec.order.submitted` then a
//!     simulated `exec.fill.<sym>` on the cockpit `ExecEvent` shape so
//!     the Execution panel renders the order lifecycle, preserving the
//!     Authority_Hierarchy contract (every order carries the approval's
//!     `correlation_id`).
//!
//! ### SAFETY — paper mode by default
//!
//! This binary does **NOT** submit live broker orders unless
//! `HEDGE_EXEC_LIVE=on` is explicitly set AND a real broker adapter is
//! wired. Placing real orders on a live Upstox account is irreversible
//! and must be an explicit, deliberate operator choice. In the default
//! paper mode the engine emits realistic synthetic fills so the cockpit
//! works end-to-end without risking capital. Every paper event carries
//! `"paper": true`.

use anyhow::{Context, Result};
use futures::StreamExt;
use hedge_config::{defaults, HedgeConfig};
use hedge_obs::init_metrics;
use serde_json::{json, Value};
use tracing::{debug, info, warn};

const SERVICE_NAME: &str = "hedge-exec";
const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        .json()
        .with_target(true)
        .try_init()
        .ok();

    let _ = init_metrics()?;
    let _config: HedgeConfig = defaults::hedge_config();

    let live = std::env::var("HEDGE_EXEC_LIVE")
        .map(|v| v == "on" || v == "1" || v == "true")
        .unwrap_or(false);

    if live {
        warn!(
            target: SERVICE_NAME,
            "HEDGE_EXEC_LIVE is set but no live broker adapter is wired in this build — \
             refusing to place real orders. Running in PAPER mode."
        );
    }
    info!(target: SERVICE_NAME, mode = "paper", "Execution_Engine starting");

    let nats_url = std::env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());
    let nats = hedge_bus::NatsClient::connect(&nats_url)
        .await
        .with_context(|| format!("connect to NATS at {}", nats_url))?;
    info!(target: SERVICE_NAME, nats_url = %nats_url, "connected to NATS");

    let mut sub = nats
        .raw()
        .subscribe("risk.decision.approved".to_string())
        .await?;
    info!(target: SERVICE_NAME, "subscribed risk.decision.approved");

    let nats_pub = nats.clone();
    let consumer = tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            if let Err(e) = handle_approval(&nats_pub, msg.payload.as_ref()).await {
                debug!(target: SERVICE_NAME, error = %e, "handle_approval error");
            }
        }
        warn!(target: SERVICE_NAME, "risk.decision.approved subscription ended");
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!(target: SERVICE_NAME, "shutdown requested"),
        _ = consumer => warn!(target: SERVICE_NAME, "consumer task exited"),
    }
    Ok(())
}

/// Handle one `risk.decision.approved` event: emit a submitted order then
/// a simulated fill, both tagged paper-mode and carrying the approval's
/// correlation_id (Authority_Hierarchy).
async fn handle_approval(nats: &hedge_bus::NatsClient, bytes: &[u8]) -> Result<()> {
    let v: Value = serde_json::from_slice(bytes).context("parse risk.decision.approved")?;
    let data = v.get("data").unwrap_or(&v);

    let correlation_id = data
        .get("correlation_id")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    if correlation_id.is_empty() {
        debug!(target: SERVICE_NAME, "approval missing correlation_id; skipping");
        return Ok(());
    }
    let qty = data
        .get("sized_quantity")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    if qty == 0 {
        return Ok(());
    }
    // The risk decision carries no symbol today; the synth signal that
    // produced it does. Carry an unknown symbol through gracefully — the
    // cockpit Execution panel keys on correlation_id, not symbol.
    let symbol = data
        .get("symbol")
        .and_then(|x| x.as_str())
        .unwrap_or("UNKNOWN")
        .to_string();

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let broker_order_id = format!("PAPER-{:08x}", (now_ns as u64) & 0xFFFF_FFFF);

    // exec.order.submitted
    let submitted = json!({
        "kind": "order",
        "data": {
            "correlation_id": correlation_id,
            "broker_order_id": broker_order_id,
            "symbol": symbol,
            "state": "Submitted",
            "filled_qty": 0,
            "ts_ns": now_ns,
        },
        "paper": true,
    });
    publish(nats, "exec.order.submitted", &submitted).await;

    // Simulated fill ~300ms later.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let fill_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fill = json!({
        "kind": "fill",
        "data": {
            "correlation_id": correlation_id,
            "broker_order_id": broker_order_id,
            "symbol": symbol,
            "state": "Filled",
            "filled_qty": qty,
            "avg_fill_paise": Value::Null,
            "ts_ns": fill_ns,
        },
        "paper": true,
    });
    let fill_subject = format!("exec.fill.{}", symbol);
    publish(nats, &fill_subject, &fill).await;

    Ok(())
}

async fn publish(nats: &hedge_bus::NatsClient, subject: &str, payload: &Value) {
    match serde_json::to_vec(payload) {
        Ok(bytes) => {
            if let Err(e) = nats.raw().publish(subject.to_string(), bytes.into()).await {
                debug!(target: SERVICE_NAME, subject, error = %e, "publish failed");
            }
        }
        Err(e) => debug!(target: SERVICE_NAME, error = %e, "serialize failed"),
    }
}
