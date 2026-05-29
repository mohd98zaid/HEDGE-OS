//! `hedge-position` — Position_Engine binary entry point.
//!
//! Phase C wiring (full-cockpit-data spec, task C.6):
//!
//!   * Subscribes to `exec.fill.*` (cockpit-shaped JSON fills) and folds
//!     each into the [`PositionEngine`] via `on_fill`.
//!   * Subscribes to `md.tick.bin.>` (binary Tick_v1) and folds each tick
//!     into `on_tick` for mark-to-market.
//!   * Publishes `pos.update.<sym>` per fill and `pos.risk_state` on the
//!     cockpit `RiskEvent` discriminator so the Positions / LivePnl / Risk
//!     panels render real P&L.
//!
//! The PositionEngine itself (qty / avg-cost / realised / unrealised P&L,
//! aggregate exposure / drawdown / margin) was already fully implemented;
//! this binary supplies the NATS glue.

use anyhow::{Context, Result};
use futures::StreamExt;
use hedge_config::{load_default, HedgeConfig};
use hedge_core::{Px, Side, SymbolId};
use hedge_position::{PositionEngine, PositionEvent};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

const SERVICE_NAME: &str = "hedge-position";
const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        .json()
        .with_target(true)
        .try_init()
        .ok();

    let config: HedgeConfig = load_default();
    let base_capital_paise: i64 = i64::from(config.capital.base_inr) * 100;
    info!(
        target: SERVICE_NAME,
        base_capital_inr = config.capital.base_inr,
        "Position_Engine starting"
    );

    let engine = PositionEngine::new(base_capital_paise);

    let nats_url = std::env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());
    let nats = hedge_bus::NatsClient::connect(&nats_url)
        .await
        .with_context(|| format!("connect to NATS at {}", nats_url))?;
    info!(target: SERVICE_NAME, nats_url = %nats_url, "connected to NATS");

    // --- Fill consumer ----------------------------------------------------
    {
        let mut sub = nats.raw().subscribe("exec.fill.>".to_string()).await?;
        let engine = engine.clone();
        let nats_pub = nats.clone();
        tokio::spawn(async move {
            info!(target: SERVICE_NAME, "subscribed exec.fill.>");
            while let Some(msg) = sub.next().await {
                if let Err(e) =
                    handle_fill(&engine, &nats_pub, base_capital_paise, msg.payload.as_ref()).await
                {
                    debug!(target: SERVICE_NAME, error = %e, "handle_fill error");
                }
            }
            warn!(target: SERVICE_NAME, "exec.fill.> subscription ended");
        });
    }

    // --- Tick consumer (mark-to-market) -----------------------------------
    {
        let mut sub = nats.raw().subscribe("md.tick.bin.>".to_string()).await?;
        let engine = engine.clone();
        let nats_pub = nats.clone();
        tokio::spawn(async move {
            info!(target: SERVICE_NAME, "subscribed md.tick.bin.>");
            while let Some(msg) = sub.next().await {
                if let Err(e) =
                    handle_tick(&engine, &nats_pub, base_capital_paise, msg.payload.as_ref()).await
                {
                    debug!(target: SERVICE_NAME, error = %e, "handle_tick error");
                }
            }
            warn!(target: SERVICE_NAME, "md.tick.bin.> subscription ended");
        });
    }

    tokio::signal::ctrl_c()
        .await
        .context("install ctrl_c handler")?;
    info!(target: SERVICE_NAME, "Position_Engine shutting down");
    Ok(())
}

/// Fold one `exec.fill.<sym>` JSON event into the engine and publish the
/// resulting position-update + risk-state events.
async fn handle_fill(
    engine: &PositionEngine,
    nats: &hedge_bus::NatsClient,
    base_capital_paise: i64,
    bytes: &[u8],
) -> Result<()> {
    let v: Value = serde_json::from_slice(bytes).context("parse exec.fill")?;
    let data = v.get("data").unwrap_or(&v);

    let symbol_str = data.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
    let symbol_id = hedge_bus::symbol_id_for(symbol_str);
    if symbol_id == 0 {
        return Ok(());
    }
    let qty = data.get("filled_qty").and_then(|x| x.as_u64()).unwrap_or(0);
    if qty == 0 {
        return Ok(());
    }
    // The cockpit fill carries side implicitly; exec currently emits Buy
    // fills. avg_fill_paise may be null in paper mode — fall back to 0.
    let fill_paise = data
        .get("avg_fill_paise")
        .and_then(|x| x.as_i64())
        .unwrap_or(0);
    let side = match data.get("side").and_then(|x| x.as_str()) {
        Some("sell") | Some("Sell") => Side::Sell,
        _ => Side::Buy,
    };

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    let events = engine.on_fill(
        SymbolId::new(symbol_id),
        side,
        qty,
        Px::from_paise(fill_paise),
        now_ns,
    );
    publish_events(nats, base_capital_paise, symbol_str, &events).await;
    Ok(())
}

/// Fold one binary `Tick_v1` into the engine for mark-to-market and
/// publish any resulting events.
async fn handle_tick(
    engine: &PositionEngine,
    nats: &hedge_bus::NatsClient,
    base_capital_paise: i64,
    bytes: &[u8],
) -> Result<()> {
    // Tick_v1 layout (see hedge-features::decode_tick): symbol u32 at
    // offset 16, ltp_paise i64 at offset 21.
    if bytes.len() < 29 {
        return Ok(());
    }
    let symbol_id = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    if symbol_id == 0 {
        return Ok(());
    }
    let ltp_paise = i64::from_le_bytes(bytes[21..29].try_into().unwrap());
    let symbol_str = hedge_bus::symbol_for_id(symbol_id).unwrap_or("UNKNOWN");

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    let events = engine.on_tick(SymbolId::new(symbol_id), Px::from_paise(ltp_paise), now_ns);
    publish_events(nats, base_capital_paise, symbol_str, &events).await;
    Ok(())
}

/// Publish the engine's emitted events on the cockpit `RiskEvent` shape.
async fn publish_events(
    nats: &hedge_bus::NatsClient,
    base_capital_paise: i64,
    symbol_str: &str,
    events: &[PositionEvent],
) {
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    for ev in events {
        match ev {
            PositionEvent::PositionUpdate { snapshot, .. } => {
                let payload = json!({
                    "kind": "pos.update",
                    "data": {
                        "symbol": symbol_str,
                        "quantity": snapshot.quantity,
                        "avg_price_paise": snapshot.avg_entry_px.to_paise(),
                        "realised_pnl_inr": (snapshot.realized_pnl_paise as f64) / 100.0,
                        "unrealised_pnl_inr": (snapshot.unrealized_pnl_paise as f64) / 100.0,
                        "ts_ns": now_ns,
                    }
                });
                publish(nats, &format!("pos.update.{}", symbol_str), &payload).await;
            }
            PositionEvent::RiskState(rs) => {
                // equity = peak_equity - drawdown = base_capital + total_pnl.
                // The cockpit's `portfolio_pnl_inr` wants total P&L, so we
                // subtract the base capital back out.
                let equity_paise = rs.peak_equity_paise - rs.drawdown_paise;
                let portfolio_pnl_paise = equity_paise - base_capital_paise;
                let payload = json!({
                    "kind": "pos.risk_state",
                    "data": {
                        "gross_exposure_inr": (rs.aggregate_exposure_paise as f64) / 100.0,
                        "portfolio_pnl_inr": (portfolio_pnl_paise as f64) / 100.0,
                        "drawdown_inr": (rs.drawdown_paise as f64) / 100.0,
                        "ts_ns": now_ns,
                    }
                });
                publish(nats, "pos.risk_state", &payload).await;
            }
        }
    }
}

async fn publish(nats: &hedge_bus::NatsClient, subject: &str, payload: &Value) {
    if let Ok(bytes) = serde_json::to_vec(payload) {
        if let Err(e) = nats.raw().publish(subject.to_string(), bytes.into()).await {
            debug!(target: SERVICE_NAME, subject, error = %e, "publish failed");
        }
    }
}
