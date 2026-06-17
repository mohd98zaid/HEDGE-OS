//! Orderflow_Engine binary.
//!
//! Wiring overview:
//!
//! 1. Connect to NATS using `HEDGE_NATS_CREDS` if present, otherwise
//!    unauthenticated (dev only).
//! 2. Subscribe to `md.book.>` (every per-symbol book stream) using the
//!    `RawBytes` codec; decode each payload as the workspace's
//!    `OrderBook_v1` POD form (the same fixed-layout little-endian
//!    representation produced by the Market_Data_Engine).
//! 3. Subscribe to `md.tick.>` similarly.
//! 4. For each event, drive the [`OrderflowEngine`], publish the resulting
//!    [`OrderflowEvent`]s on `of.event.<symbol_id>`, and publish the
//!    refreshed [`HeatmapSnapshot`] on `of.heatmap.<symbol_id>`.
//!
//! ### NATS subject mapping
//!
//! * subscribe: `md.book.>`, `md.tick.*`
//! * publish: `of.event.<symbol_id>`, `of.heatmap.<symbol_id>`
//!
//! In production, the inbound `md.*` subscriptions terminate on a typed
//! `FlatBuffersCodec<OrderBook_v1>` once `hedge-schemas` ships the typed
//! codec (task 4.2). For now the binary plumbing accepts a fixed-layout
//! body so the rest of the wiring can be exercised end-to-end.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use hedge_bus::{subjects, JsonCodec, NatsClient, Subject};
use hedge_core::{now_ns, SymbolId};
use hedge_orderflow::{HeatmapSnapshot, OrderflowEngine, OrderflowEvent};
use tokio::task;
use tracing::{info, warn};

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let nats_url = env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());
    let nats = match env::var("HEDGE_NATS_CREDS") {
        Ok(creds) if !creds.is_empty() => {
            info!(creds = %creds, "connecting to NATS with credentials");
            NatsClient::connect_with_creds(&nats_url, PathBuf::from(creds))
                .await
                .context("nats connect with creds")?
        }
        _ => {
            warn!("HEDGE_NATS_CREDS not set; connecting to NATS unauthenticated (dev only)");
            NatsClient::connect(&nats_url).await.context("nats connect")?
        }
    };

    let engine = Arc::new(OrderflowEngine::new());

    // Spawn the inbound book listener.
    let book_engine = Arc::clone(&engine);
    let book_nats = nats.clone();
    let book_handle = task::spawn(async move {
        if let Err(err) = run_book_loop(book_engine, book_nats).await {
            tracing::error!(error = %err, "book loop terminated");
        }
    });

    // Spawn the inbound tick listener.
    let tick_engine = Arc::clone(&engine);
    let tick_nats = nats.clone();
    let tick_handle = task::spawn(async move {
        if let Err(err) = run_tick_loop(tick_engine, tick_nats).await {
            tracing::error!(error = %err, "tick loop terminated");
        }
    });

    // Run forever; either loop terminating is a fatal startup condition that
    // the supervisor restarts.
    tokio::select! {
        _ = book_handle => warn!("book loop joined"),
        _ = tick_handle => warn!("tick loop joined"),
    }
    Ok(())
}

async fn run_book_loop(_engine: Arc<OrderflowEngine>, nats: NatsClient) -> Result<()> {
    let subject: Subject<serde_json::Value> = Subject::new("md.book.>");
    let mut sub = nats
        .subscriber(subject, JsonCodec::new())
        .await
        .context("subscribe md.book.>")?;
    info!("subscribed md.book.>");
    loop {
        match sub.recv().await {
            Ok(envelope) => {
                if let Some(data) = envelope.get("data") {
                    if let Some(symbol_str) = data.get("symbol").and_then(|v| v.as_str()) {
                        let symbol_id = hedge_bus::symbol_id_for(symbol_str);
                        if symbol_id == 0 {
                            continue;
                        }

                        let bid_paise = data.get("bid_paise").and_then(|v| v.as_i64()).unwrap_or(0);
                        let bid_qty = data.get("bid_qty").and_then(|v| v.as_u64()).unwrap_or(0);
                        let ask_paise = data.get("ask_paise").and_then(|v| v.as_i64()).unwrap_or(0);
                        let ask_qty = data.get("ask_qty").and_then(|v| v.as_u64()).unwrap_or(0);
                        let ts_ns = data.get("ts_ns").and_then(|v| v.as_u64()).unwrap_or_else(engine_now_ns);

                        let mut book = hedge_schemas::OrderBook {
                            symbol: symbol_id,
                            ts_ns,
                            ..hedge_schemas::OrderBook::default()
                        };
                        if bid_qty > 0 {
                            book.bid_levels.push(hedge_schemas::BookLevel {
                                price_paise: bid_paise,
                                qty: bid_qty,
                                orders: 1,
                            });
                        }
                        if ask_qty > 0 {
                            book.ask_levels.push(hedge_schemas::BookLevel {
                                price_paise: ask_paise,
                                qty: ask_qty,
                                orders: 1,
                            });
                        }

                        if let Some(snap) = _engine.ingest_book(&book, engine_now_ns()) {
                            if let Some(heatmap_snap) = _engine.current_heatmap(hedge_core::SymbolId::new(symbol_id)) {
                                let _ = publish_heatmap(&nats, hedge_core::SymbolId::new(symbol_id), &heatmap_snap).await;
                            }
                            for ev in snap.events.as_slice() {
                                let _ = publish_event(&nats, hedge_core::SymbolId::new(symbol_id), ev).await;
                            }
                        }
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "book recv terminated");
                break Err(err.into());
            }
        }
    }
}

async fn run_tick_loop(_engine: Arc<OrderflowEngine>, nats: NatsClient) -> Result<()> {
    use hedge_bus::{FlatBuffersCodec, RawBytes};
    let subject: Subject<RawBytes> = Subject::new("md.tick.*");
    let mut sub = nats
        .subscriber(subject, FlatBuffersCodec)
        .await
        .context("subscribe md.tick.*")?;
    info!("subscribed md.tick.* (binary)");
    loop {
        match sub.recv().await {
            Ok(envelope) => {
                let bytes = envelope.as_slice();
                if bytes.len() != 77 {
                    continue; // Skip malformed ticks
                }

                // Temporary inline decoding matching decoder_shim.rs logic.
                let symbol_id = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
                let exchange = bytes[20] as i8;
                let ltp_paise = i64::from_le_bytes(bytes[21..29].try_into().unwrap());
                let bid_paise = i64::from_le_bytes(bytes[29..37].try_into().unwrap());
                let ask_paise = i64::from_le_bytes(bytes[37..45].try_into().unwrap());
                let ltq = u64::from_le_bytes(bytes[45..53].try_into().unwrap());
                let total_buy_qty = u64::from_le_bytes(bytes[53..61].try_into().unwrap());
                let total_sell_qty = u64::from_le_bytes(bytes[61..69].try_into().unwrap());
                let ts_recv_ns = u64::from_le_bytes(bytes[69..77].try_into().unwrap());

                let tick = hedge_schemas::Tick {
                    correlation_id: [0; 16],
                    symbol: symbol_id,
                    exchange,
                    ltp_paise,
                    bid_paise,
                    ask_paise,
                    ltq,
                    total_buy_qty,
                    total_sell_qty,
                    ts_exchange_ns: ts_recv_ns,
                    ts_recv_ns,
                };

                let snap = _engine.ingest_tick(&tick, ts_recv_ns);
                
                if let Some(heatmap_snap) = _engine.current_heatmap(hedge_core::SymbolId::new(symbol_id)) {
                    let _ = publish_heatmap(&nats, hedge_core::SymbolId::new(symbol_id), &heatmap_snap).await;
                }
                for ev in snap.events.as_slice() {
                    let _ = publish_event(&nats, hedge_core::SymbolId::new(symbol_id), ev).await;
                }
            }
            Err(err) => {
                warn!(error = %err, "tick recv terminated");
                break Err(err.into());
            }
        }
    }
}

/// Publish a single [`OrderflowEvent`] on `of.event.<symbol_id>`. Public
/// so a future replay-driven harness (Replay_Engine) can call into the same
/// publish path without going through the live binary.
pub async fn publish_event(
    nats: &NatsClient,
    symbol: SymbolId,
    event: &OrderflowEvent,
) -> Result<()> {
    let subject: Subject<OrderflowEvent> = subjects::of_event(symbol);
    let publisher = nats.publisher(subject, JsonCodec::<OrderflowEvent>::new());
    publisher.publish(event).await.context("publish of.event")
}

/// Publish a [`HeatmapSnapshot`] on `of.heatmap.<symbol_id>`. Public for
/// the same reason as [`publish_event`].
pub async fn publish_heatmap(
    nats: &NatsClient,
    symbol: SymbolId,
    snap: &HeatmapSnapshot,
) -> Result<()> {
    let subject: Subject<HeatmapSnapshot> = subjects::of_heatmap(symbol);
    let publisher = nats.publisher(subject, JsonCodec::<HeatmapSnapshot>::new());
    publisher.publish(snap).await.context("publish of.heatmap")
}

/// Stamp the current monotonic nanosecond time. Re-exported for the public
/// `publish_*` helpers to share the same clock as the engine.
#[inline]
pub fn engine_now_ns() -> u64 {
    now_ns()
}
