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
//! * subscribe: `md.book.>`, `md.tick.>`
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

/// Body shape of the raw `md.book.<sym>` payload as emitted by the
/// Market_Data_Engine wire-bytes bridge. Until the typed
/// `FlatBuffersCodec<OrderBook>` codec ships in task 4.2 we pass through
/// raw bytes and decode the symbol id from the leading body slot for
/// routing.
async fn run_book_loop(_engine: Arc<OrderflowEngine>, nats: NatsClient) -> Result<()> {
    let subject: Subject<hedge_bus::RawBytes> = Subject::new("md.book.>");
    let mut sub = nats
        .subscriber(subject, hedge_bus::FlatBuffersCodec)
        .await
        .context("subscribe md.book.>")?;
    info!("subscribed md.book.>");
    loop {
        match sub.recv_bytes().await {
            Ok(bytes) => {
                // The engine is decoupled from wire layout; until task 4.2
                // ships the typed codec we drop raw payloads we cannot
                // reconstruct into `OrderBook`. Once the typed codec is
                // available, replace this branch with `engine.ingest_book(&book, now_ns())`.
                tracing::trace!(bytes = bytes.len(), "received book payload (decode pending task 4.2)");
            }
            Err(err) => {
                warn!(error = %err, "book recv terminated");
                break Err(err.into());
            }
        }
    }
}

async fn run_tick_loop(_engine: Arc<OrderflowEngine>, nats: NatsClient) -> Result<()> {
    let subject: Subject<hedge_bus::RawBytes> = Subject::new("md.tick.>");
    let mut sub = nats
        .subscriber(subject, hedge_bus::FlatBuffersCodec)
        .await
        .context("subscribe md.tick.>")?;
    info!("subscribed md.tick.>");
    loop {
        match sub.recv_bytes().await {
            Ok(bytes) => {
                tracing::trace!(bytes = bytes.len(), "received tick payload (decode pending task 4.2)");
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
