//! Boot every generator task and the suppression subscriber.
//!
//! The coordinator owns:
//!
//! * A clone of the [`hedge_bus::NatsClient`] for every generator.
//! * The shared [`SuppressionRegistry`] so all generators back off
//!   together when a real publisher appears.
//! * A shared [`LtpBoard`] so derived generators (oi, breadth, orderflow,
//!   features, signal) all see the latest LTP for every symbol.
//! * The single-process [`SignalBus`] used to chain `sig.emitted` →
//!   `ai.rank` → `risk.decision.*` → `exec.*` → `pos.update.*`.

use anyhow::Result;
use futures::StreamExt;
use hedge_bus::NatsClient;
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::generators;
use crate::ltp_board::{LtpBoard, Quote};
use crate::signal_bus::SignalBus;
use crate::suppression::SuppressionRegistry;

/// Subjects the suppression subscriber listens on. Every subject the synth
/// publishes on must appear here so synth can detect a real publisher.
const SUPPRESSION_PATTERNS: &[&str] = &[
    "md.tick.>",
    "md.book.>",
    "md.oi.>",
    "md.breadth.sector",
    "md.breadth.volatility",
    "md.connection.>",
    "of.event.>",
    "of.heatmap.>",
    "feat.update.>",
    "sig.emitted",
    "ai.rank.>",
    "ai.news.impact.>",
    "ai.psych.stability",
    "ai.psych.intervention",
    "risk.decision.approved",
    "risk.decision.rejected",
    "risk.killswitch.activated",
    "risk.target.reached",
    "risk.cooldown.>",
    "pos.update.>",
    "pos.risk_state",
    "exec.order.>",
    "exec.fill.>",
    "exec.broker.failover",
    "exec.trade.closed",
    "obs.latency.>",
    "obs.budget.breach.>",
    "ops.action.replay",
];

pub async fn run(nats: NatsClient) -> Result<()> {
    let suppression = SuppressionRegistry::new();
    let board = LtpBoard::new();
    let bus = SignalBus::new(64);

    // Spawn the suppression subscriber first so we don't miss real
    // publishers that come up while we boot generators.
    spawn_suppression_subscriber(&nats, suppression.clone(), board.clone()).await?;

    let mut joinset: JoinSet<Result<()>> = JoinSet::new();

    // Per-symbol fallback generators.
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let b = board.clone();
        joinset.spawn(async move { generators::tick::run(nats, s, b).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let b = board.clone();
        joinset.spawn(async move { generators::book::run(nats, s, b).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let b = board.clone();
        joinset.spawn(async move { generators::oi::run(nats, s, b).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let b = board.clone();
        joinset.spawn(async move { generators::breadth::run(nats, s, b).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        joinset.spawn(async move { generators::connection::run(nats, s).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let b = board.clone();
        joinset.spawn(async move { generators::orderflow::run(nats, s, b).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let b = board.clone();
        joinset.spawn(async move { generators::features::run(nats, s, b).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let b = board.clone();
        let bus = bus.clone();
        joinset.spawn(async move { generators::signal::run(nats, s, b, bus).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        let bus = bus.clone();
        joinset.spawn(async move { generators::trade_chain::run(nats, s, bus).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        joinset.spawn(async move { generators::news::run(nats, s).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        joinset.spawn(async move { generators::psych::run(nats, s).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        joinset.spawn(async move { generators::latency::run(nats, s).await });
    }
    {
        let nats = nats.clone();
        let s = suppression.clone();
        joinset.spawn(async move { generators::replay::run(nats, s).await });
    }

    info!(generators = joinset.len(), "demo-synth all generators spawned");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl_c received");
        }
        Some(res) = joinset.join_next() => {
            match res {
                Ok(Ok(())) => warn!("generator task exited cleanly (unexpected)"),
                Ok(Err(e)) => warn!(error = %e, "generator task errored"),
                Err(e) => warn!(error = %e, "generator task join failed"),
            }
        }
    }

    info!("demo-synth shutting down");
    joinset.abort_all();
    Ok(())
}

async fn spawn_suppression_subscriber(
    nats: &NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> Result<()> {
    for pattern in SUPPRESSION_PATTERNS {
        let mut sub = nats.raw().subscribe(pattern.to_string()).await?;
        let suppression = suppression.clone();
        let board = board.clone();
        tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                let subject = msg.subject.to_string();
                let payload = msg.payload.as_ref();
                suppression.record_message(&subject, payload);
                // Opportunistically capture LTPs from real `md.tick.*`
                // payloads so derived synth generators can ride on top
                // of live prices when present (REQ-3.3).
                if subject.starts_with("md.tick.") {
                    if let Some(q) = parse_real_tick(payload) {
                        if let Some(sym) = subject.strip_prefix("md.tick.") {
                            board.set(sym, q);
                        }
                    }
                }
            }
            debug!(pattern, "suppression subscriber stream ended");
        });
    }
    Ok(())
}

fn parse_real_tick(payload: &[u8]) -> Option<Quote> {
    let v: Value = serde_json::from_slice(payload).ok()?;
    if v.get("_synth").and_then(|x| x.as_bool()).unwrap_or(false) {
        return None;
    }
    let data = v.get("data")?.as_object()?;
    let ltp = data.get("ltp_paise")?.as_i64()?;
    let bid = data.get("bid_paise").and_then(|x| x.as_i64()).unwrap_or(ltp - 5);
    let ask = data.get("ask_paise").and_then(|x| x.as_i64()).unwrap_or(ltp + 5);
    let ts_ns = data
        .get("ts_recv_ns")
        .and_then(|x| x.as_i64())
        .unwrap_or_else(crate::derive::now_ns);
    Some(Quote {
        ltp_paise: ltp,
        bid_paise: bid,
        ask_paise: ask,
        ts_ns,
    })
}
