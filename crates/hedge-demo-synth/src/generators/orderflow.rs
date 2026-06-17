//! `of.event.<SYM>` and `of.heatmap.<SYM>` synthetic generators.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::{json, Value};
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::ltp_board::LtpBoard;
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;
use crate::symbols::DEMO_BASKET;

const EVENT_PERIOD: Duration = Duration::from_millis(500);
const HEATMAP_PERIOD: Duration = Duration::from_secs(1);

const EVENT_NAMES: &[&str] = &["LiquidityGap", "Absorption", "HiddenLiquidity", "Spoofing"];

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> anyhow::Result<()> {
    let nats_a = nats.clone();
    let sup_a = suppression.clone();
    let board_a = board.clone();
    let event_handle = tokio::spawn(async move { run_events(nats_a, sup_a, board_a).await });

    let nats_b = nats.clone();
    let sup_b = suppression.clone();
    let board_b = board.clone();
    let heat_handle = tokio::spawn(async move { run_heatmaps(nats_b, sup_b, board_b).await });

    let _ = tokio::try_join!(event_handle, heat_handle);
    Ok(())
}

async fn run_events(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    _board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::ORDERFLOW_EVENT);
    let mut ticker = interval(EVENT_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        // Fire on a random subset (~30%) of symbols each tick.
        for sym in DEMO_BASKET {
            if rng.next_f64() > 0.3 {
                continue;
            }
            let subject = format!("of.event.{}", sym.trading_symbol);
            if !suppression.allow_publish(&subject) {
                continue;
            }
            let event_idx = rng.range_i64(0, EVENT_NAMES.len() as i64) as usize;
            let event_name = EVENT_NAMES[event_idx];
            let payload = synth_tag(json!({
                "kind": "event",
                "data": {
                    "symbol": sym.trading_symbol,
                    "event": event_name,
                    "ts_ns": now_ns(),
                    "detail": format!("synthetic {} on {}", event_name, sym.trading_symbol),
                }
            }));
            let bytes = serde_json::to_vec(&payload)?;
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth of.event publish failed");
            }
        }
    }
}

async fn run_heatmaps(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::ORDERFLOW_HEATMAP);
    let mut ticker = interval(HEATMAP_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        for sym in DEMO_BASKET {
            let subject = format!("of.heatmap.{}", sym.trading_symbol);
            if !suppression.allow_publish(&subject) {
                continue;
            }
            let ltp = board
                .get(sym.trading_symbol)
                .map(|q| q.ltp_paise)
                .unwrap_or(sym.anchor_paise);
            let cells = build_cells(ltp, &mut rng);
            let payload = synth_tag(json!({
                "kind": "heatmap",
                "data": {
                    "symbol": sym.trading_symbol,
                    "cells": cells,
                    "ts_ns": now_ns(),
                }
            }));
            let bytes = serde_json::to_vec(&payload)?;
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth of.heatmap publish failed");
            }
        }
    }
}

fn build_cells(ltp_paise: i64, rng: &mut Mulberry32) -> Vec<Value> {
    let step = (ltp_paise / 10_000).max(5); // ~0.01% in paise
    (-5..=5i64)
        .map(|offset| {
            let price = ltp_paise + offset * step;
            // Buy heavier at offsets <= 0 (below LTP), sell heavier above.
            let buy_bias = if offset <= 0 { 1.5 } else { 0.6 };
            let sell_bias = if offset > 0 { 1.5 } else { 0.6 };
            let buy_qty = (rng.range_f64(20.0, 800.0) * buy_bias) as u64;
            let sell_qty = (rng.range_f64(20.0, 800.0) * sell_bias) as u64;
            json!({
                "price_paise": price,
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
            })
        })
        .collect()
}
