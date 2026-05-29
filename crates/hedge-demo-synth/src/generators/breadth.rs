//! `md.breadth.sector` and `md.breadth.volatility` synthetic generators.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::ltp_board::LtpBoard;
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;
use crate::symbols::{sectors, DEMO_BASKET};

const SECTOR_PERIOD: Duration = Duration::from_secs(1);
const VOLATILITY_PERIOD: Duration = Duration::from_secs(5);

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> anyhow::Result<()> {
    // Two independent loops sharing the same task (alternating cadence).
    let nats_a = nats.clone();
    let sup_a = suppression.clone();
    let board_a = board.clone();
    let sector_handle = tokio::spawn(async move { run_sector(nats_a, sup_a, board_a).await });

    let nats_b = nats.clone();
    let sup_b = suppression.clone();
    let board_b = board.clone();
    let vol_handle = tokio::spawn(async move { run_volatility(nats_b, sup_b, board_b).await });

    let _ = tokio::try_join!(sector_handle, vol_handle);
    Ok(())
}

async fn run_sector(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    _board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::BREADTH);
    let mut ticker = interval(SECTOR_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let subject = "md.breadth.sector".to_string();
    let sectors = sectors();
    loop {
        ticker.tick().await;
        if !suppression.allow_publish(&subject) {
            continue;
        }
        // Cycle through sectors so the panel shows movement across the basket.
        for sec in &sectors {
            let total = DEMO_BASKET.iter().filter(|d| d.sector == *sec).count() as i64;
            // Use a 60/40 advancers/decliners-ish bias modulated by RNG.
            let bias = rng.range_f64(0.3, 0.7);
            let adv = ((total as f64) * bias).round() as i64;
            let dec = total - adv;
            let payload = synth_tag(json!({
                "kind": "breadth.sector",
                "data": {
                    "sector": sec,
                    "advancers": adv.max(0),
                    "decliners": dec.max(0),
                    "ts_ns": now_ns(),
                }
            }));
            let bytes = serde_json::to_vec(&payload)?;
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth breadth.sector publish failed");
            }
        }
    }
}

async fn run_volatility(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    _board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::BREADTH);
    let mut ticker = interval(VOLATILITY_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let subject = "md.breadth.volatility".to_string();
    loop {
        ticker.tick().await;
        if !suppression.allow_publish(&subject) {
            continue;
        }
        // Realistic volatility ratio in [0.01, 0.08] with mild persistence.
        let v = rng.range_f64(0.01, 0.08);
        let payload = synth_tag(json!({
            "kind": "breadth.volatility",
            "data": {
                "volatility": v,
                "ts_ns": now_ns(),
            }
        }));
        let bytes = serde_json::to_vec(&payload)?;
        if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
            debug!(error = %e, "synth breadth.volatility publish failed");
        }
    }
}
