//! `ai.psych.stability` and `ai.psych.intervention` synthetic publishers.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;

pub async fn run(nats: NatsClient, suppression: SuppressionRegistry) -> anyhow::Result<()> {
    let nats_a = nats.clone();
    let sup_a = suppression.clone();
    let stab_handle =
        tokio::spawn(async move { run_stability(nats_a, sup_a).await });

    let nats_b = nats.clone();
    let sup_b = suppression.clone();
    let int_handle = tokio::spawn(async move { run_intervention(nats_b, sup_b).await });

    let _ = tokio::try_join!(stab_handle, int_handle);
    Ok(())
}

async fn run_stability(
    nats: NatsClient,
    suppression: SuppressionRegistry,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::PSYCH);
    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let subject = "ai.psych.stability".to_string();
    let mut score = 0.7;
    loop {
        ticker.tick().await;
        if !suppression.allow_publish(&subject) {
            continue;
        }
        // Random walk in [0.4, 0.9] with mean reversion.
        let step = rng.range_f64(-0.03, 0.03);
        score = (score + step + (0.7 - score) * 0.05).clamp(0.4, 0.9);
        let discipline = (score + rng.range_f64(-0.05, 0.05)).clamp(0.0, 1.0);
        let emotional = (score + rng.range_f64(-0.08, 0.08)).clamp(0.0, 1.0);
        let risk_cons = (score + rng.range_f64(-0.05, 0.05)).clamp(0.0, 1.0);
        let patience = (score + rng.range_f64(-0.06, 0.06)).clamp(0.0, 1.0);
        let payload = synth_tag(json!({
            "kind": "stability",
            "data": {
                "score": score,
                "components": {
                    "discipline": discipline,
                    "emotional_control": emotional,
                    "risk_consistency": risk_cons,
                    "patience": patience,
                },
                "behaviors": [],
                "ts_ns": now_ns(),
            }
        }));
        if let Ok(bytes) = serde_json::to_vec(&payload) {
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth psych.stability publish failed");
            }
        }
    }
}

async fn run_intervention(
    nats: NatsClient,
    suppression: SuppressionRegistry,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::PSYCH);
    let subject = "ai.psych.intervention".to_string();
    loop {
        // 4–10 minute gap.
        let gap = rng.range_i64(240_000, 600_000);
        sleep(Duration::from_millis(gap as u64)).await;
        if !suppression.allow_publish(&subject) {
            continue;
        }
        let actions = ["warning", "cooldown", "size_reduction", "kill_switch"];
        let action = actions[rng.range_i64(0, actions.len() as i64) as usize];
        let payload = synth_tag(json!({
            "kind": "intervention",
            "data": {
                "action": action,
                "trigger_score": rng.range_f64(0.35, 0.55),
                "ts_ns": now_ns(),
            }
        }));
        if let Ok(bytes) = serde_json::to_vec(&payload) {
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth psych.intervention publish failed");
            }
        }
    }
}
