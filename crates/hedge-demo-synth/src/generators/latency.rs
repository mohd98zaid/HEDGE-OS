//! `obs.latency.<stage>` synthetic latency-record publisher (1 Hz per stage)
//! plus occasional `obs.budget.breach.<stage>` events.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;

const STAGES: &[(&str, u64)] = &[
    ("TickIngest", 2_000_000),         // 2 ms budget
    ("FeatureExtraction", 3_000_000),  // 3 ms
    ("AiScoringFetch", 50_000_000),    // 50 ms
    ("RiskCheck", 5_000_000),          // 5 ms
    ("ExecutionRouting", 10_000_000),  // 10 ms
    ("BrokerSubmit", 250_000_000),     // 250 ms
];

pub async fn run(nats: NatsClient, suppression: SuppressionRegistry) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::LATENCY);
    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        for (stage, budget) in STAGES {
            let subject = format!("obs.latency.{}", stage);
            if !suppression.allow_publish(&subject) {
                continue;
            }
            // Realistic latency ~ 30%–80% of budget normally, ~1% breaches.
            let breach = rng.next_f64() < 0.01;
            let nanos = if breach {
                (*budget as f64 * rng.range_f64(1.05, 1.5)) as u64
            } else {
                (*budget as f64 * rng.range_f64(0.3, 0.8)) as u64
            };
            let payload = synth_tag(json!({
                "kind": "record",
                "data": {
                    "correlation_id": format!("synth-lat-{:08x}", rng.next_u32()),
                    "stage": stage,
                    "nanos": nanos,
                    "budget_nanos": *budget,
                    "breach": breach,
                    "ts_ns": now_ns(),
                }
            }));
            if let Ok(bytes) = serde_json::to_vec(&payload) {
                if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                    debug!(error = %e, "synth latency publish failed");
                }
            }
            if breach {
                let breach_subject = format!("obs.budget.breach.{}", stage);
                if suppression.allow_publish(&breach_subject) {
                    let payload = synth_tag(json!({
                        "stage": stage,
                        "nanos": nanos,
                        "budget_nanos": *budget,
                        "ts_ns": now_ns(),
                    }));
                    if let Ok(bytes) = serde_json::to_vec(&payload) {
                        let _ = nats.raw().publish(breach_subject, bytes.into()).await;
                    }
                }
            }
        }
    }
}
