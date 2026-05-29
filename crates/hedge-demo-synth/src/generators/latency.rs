//! `obs.latency.<stage>` synthetic latency-record publisher (1 Hz per stage)
//! plus a matching aggregate frame per stage and occasional
//! `obs.budget.breach.<stage>` events.
//!
//! The cockpit's Latency panel renders aggregate `{kind:"aggregate"}`
//! frames (p50/p95/p99 buckets) — not raw records. We publish both shapes
//! on the same subject so:
//!
//! * any subscriber that wants raw records (e.g. the Replay engine) gets them
//! * the Latency panel sees a steady stream of buckets
//!
//! Buckets are synthesised by drawing N independent samples each tick from
//! the same distribution and computing percentiles.

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

/// Sample size used to synthesise the per-second aggregate. Realistic
/// engines push hundreds–thousands of samples; 200 is enough to make
/// p50/p95/p99 visually distinct without wasting CPU.
const AGGREGATE_SAMPLES: usize = 200;

pub async fn run(nats: NatsClient, suppression: SuppressionRegistry) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::LATENCY);
    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut breach_counts = vec![0u64; STAGES.len()];

    loop {
        ticker.tick().await;
        for (i, (stage, budget)) in STAGES.iter().enumerate() {
            let subject = format!("obs.latency.{}", stage);
            if !suppression.allow_publish(&subject) {
                continue;
            }

            // 1. Emit a single representative record (raw subscribers).
            let breach = rng.next_f64() < 0.01;
            let record_nanos = if breach {
                (*budget as f64 * rng.range_f64(1.05, 1.5)) as u64
            } else {
                (*budget as f64 * rng.range_f64(0.3, 0.8)) as u64
            };
            if breach {
                breach_counts[i] += 1;
            }
            let record_payload = synth_tag(json!({
                "kind": "record",
                "data": {
                    "correlation_id": format!("synth-lat-{:08x}", rng.next_u32()),
                    "stage": stage,
                    "nanos": record_nanos,
                    "budget_nanos": *budget,
                    "breach": breach,
                    "ts_ns": now_ns(),
                }
            }));
            if let Ok(bytes) = serde_json::to_vec(&record_payload) {
                if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                    debug!(error = %e, "synth latency record publish failed");
                }
            }

            // 2. Emit the aggregate (cockpit Latency panel consumer).
            let mut samples: Vec<u64> = (0..AGGREGATE_SAMPLES)
                .map(|_| {
                    let factor = rng.range_f64(0.3, 0.95);
                    (*budget as f64 * factor) as u64
                })
                .collect();
            samples.sort_unstable();
            let p = |q: f64| -> u64 {
                let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
                samples[idx]
            };
            let agg_payload = synth_tag(json!({
                "kind": "aggregate",
                "data": {
                    "stage": stage,
                    "p50_nanos": p(0.50),
                    "p95_nanos": p(0.95),
                    "p99_nanos": p(0.99),
                    "budget_nanos": *budget,
                    "samples": samples.len() as u64,
                    "breach_count": breach_counts[i],
                    "ts_ns": now_ns(),
                }
            }));
            if let Ok(bytes) = serde_json::to_vec(&agg_payload) {
                if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                    debug!(error = %e, "synth latency aggregate publish failed");
                }
            }

            // 3. Optionally emit a budget-breach event.
            if breach {
                let breach_subject = format!("obs.budget.breach.{}", stage);
                if suppression.allow_publish(&breach_subject) {
                    let payload = synth_tag(json!({
                        "stage": stage,
                        "nanos": record_nanos,
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
