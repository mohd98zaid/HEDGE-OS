//! `ai.news.impact.<topic>` synthetic news impact publisher.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::sleep;
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;
use crate::symbols::DEMO_BASKET;

const HEADLINES: &[&str] = &[
    "Q3 earnings beat expectations on margin expansion",
    "Regulator approves new pricing scheme",
    "Sector rotation hits IT exporters on rupee strength",
    "Banking guidance signals near-term NIM compression",
    "Energy major announces new capex cycle",
    "Mid-cap rally cools as FII flows reverse",
    "RBI commentary boosts financials sentiment",
    "Auto sales numbers miss street estimates",
    "Index breaks above multi-month resistance",
    "Volatility expands ahead of policy announcement",
];

pub async fn run(nats: NatsClient, suppression: SuppressionRegistry) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::NEWS);
    loop {
        let gap = rng.range_i64(30_000, 120_000);
        sleep(Duration::from_millis(gap as u64)).await;

        let sym = &DEMO_BASKET[rng.range_i64(0, DEMO_BASKET.len() as i64) as usize];
        let subject = format!("ai.news.impact.{}", sym.trading_symbol);
        if !suppression.allow_publish(&subject) {
            continue;
        }

        let headline = HEADLINES[rng.range_i64(0, HEADLINES.len() as i64) as usize];
        let payload = synth_tag(json!({
            "correlation_id": format!("synth-news-{:08x}", rng.next_u32()),
            "symbol": sym.trading_symbol,
            "headline_id": format!("hdl-{:08x}", rng.next_u32()),
            "headline": headline,
            "source": "synthetic",
            "sentiment": rng.range_f64(-0.8, 0.8),
            "impact_magnitude": rng.range_f64(0.1, 0.85),
            "fast_path": rng.next_f64() < 0.5,
            "slow_path_pending": rng.next_f64() < 0.3,
            "ts_ns": now_ns(),
        }));
        if let Ok(bytes) = serde_json::to_vec(&payload) {
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth news publish failed");
            }
        }
    }
}
