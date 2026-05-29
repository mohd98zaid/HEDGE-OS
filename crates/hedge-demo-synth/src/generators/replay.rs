//! `ops.action.replay` heartbeat — a low-rate synthetic status frame
//! that keeps the cockpit's Replay panel populated.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::suppression::SuppressionRegistry;

pub async fn run(nats: NatsClient, suppression: SuppressionRegistry) -> anyhow::Result<()> {
    let mut ticker = interval(Duration::from_secs(60));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let subject = "ops.action.replay".to_string();
    let mut sequence_no: u64 = 0;
    loop {
        ticker.tick().await;
        if !suppression.allow_publish(&subject) {
            continue;
        }
        sequence_no = sequence_no.wrapping_add(1);
        let payload = synth_tag(json!({
            "kind": "status",
            "data": {
                "session_id": "synth-session",
                "playing": false,
                "speed": 1,
                "sequence_no": sequence_no,
                "total_records": 0,
                "ts_ns": now_ns(),
            }
        }));
        if let Ok(bytes) = serde_json::to_vec(&payload) {
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth replay publish failed");
            }
        }
    }
}
