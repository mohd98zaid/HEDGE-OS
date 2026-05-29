//! `md.connection.synth` heartbeats — keep the cockpit's connection panel
//! happy with a steady "ok" reading. Cockpit reducer expects
//! `{kind:"connection", data:{source, status, ts_ns}}` plus the
//! supervisor-friendly flat fields (source, status, attempt, at).

use std::time::Duration;

use chrono::Utc;
use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::suppression::SuppressionRegistry;

const HEARTBEAT_PERIOD: Duration = Duration::from_secs(30);

pub async fn run(nats: NatsClient, suppression: SuppressionRegistry) -> anyhow::Result<()> {
    let mut ticker = interval(HEARTBEAT_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let subject = "md.connection.synth".to_string();
    loop {
        ticker.tick().await;
        if !suppression.allow_publish(&subject) {
            continue;
        }
        let payload = synth_tag(json!({
            // legacy flat fields (supervisor-readable)
            "source": "synth",
            "status": "reconnected",
            "reason": null,
            "attempt": 0u32,
            "at": Utc::now().to_rfc3339(),
            // cockpit MarketEvent shape
            "kind": "connection",
            "data": {
                "source": "synth",
                "status": "ok",
                "ts_ns": now_ns(),
            }
        }));
        let bytes = serde_json::to_vec(&payload)?;
        if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
            debug!(error = %e, "synth connection publish failed");
        }
    }
}
