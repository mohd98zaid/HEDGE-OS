//! Optional Loki shipper task.
//!
//! The shipper runs as part of each binary's startup (not from inside
//! `hedge-obs`'s core paths) so the Hot_Path Drop sites never link
//! `reqwest`. It drains a `mpsc::Receiver<LogEnvelope>` and pushes records
//! to the Loki HTTP `/loki/api/v1/push` endpoint, with exponential backoff
//! and the [`crate::degraded::loki_unavailable`] flag toggled on transport
//! failure.
//!
//! ### Why a separate module + feature?
//!
//! `hedge-obs` ships into Hot_Path crates as a Drop-time emitter. Adding
//! `reqwest` to the core dependency surface would put a (potentially
//! blocking) HTTP stack on the Hot_Path. Instead the shipper is gated
//! behind the `loki-shipper` feature: each binary opts in at startup, and
//! the resulting `reqwest` link footprint is contained to the supervisor /
//! main task.

#![cfg(feature = "loki-shipper")]

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::mpsc;

use crate::degraded::{self, BoundedRingLogBuffer};
use crate::error::ObsError;
use crate::logging::{LogEnvelope, LOKI_BACKLOG_CAPACITY};

/// Run the Loki shipper. The future resolves only when `rx` is closed.
///
/// On transient HTTP failure the shipper:
///
/// 1. Sets [`degraded::set_loki_unavailable(true)`].
/// 2. Backs off exponentially up to `max_backoff`.
/// 3. Retries the same envelope until success.
///
/// On reconnect (first successful push after a failure) the shipper drains
/// `backlog` in FIFO order before resuming normal flow.
pub async fn run_loki_shipper(
    url: String,
    mut rx: mpsc::Receiver<LogEnvelope>,
    backlog: Arc<BoundedRingLogBuffer<LOKI_BACKLOG_CAPACITY, LogEnvelope>>,
) -> Result<(), ObsError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| ObsError::Config(format!("reqwest builder: {}", e)))?;

    let max_backoff = Duration::from_secs(30);
    let mut backoff = Duration::from_millis(100);

    while let Some(envelope) = rx.recv().await {
        loop {
            match push_one(&client, &url, &envelope).await {
                Ok(()) => {
                    // First successful push — clear the degraded flag and
                    // drain the backlog.
                    if degraded::loki_unavailable() {
                        degraded::set_loki_unavailable(false);
                        for env in backlog.drain() {
                            // Best-effort: if a single drained envelope
                            // fails we re-enter degraded mode and re-buffer
                            // the rest.
                            if push_one(&client, &url, &env).await.is_err() {
                                degraded::set_loki_unavailable(true);
                                backlog.push(env);
                                break;
                            }
                        }
                    }
                    backoff = Duration::from_millis(100);
                    break;
                }
                Err(err) => {
                    degraded::set_loki_unavailable(true);
                    tracing::warn!(error = %err, backoff_ms = backoff.as_millis() as u64, "loki push failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct LokiStream {
    streams: Vec<LokiStreamEntry>,
}

#[derive(Serialize)]
struct LokiStreamEntry {
    stream: serde_json::Value,
    values: Vec<[String; 2]>,
}

async fn push_one(
    client: &reqwest::Client,
    url: &str,
    env: &LogEnvelope,
) -> Result<(), reqwest::Error> {
    let stream = LokiStream {
        streams: vec![LokiStreamEntry {
            stream: serde_json::json!({
                "level": env.level,
                "target": env.target,
                "severity": format!("{:?}", env.severity),
            }),
            values: vec![[env.timestamp_ns.to_string(), env.message.clone()]],
        }],
    };
    let resp = client.post(url).json(&stream).send().await?;
    resp.error_for_status().map(|_| ())
}
