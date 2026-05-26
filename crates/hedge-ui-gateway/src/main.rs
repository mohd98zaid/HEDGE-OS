//! `hedge-ui-gateway` binary entry point (task 36.1).
//!
//! The binary is the production wiring of the gateway:
//!
//! 1. Load `HedgeConfig` from `/etc/hedge/config.yaml` (with defaults).
//! 2. Connect to NATS using the `ui_gateway` account credentials.
//! 3. Subscribe to every NATS subject pattern in
//!    [`hedge_ui_gateway::channels::nats_patterns`] across the curated
//!    channel set.
//! 4. Forward every received NATS message into a process-wide
//!    `tokio::sync::broadcast` of [`NatsEvent`]; per-connection
//!    [`Dispatcher`]s subscribe to that broadcast.
//! 5. Bind a TCP listener and run [`serve`] for as long as the process
//!    lives.
//!
//! ### NATS payload shape
//!
//! The Hot_Path publishes FlatBuffers payloads on `md.*`, `of.*`,
//! `feat.*`, `sig.*`, `risk.*`, `exec.*`, `pos.*`. The Warm_AI_Pipeline
//! publishes JSON on `ai.*` and `mem.*`. The gateway converts FlatBuffers
//! payloads to JSON before fan-out (the cockpit consumes JSON) — that
//! conversion is delegated to a `payload_decode` helper in this binary.
//! Until the FlatBuffers→JSON adaptors land for every Hot_Path schema,
//! the binary forwards the raw bytes as a JSON string carrying the
//! `subject` and a base64-encoded payload, which the cockpit can decode
//! through the same `hedge-schemas` JSON adaptors. This keeps the
//! gateway operational against the existing test deployments while the
//! decoder shim is evolved alongside `hedge-schemas` task 4.x.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{info, warn};
use tracing_subscriber::{prelude::*, EnvFilter};

use hedge_ui_gateway::alerts::AlertBuffer;
use hedge_ui_gateway::channels::nats_patterns;
use hedge_ui_gateway::dispatcher::{DispatcherState, NatsEvent};
use hedge_ui_gateway::gateway::{serve, GatewayConfig};
use hedge_ui_gateway::intents::{IntentPublisher, NatsIntentPublisher};
use hedge_ui_gateway::protocol::Channel;
use hedge_ui_gateway::signals_join::{AiShadowFilter, SignalsJoiner};
use hedge_ui_gateway::volatility::VolatilityTracker;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cfg = hedge_config::load_or_default(None)
        .context("failed to load /etc/hedge/config.yaml")?;
    info!(
        ui_high_vol_threshold = cfg.ui.high_vol_threshold,
        shadow_components = ?cfg.ai.shadow_components,
        "ui-gateway starting"
    );

    // 1. NATS connection. The `ui_gateway` account has publish on
    // `trader.*` and subscribe on the curated UI subject set.
    let nats_url = std::env::var("HEDGE_NATS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let nats = match std::env::var("HEDGE_NATS_CREDS") {
        Ok(p) => hedge_bus::NatsClient::connect_with_creds(&nats_url, &p)
            .await
            .with_context(|| format!("nats connect_with_creds {} failed", &nats_url))?,
        Err(_) => hedge_bus::NatsClient::connect(&nats_url)
            .await
            .with_context(|| format!("nats connect {} failed", &nats_url))?,
    };

    // 2. Process-wide broadcast for fan-out. One sender, many per-connection
    // receivers. Capacity is sized to soak a brief receiver stall without
    // dropping critical events.
    let (event_tx, _) = broadcast::channel::<NatsEvent>(8192);

    // 3. Subscribe to every curated NATS subject pattern.
    spawn_nats_to_broadcast(&nats, event_tx.clone()).await?;

    // 4. Build shared dispatcher state.
    let intents: Arc<dyn IntentPublisher> = Arc::new(NatsIntentPublisher::new(nats));
    let state = Arc::new(DispatcherState {
        signals: Arc::new(SignalsJoiner::new(
            Duration::from_millis((cfg.ai.rank_p95_budget_ms * 4).max(2_000)),
            4096,
            Arc::new(AiShadowFilter::from_iter(cfg.ai.shadow_components.clone())),
        )),
        alerts: Arc::new(AlertBuffer::new(256)),
        volatility: Arc::new(VolatilityTracker::from_config(&cfg.ui)),
        intents,
    });

    // 5. Serve.
    let bind = std::env::var("HEDGE_UI_GATEWAY_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8088".into());
    let gw_cfg = GatewayConfig {
        bind,
        broadcast_capacity: 8192,
        signal_flush_interval: Duration::from_secs(2),
    };
    let factory = move || event_tx.subscribe();
    serve(gw_cfg, state, factory).await
}

/// Subscribe to every channel's NATS subject pattern and forward each
/// received message to `event_tx`. One subscription per pattern.
async fn spawn_nats_to_broadcast(
    nats: &hedge_bus::NatsClient,
    event_tx: broadcast::Sender<NatsEvent>,
) -> Result<()> {
    for ch in Channel::ALL {
        for pattern in nats_patterns(ch) {
            let mut sub = nats
                .raw()
                .subscribe(pattern.to_string())
                .await
                .with_context(|| format!("nats subscribe {} failed", pattern))?;
            let tx = event_tx.clone();
            let pattern_owned = pattern.to_string();
            tokio::spawn(async move {
                info!(subject = %pattern_owned, "nats fan-out subscriber started");
                while let Some(msg) = sub.next().await {
                    let subject = msg.subject.to_string();
                    let topic_suffix = subject
                        .rsplit('.')
                        .next()
                        .unwrap_or("")
                        .to_owned();
                    // Try JSON first (Warm_AI_Pipeline + UI-shaped events
                    // we forward), fall back to a base64 envelope so the
                    // cockpit can decode FlatBuffers payloads itself
                    // until the dedicated decoder shim lands.
                    let payload = decode_payload(&msg.payload);
                    let ts_ns = chrono::Utc::now()
                        .timestamp_nanos_opt()
                        .unwrap_or(0)
                        as u128;
                    let ev = NatsEvent {
                        subject,
                        topic_suffix,
                        payload,
                        ts_ns,
                    };
                    if tx.send(ev).is_err() {
                        warn!("event_tx has no receivers");
                    }
                }
                warn!(subject = %pattern_owned, "nats fan-out subscriber stream ended");
            });
        }
    }
    Ok(())
}

/// Decode a NATS payload into a JSON [`Value`]. JSON-shaped payloads
/// pass through unchanged. Anything else is wrapped in
/// `{"_raw_b64": "..."}` so the React cockpit can route to its own
/// FlatBuffers decoder until the dedicated shim lands.
fn decode_payload(bytes: &[u8]) -> Value {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(v) => v,
        Err(_) => {
            use base64::Engine as _;
            let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
            serde_json::json!({ "_raw_b64": b64 })
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .try_init();
    info!("ui-gateway tracing initialised");
}
