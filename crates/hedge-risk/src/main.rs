//! `hedge-risk` binary entry point.
//!
//! Phase C wiring (full-cockpit-data spec, task C.1):
//!
//!   * Subscribes to `sig.emitted` (cockpit JSON shape, currently produced
//!     by `hedge-demo-synth` and — once Phase B feature → signal pipeline
//!     is fully wired — by `hedge-signals`).
//!   * Parses each signal into a `Signal_v1` struct.
//!   * Calls [`RiskEngine::evaluate_no_obs`].
//!   * Publishes `risk.decision.approved` or `risk.decision.rejected` in
//!     the cockpit `RiskEvent::decision` shape so the React Risk panel
//!     renders the decision immediately.
//!
//! Out of scope for C.1 (deferred to follow-up tasks):
//!   * `risk.cooldown.<sym>` edge events (the engine sets cooldowns
//!     internally; publishing on the edge needs a dedicated detector).
//!   * `pos.update.*` ingestion to keep `RiskState` in sync (synth
//!     publishes pos.update.* directly).
//!   * `trader.intent.killswitch` consumer to flip `KillSwitchState`.
//!   * Redis-backed cooldown / daily-PnL persistence (task C.2).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use hedge_config::{defaults, HedgeConfig};
use hedge_obs::init_metrics;
use hedge_risk::{ApprovalSigner, MockWarmCacheView, RiskEngine, WarmCacheView};
use hedge_schemas::{RiskProfile as RiskProfile_v1, Signal as Signal_v1};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        .json()
        .with_target(true)
        .try_init()
        .ok();

    let _ = init_metrics()?;

    let cfg: HedgeConfig = defaults::hedge_config();

    let key = generate_ephemeral_hmac_key();
    let signer = ApprovalSigner::from_key(key);

    let warm: Arc<dyn WarmCacheView> = Arc::new(MockWarmCacheView::neutral());

    let engine = Arc::new(RiskEngine::new(
        cfg.capital,
        cfg.risk,
        cfg.session,
        signer,
        warm,
    ));

    info!("hedge-risk: scaffolding boot complete");

    // Connect to NATS.
    let nats_url = std::env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());
    let nats = hedge_bus::NatsClient::connect(&nats_url)
        .await
        .with_context(|| format!("connect to NATS at {}", nats_url))?;
    info!(nats_url = %nats_url, "hedge-risk: connected to NATS");

    // Subscribe to sig.emitted.
    let mut sub = nats.raw().subscribe("sig.emitted".to_string()).await?;
    info!("hedge-risk: subscribed sig.emitted");

    let nats_pub = nats.clone();
    let engine_arc = Arc::clone(&engine);

    let consumer = tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            if let Err(e) =
                handle_signal(&engine_arc, &nats_pub, msg.payload.as_ref()).await
            {
                debug!(error = %e, "hedge-risk: handle_signal error");
            }
        }
        warn!("hedge-risk: sig.emitted subscription ended");
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("hedge-risk: shutdown requested");
        }
        _ = consumer => {
            warn!("hedge-risk: consumer task exited");
        }
    }
    Ok(())
}

/// Parse a single `sig.emitted` envelope and run it through the engine.
/// Both the synth and the future hedge-signals publisher carry the
/// cockpit-shaped JSON `{correlation_id, strategy, symbol, side,
/// base_probability, confidence, ts_ns, _synth?}`.
async fn handle_signal(
    engine: &RiskEngine,
    nats: &hedge_bus::NatsClient,
    bytes: &[u8],
) -> Result<()> {
    // Skip our own approvals/rejections if they ever loop back somehow.
    let v: Value = serde_json::from_slice(bytes).context("parse sig.emitted JSON")?;

    let signal = match build_signal_v1(&v) {
        Some(s) => s,
        None => {
            debug!("hedge-risk: dropped malformed sig.emitted (missing required fields)");
            return Ok(());
        }
    };

    // Use the synth's published LTP-ish field if present; otherwise zero.
    let entry_price_paise = v
        .get("entry_price_paise")
        .and_then(|x| x.as_i64())
        .unwrap_or(0);

    // Run the gate sequence.
    let decision = engine.evaluate_no_obs(&signal, entry_price_paise);

    let corr_hex = v
        .get("correlation_id")
        .and_then(|x| x.as_str())
        .unwrap_or("");

    // Build the cockpit-shaped envelope and publish.
    let (subject, payload) = build_decision_payload(corr_hex, &decision, &signal);
    let bytes = serde_json::to_vec(&payload)?;
    nats.raw().publish(subject.to_string(), bytes.into()).await?;
    Ok(())
}

/// Best-effort conversion from the cockpit-shaped JSON `sig.emitted`
/// envelope into a `Signal_v1`. Returns `None` when the envelope is
/// missing fields the engine requires (symbol, side).
fn build_signal_v1(v: &Value) -> Option<Signal_v1> {
    let symbol_str = v.get("symbol").and_then(|x| x.as_str())?;
    let symbol_id = hedge_bus::symbol_id_for(symbol_str);
    if symbol_id == 0 {
        return None;
    }
    let side_str = v.get("side").and_then(|x| x.as_str()).unwrap_or("buy");
    let side: u8 = match side_str {
        "buy" | "Buy" | "BUY" => 0,
        "sell" | "Sell" | "SELL" => 1,
        _ => 0,
    };
    let base_p = v
        .get("base_probability")
        .and_then(|x| x.as_f64())
        .unwrap_or(0.5) as f32;
    let conf = v
        .get("confidence")
        .and_then(|x| x.as_f64())
        .unwrap_or(0.5) as f32;

    // Hash the correlation_id string into a 16-byte array. Real
    // hedge-signals will produce a binary correlation_id; until then
    // we derive one from the synth's hex string.
    let cid_str = v.get("correlation_id").and_then(|x| x.as_str()).unwrap_or("");
    let mut cid_bytes = [0u8; 16];
    for (i, b) in cid_str.bytes().take(16).enumerate() {
        cid_bytes[i] = b;
    }

    Some(Signal_v1 {
        correlation_id: cid_bytes,
        strategy: 0,
        symbol: symbol_id,
        side,
        base_probability: base_p,
        confidence: conf,
        risk_profile: RiskProfile_v1 {
            stop_loss_paise: 0,
            take_profit_paise: 0,
            max_size_qty: 100, // synth signals don't carry a size hint; use a sane default
            time_horizon_seconds: 60,
        },
        ts_ns: v.get("ts_ns").and_then(|x| x.as_u64()).unwrap_or(0),
    })
}

/// Build the `risk.decision.{approved|rejected}` envelope matching the
/// cockpit's `RiskEvent::decision` discriminator.
fn build_decision_payload(
    correlation_id: &str,
    decision: &hedge_risk::RiskDecision,
    signal: &Signal_v1,
) -> (&'static str, Value) {
    let _ = signal;
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    match decision {
        hedge_risk::RiskDecision::Approved {
            sized_quantity, ..
        } => (
            "risk.decision.approved",
            json!({
                "kind": "decision",
                "data": {
                    "correlation_id": correlation_id,
                    "approved": true,
                    "rationale_code": 0,
                    "rationale": "approved",
                    "sized_quantity": *sized_quantity,
                    "ts_ns": now_ns,
                }
            }),
        ),
        hedge_risk::RiskDecision::Rejected { reason, .. } => {
            let rationale_str = format!("{}", reason);
            (
                "risk.decision.rejected",
                json!({
                    "kind": "decision",
                    "data": {
                        "correlation_id": correlation_id,
                        "approved": false,
                        "rationale_code": reason.as_u8(),
                        "rationale": rationale_str,
                        "sized_quantity": 0u64,
                        "ts_ns": now_ns,
                    }
                }),
            )
        }
    }
}

/// Generate a 64-byte ephemeral HMAC key from process-local entropy.
fn generate_ephemeral_hmac_key() -> Vec<u8> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let pid = std::process::id() as u64;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut key = Vec::with_capacity(64);
    for i in 0u64..8 {
        let salted = pid
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(nanos)
            .wrapping_add(i.wrapping_mul(0xDEAD_BEEF));
        key.extend_from_slice(&salted.to_be_bytes());
    }
    let _ = Duration::from_secs(0); // silence unused-import lint when Duration ever drops
    key
}
