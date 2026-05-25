//! `hedge-risk` binary entry point.
//!
//! Wires the [`RiskEngine`](hedge_risk::RiskEngine) to its NATS / Redis
//! Stream subscriptions:
//!
//! | Subject / stream | Purpose |
//! |---|---|
//! | `hedge.hot.signals` (consumer group `risk_engine`) | inbound signals (R5.1, R29.3) |
//! | `pos.risk_state` | aggregate exposure / drawdown updates (R8.5) |
//! | `ai.regime.changed` | MarketStability factor (R13.5) |
//! | `ai.psych.intervention` | TraderDiscipline factor (R16.5–R16.7) |
//! | `ai.news.impact.*` | news-driven gating (R21) |
//! | `broker.metric.*` | broker latency table (R5.11) |
//! | `trader.intent.*` | UI-driven kill-switch / intent overrides (R20.6) |
//! | `ops.session.*` | session-window resets (R31.1) |
//!
//! Outbound:
//!
//! | Subject | Purpose |
//! |---|---|
//! | `risk.decision.approved` | approval payload (R5.13) |
//! | `risk.decision.rejected` | rejection payload (R5.2–R5.11) |
//! | `risk.killswitch.activated` | edge-triggered kill-switch event (R5.5, R5.9) |
//! | `risk.target.reached` | edge-triggered profit-target event (R32.3) |
//! | `risk.cooldown.<sym>` | per-symbol cooldown engage / release (R5.8) |
//!
//! The full subscriber wiring is implemented incrementally as the
//! upstream producers come online (tasks 11.1, 12.1, 13.1, 16.1, 22.1,
//! 25.1, 26.1, 28.1, 38.1, 43.1, 44.1). Today the binary boots the
//! engine, registers metrics, and idles — enough for `cargo run -p
//! hedge-risk --bin hedge-risk` to demonstrate end-to-end startup.

use std::sync::Arc;

use anyhow::Result;
use hedge_config::{defaults, HedgeConfig};
use hedge_obs::{init_metrics, NoopEmitter};
use hedge_risk::{ApprovalSigner, MockWarmCacheView, RiskEngine, WarmCacheView};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Bare-bones JSON logging — full ObsHandle wiring is deferred to the
    // session manager (task 43.1) which orchestrates startup for every
    // Hot_Path service.
    tracing_subscriber::fmt::fmt()
        .json()
        .with_target(true)
        .try_init()
        .ok();

    let _ = init_metrics()?;

    let cfg: HedgeConfig = defaults::hedge_config();

    // Until task 4.3 ships a real key-management story, a placeholder
    // key is generated at startup. This is acceptable because:
    //  1. The key never crosses a process boundary.
    //  2. Restart invalidates every outstanding token (single-use is
    //     enforced by the Execution_Engine FSM).
    let key = generate_ephemeral_hmac_key();
    let signer = ApprovalSigner::from_key(key);

    // WarmCacheView placeholder — real implementation arrives in task 44.1
    // (`hedge-warmcache`). Until then the engine reads neutral
    // factors so `Adaptive_Risk` behaves as `BaseRisk × Confidence`.
    let warm: Arc<dyn WarmCacheView> = Arc::new(MockWarmCacheView::neutral());

    let engine = Arc::new(RiskEngine::new(
        cfg.capital,
        cfg.risk,
        cfg.session,
        signer,
        warm,
    ));

    info!(
        "hedge-risk: scaffolding boot complete; subscribers wired in successive tasks"
    );

    // The latency-tracer emitter is `NoopEmitter` until the NATS
    // emitter is plumbed through. Engine evaluations stay safe because
    // every emission path tolerates a no-op sink.
    let _emitter = NoopEmitter;
    let _engine_arc = Arc::clone(&engine);

    // Idle until the supervisor signals shutdown. A full event loop
    // wiring lands in task 43.1.
    tokio::signal::ctrl_c().await.ok();
    info!("hedge-risk: shutdown requested");
    Ok(())
}

/// Generate a 64-byte ephemeral HMAC key from process-local entropy.
///
/// The session manager (task 43.1) replaces this with a key sealed on
/// disk and loaded under a `varlock`-style guard. For now we use the
/// monotonic clock combined with `std::process::id` so the key is
/// distinct across restarts but deterministic enough for early dev.
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
    key
}
