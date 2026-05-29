//! `md.tick.<SYM>` fallback generator.
//!
//! Emits cockpit-shaped tick envelopes at 4 Hz per symbol when no real
//! publisher is alive on the same subject (REQ-1.4, REQ-3.3, REQ-3.4 of
//! the full-cockpit-data spec).
//!
//! When a real `upstox-feed` is publishing, the SuppressionRegistry
//! short-circuits this loop so we never double-publish.

use std::time::Duration;

use hedge_bus::NatsClient;
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{build_tick_envelope, step_quote};
use crate::ltp_board::LtpBoard;
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;
use crate::symbols::DEMO_BASKET;

/// 4 Hz cadence — matches the live upstox-feed REST poll rate so the
/// dashboard "feels" alive without overwhelming subscribers.
const TICK_PERIOD: Duration = Duration::from_millis(250);

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::TICK);
    let mut ticker = interval(TICK_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        for sym in DEMO_BASKET {
            let subject = format!("md.tick.{}", sym.trading_symbol);
            if !suppression.allow_publish(&subject) {
                continue;
            }
            let q = step_quote(&board, sym.trading_symbol, sym.anchor_paise, &mut rng);
            let payload = build_tick_envelope(sym.trading_symbol, q);
            let bytes = serde_json::to_vec(&payload)?;
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(subject = %subject, error = %e, "synth tick publish failed");
            }
        }
    }
}
