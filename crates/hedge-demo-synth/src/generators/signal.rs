//! `sig.emitted` synthetic signal generator.
//!
//! Emits one signal every 5–30 seconds for a randomly chosen symbol /
//! strategy / side. Every emitted signal is also pushed to the in-process
//! `SignalBus` so the downstream ai_rank / risk / exec / position generators
//! produce correlated events.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::sleep;
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::ltp_board::LtpBoard;
use crate::rng::{stream, Mulberry32};
use crate::signal_bus::{SignalBus, SignalEvent};
use crate::suppression::SuppressionRegistry;
use crate::symbols::DEMO_BASKET;

const STRATEGIES: &[&str] = &[
    "OpeningRangeBreakout",
    "VwapPullback",
    "MomentumBreakout",
    "LiquiditySweepReversal",
    "OptionsOiExpansionBreakout",
    "VolatilityCompressionBreakout",
];

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
    bus: SignalBus,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::SIGNAL);
    let subject = "sig.emitted".to_string();

    loop {
        // Poisson-spaced 5–30 s gap.
        let gap_ms = rng.range_i64(5_000, 30_000);
        sleep(Duration::from_millis(gap_ms as u64)).await;

        if !suppression.allow_publish(&subject) {
            continue;
        }

        let sym = &DEMO_BASKET[rng.range_i64(0, DEMO_BASKET.len() as i64) as usize];
        let strat = STRATEGIES[rng.range_i64(0, STRATEGIES.len() as i64) as usize];
        let side = if rng.next_f64() > 0.5 { "buy" } else { "sell" };
        let corr_id = format!("synth-{:016x}", rng.next_u32() as u64 * 0x9E37_79B9 + now_ns() as u64);
        let base_p = rng.range_f64(0.45, 0.7);
        let conf = rng.range_f64(0.55, 0.85);
        let ltp = board
            .get(sym.trading_symbol)
            .map(|q| q.ltp_paise)
            .unwrap_or(sym.anchor_paise);

        let payload = synth_tag(json!({
            "correlation_id": corr_id,
            "strategy": strat,
            "symbol": sym.trading_symbol,
            "side": side,
            "base_probability": base_p,
            "confidence": conf,
            "ts_ns": now_ns(),
        }));
        let bytes = serde_json::to_vec(&payload)?;
        if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
            debug!(error = %e, "synth sig.emitted publish failed");
            continue;
        }

        let _ = bus
            .sender()
            .send(SignalEvent {
                correlation_id: corr_id,
                symbol: sym.trading_symbol,
                side,
                strategy: strat,
                ltp_paise: ltp,
                base_probability: base_p,
                confidence: conf,
            })
            .await;
    }
}
