//! `feat.update.<SYM>` synthetic feature snapshot publisher.
//!
//! `feat.update.*` is not directly subscribed by the cockpit gateway;
//! it's an intermediate Hot_Path subject the Signal_Engine reads. We
//! still publish synth versions so the dashboard's downstream synthetic
//! signals look believable when the real Hot_Path is not running.

use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::json;
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::ltp_board::LtpBoard;
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;
use crate::symbols::DEMO_BASKET;

const FEATURES_PERIOD: Duration = Duration::from_secs(1);

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::FEATURES);
    let mut ticker = interval(FEATURES_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        for sym in DEMO_BASKET {
            let subject = format!("feat.update.{}", sym.trading_symbol);
            if !suppression.allow_publish(&subject) {
                continue;
            }
            let ltp = board
                .get(sym.trading_symbol)
                .map(|q| q.ltp_paise)
                .unwrap_or(sym.anchor_paise);
            let payload = synth_tag(json!({
                "symbol": sym.trading_symbol,
                "vwap_paise": ltp + rng.range_i64(-50, 50),
                "ema_fast_paise": ltp + rng.range_i64(-30, 30),
                "ema_slow_paise": ltp + rng.range_i64(-100, 100),
                "atr_paise": rng.range_i64(50, 250),
                "realized_vol": rng.range_f64(0.005, 0.04),
                "momentum": rng.range_f64(-0.5, 0.5),
                "liquidity_imbalance": rng.range_f64(-0.4, 0.4),
                "ts_ns": now_ns(),
            }));
            let bytes = serde_json::to_vec(&payload)?;
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(error = %e, "synth feat.update publish failed");
            }
        }
    }
}
