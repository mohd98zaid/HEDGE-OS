//! `md.book.<SYM>` fallback generator.
//!
//! Emits cockpit-shaped book envelopes (top-of-book bid/ask + qty) at
//! 1 Hz per symbol when no real publisher is alive on the same subject.

use std::time::Duration;

use hedge_bus::NatsClient;
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::build_book_envelope;
use crate::ltp_board::{LtpBoard, Quote};
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;
use crate::symbols::DEMO_BASKET;

const BOOK_PERIOD: Duration = Duration::from_secs(1);

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::BOOK);
    let mut ticker = interval(BOOK_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        for sym in DEMO_BASKET {
            let subject = format!("md.book.{}", sym.trading_symbol);
            if !suppression.allow_publish(&subject) {
                continue;
            }
            // Pull the latest tick from the board (synth tick generator
            // updates it 4× more frequently); fall back to anchor so the
            // book is never empty.
            let q = board.get(sym.trading_symbol).unwrap_or(Quote {
                ltp_paise: sym.anchor_paise,
                bid_paise: sym.anchor_paise - 5,
                ask_paise: sym.anchor_paise + 5,
                ts_ns: crate::derive::now_ns(),
            });
            // Realistic-looking quantities: 100..2500 lots with mild bias.
            let bid_qty = rng.range_i64(100, 2500) as u64;
            let ask_qty = rng.range_i64(100, 2500) as u64;
            let payload = build_book_envelope(sym.trading_symbol, q, bid_qty, ask_qty);
            let bytes = serde_json::to_vec(&payload)?;
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(subject = %subject, error = %e, "synth book publish failed");
            }
        }
    }
}
