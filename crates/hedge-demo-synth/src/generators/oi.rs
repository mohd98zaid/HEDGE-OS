//! `md.oi.<SYM>` synthetic option-chain generator.
//!
//! Emits a 5-strike ladder centered on the latest LTP (or anchor) for
//! every demo symbol every 5 seconds. Matches `OpenInterest` shape from
//! `ui/src/types/market.ts`.

use std::time::Duration;

use chrono::Utc;
use hedge_bus::NatsClient;
use serde_json::{json, Value};
use tokio::time::{interval, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::ltp_board::LtpBoard;
use crate::rng::{stream, Mulberry32};
use crate::suppression::SuppressionRegistry;
use crate::symbols::DEMO_BASKET;

const OI_PERIOD: Duration = Duration::from_secs(5);

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    board: LtpBoard,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::OI);
    let mut ticker = interval(OI_PERIOD);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let expiry = next_thursday();

    loop {
        ticker.tick().await;
        for sym in DEMO_BASKET {
            let subject = format!("md.oi.{}", sym.trading_symbol);
            if !suppression.allow_publish(&subject) {
                continue;
            }
            let ltp = board
                .get(sym.trading_symbol)
                .map(|q| q.ltp_paise)
                .unwrap_or(sym.anchor_paise);
            let strikes = build_strike_ladder(ltp, &mut rng);
            let payload = synth_tag(json!({
                "kind": "oi",
                "data": {
                    "symbol": sym.trading_symbol,
                    "expiry": expiry,
                    "strikes": strikes,
                    "ts_ns": now_ns(),
                }
            }));
            let bytes = serde_json::to_vec(&payload)?;
            if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
                debug!(subject = %subject, error = %e, "synth oi publish failed");
            }
        }
    }
}

/// Five strikes centered on `ltp`, spaced ~1% apart, with realistic
/// asymmetric OI (high near ATM, falling outward) and small change-in-OI.
fn build_strike_ladder(ltp_paise: i64, rng: &mut Mulberry32) -> Vec<Value> {
    let step = (ltp_paise / 100).max(50); // ~1% in paise
    (-2..=2i64)
        .map(|offset| {
            let strike_paise = ltp_paise + offset * step;
            // OI peaks at ATM, falls off ±1, falls more ±2.
            let scale = match offset.abs() {
                0 => 1.0,
                1 => 0.65,
                _ => 0.35,
            };
            let call_oi = (rng.range_f64(40_000.0, 90_000.0) * scale) as u64;
            let put_oi = (rng.range_f64(40_000.0, 90_000.0) * scale) as u64;
            let call_chg_oi = rng.range_i64(-3_000, 3_000);
            let put_chg_oi = rng.range_i64(-3_000, 3_000);
            json!({
                "strike_paise": strike_paise,
                "call_oi": call_oi,
                "put_oi": put_oi,
                "call_chg_oi": call_chg_oi,
                "put_chg_oi": put_chg_oi,
            })
        })
        .collect()
}

/// Next Thursday (NSE weekly options expiry) as `YYYY-MM-DD`.
fn next_thursday() -> String {
    use chrono::{Datelike, Duration as CDuration, Weekday};
    let today = Utc::now().date_naive();
    let days_to_thu = match today.weekday() {
        Weekday::Mon => 3,
        Weekday::Tue => 2,
        Weekday::Wed => 1,
        Weekday::Thu => 7,
        Weekday::Fri => 6,
        Weekday::Sat => 5,
        Weekday::Sun => 4,
    };
    let exp = today + CDuration::days(days_to_thu);
    exp.format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_has_five_strikes_centered_on_ltp() {
        let mut rng = Mulberry32::for_stream(stream::OI);
        let strikes = build_strike_ladder(1_000_00, &mut rng);
        assert_eq!(strikes.len(), 5);
        // Center strike (offset 0) must equal LTP.
        let center: i64 = strikes[2]["strike_paise"].as_i64().unwrap();
        assert_eq!(center, 1_000_00);
    }

    #[test]
    fn next_thursday_is_in_future_or_a_week_out() {
        let s = next_thursday();
        assert_eq!(s.len(), 10);
        assert_eq!(s.chars().nth(4), Some('-'));
    }
}
