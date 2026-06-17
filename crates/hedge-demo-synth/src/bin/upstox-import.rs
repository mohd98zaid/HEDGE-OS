use std::env;
use std::path::PathBuf;
use anyhow::{Context, Result};
use chrono::{NaiveDate};
use serde::Deserialize;
use tracing::{info, warn, error};

use hedge_replay::{SegmentWriter, ReplayRecord, RecordKind, DEFAULT_MAX_SEGMENT_BYTES};



fn encode_tick_v1(
    symbol: &str,
    ltp_paise: i64,
    bid_paise: i64,
    ask_paise: i64,
    ts_ns: i64,
) -> Option<Vec<u8>> {
    let symbol_id = hedge_bus::symbol_id_for(symbol);
    if symbol_id == 0 {
        return None;
    }
    let mut buf = Vec::with_capacity(85);
    buf.extend_from_slice(&[0u8; 16]); // correlation_id
    buf.extend_from_slice(&symbol_id.to_le_bytes()); // u32
    buf.push(0i8 as u8); // exchange = NSE
    buf.extend_from_slice(&ltp_paise.to_le_bytes()); // i64
    buf.extend_from_slice(&bid_paise.to_le_bytes()); // i64
    buf.extend_from_slice(&ask_paise.to_le_bytes()); // i64
    buf.extend_from_slice(&0u64.to_le_bytes()); // ltq
    buf.extend_from_slice(&0u64.to_le_bytes()); // total_buy_qty
    buf.extend_from_slice(&0u64.to_le_bytes()); // total_sell_qty
    buf.extend_from_slice(&(ts_ns as u64).to_le_bytes()); // ts_exchange_ns
    buf.extend_from_slice(&(ts_ns as u64).to_le_bytes()); // ts_recv_ns
    debug_assert_eq!(buf.len(), 85);
    Some(buf)
}

#[derive(Deserialize, Debug)]
struct UpstoxHistoricalResponse {
    status: String,
    data: Option<UpstoxData>,
}

#[derive(Deserialize, Debug)]
struct UpstoxData {
    candles: Vec<Vec<serde_json::Value>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <START_YYYY-MM-DD> <END_YYYY-MM-DD> <SYMBOL1> [SYMBOL2 ...]", args[0]);
        eprintln!("Example: {} 2025-06-01 2026-06-01 RELIANCE TCS", args[0]);
        std::process::exit(1);
    }

    let start_date_str = &args[1];
    let end_date_str = &args[2];
    let start_date = NaiveDate::parse_from_str(start_date_str, "%Y-%m-%d")
        .context("Invalid start date format. Use YYYY-MM-DD")?;
    let end_date = NaiveDate::parse_from_str(end_date_str, "%Y-%m-%d")
        .context("Invalid end date format. Use YYYY-MM-DD")?;

    
    // Parse date into a session ID (Unix timestamp at midnight UTC of the start date)
    let session_time = start_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let session_id = session_time.timestamp() as u64;

    let symbols = &args[3..];

    let access_token = env::var("HEDGE_UPSTOX_ACCESS_TOKEN").unwrap_or_default();
    if access_token.is_empty() {
        warn!("HEDGE_UPSTOX_ACCESS_TOKEN is not set. The API request might fail if it requires auth.");
    }

    let client = reqwest::Client::new();
    let mut writer = SegmentWriter::new(PathBuf::from("./replay"), DEFAULT_MAX_SEGMENT_BYTES);
    let mut sequence_no = 0;

    info!("Importing historical data from {} to {} into session {}", start_date_str, end_date_str, session_id);

    let mut current_date = start_date;
    while current_date <= end_date {
        let date_str = current_date.format("%Y-%m-%d").to_string();

        for symbol in symbols {
            let instrument_key = if symbol.contains('|') {
                symbol.to_string()
            } else if symbol == "RELIANCE" {
                "NSE_EQ|INE002A01018".to_string()
            } else if symbol == "HDFCBANK" {
                "NSE_EQ|INE040A01034".to_string()
            } else if symbol == "ICICIBANK" {
                "NSE_EQ|INE090A01021".to_string()
            } else if symbol == "SBIN" {
                "NSE_EQ|INE062A01020".to_string()
            } else if symbol == "TCS" {
                "NSE_EQ|INE467B01029".to_string()
            } else {
                format!("NSE_EQ|{}", symbol)
            };

            let url = format!(
                "https://api.upstox.com/v2/historical-candle/{}/1minute/{}/{}",
                instrument_key, date_str, date_str
            );

            info!("Fetching data for {} on {}", symbol, date_str);
            
            let mut retry_count = 0;
            let body: UpstoxHistoricalResponse = loop {
                let mut req = client.get(&url).header("Accept", "application/json");
                if !access_token.is_empty() {
                    req = req.bearer_auth(&access_token);
                }

                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Request failed for {}: {}", symbol, e);
                        if retry_count < 3 {
                            retry_count += 1;
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                        break UpstoxHistoricalResponse { status: "error".to_string(), data: None };
                    }
                };
                
                let status = resp.status();
                if !status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    if status.as_u16() == 429 {
                        warn!("Rate limited on {}. Waiting 2 seconds...", symbol);
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    } else {
                        error!("Failed to fetch {} ({}): {}", symbol, status, body);
                        break UpstoxHistoricalResponse { status: "error".to_string(), data: None };
                    }
                }

                match resp.json().await {
                    Ok(b) => break b,
                    Err(e) => {
                        error!("Failed to parse JSON for {}: {}", symbol, e);
                        break UpstoxHistoricalResponse { status: "error".to_string(), data: None };
                    }
                }
            };

            // Sleep a tiny bit between each symbol to prevent hitting 429s as often
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;

            if body.status != "success" || body.data.is_none() {
                warn!("Upstox API returned no data for {} on {}", symbol, date_str);
                continue;
            }

            let mut candles = body.data.unwrap().candles;
            // Upstox historical candles are often returned from newest to oldest. Let's ensure they are sorted chronologically.
            candles.sort_by(|a, b| {
                let ts_a = a.first().and_then(|v| v.as_str()).unwrap_or("");
                let ts_b = b.first().and_then(|v| v.as_str()).unwrap_or("");
                ts_a.cmp(ts_b)
            });

            for candle in candles {
                if candle.len() < 5 { continue; }
                let ts_str = candle[0].as_str().unwrap_or("");
                let ts_dt = chrono::DateTime::parse_from_rfc3339(ts_str).ok();
                if ts_dt.is_none() { continue; }
                
                let ts_ns = ts_dt.unwrap().timestamp_nanos_opt().unwrap_or(0);
                
                let open = candle[1].as_f64().unwrap_or(0.0);
                let high = candle[2].as_f64().unwrap_or(0.0);
                let low = candle[3].as_f64().unwrap_or(0.0);
                let close = candle[4].as_f64().unwrap_or(0.0);

                let open_paise = (open * 100.0).round() as i64;
                let high_paise = (high * 100.0).round() as i64;
                let low_paise = (low * 100.0).round() as i64;
                let close_paise = (close * 100.0).round() as i64;

                let (mid1, mid2) = if close_paise > open_paise {
                    (low_paise, high_paise) // Price dips, then rallies, then closes
                } else {
                    (high_paise, low_paise) // Price rallies, then dips, then closes
                };

                let points = [open_paise, mid1, mid2, close_paise];
                
                for (i, p) in points.iter().enumerate() {
                    let tick_ts_ns = ts_ns + (i as i64 * 15_000_000_000);
                    
                    let spread = (*p / 2_000).max(5);
                    let bid = *p - spread / 2;
                    let ask = *p + spread / 2;

                    let payload = match encode_tick_v1(symbol, *p, bid, ask, tick_ts_ns) {
                        Some(b) => b,
                        None => {
                            warn!("Symbol {} not found in symbol_table, skipping.", symbol);
                            continue;
                        }
                    };

                    let record = ReplayRecord {
                        session_id,
                        sequence_no,
                        monotonic_ns: tick_ts_ns as u64,
                        wallclock_utc: tick_ts_ns,
                        kind: RecordKind::Tick,
                        payload,
                    };

                    writer.append(&record)?;
                    sequence_no += 1;
                }
            }
        }
        
        // Advance to next day
        current_date = current_date.succ_opt().unwrap();
        // Sleep to avoid rate limits
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    writer.flush()?;
    info!("Import complete. Wrote {} synthesized records.", sequence_no);
    Ok(())
}
