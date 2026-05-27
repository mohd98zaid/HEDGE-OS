//! Upstox V2 WebSocket market data feed → NATS bridge.
//!
//! Connects to the Upstox V2 market data WebSocket, subscribes to configured
//! instruments, normalizes incoming ticks, and publishes them to NATS on
//! `md.tick.<instrument>` and `md.book.<instrument>` subjects.

use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use hedge_bus::NatsClient;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const DEFAULT_INSTRUMENTS: &str = "NSE_EQ|2885,NSE_EQ|1594,NSE_EQ|3045";
const AUTHORIZE_URL: &str = "https://api.upstox.com/v2/feed/market-data-feed/authorize";

/// Maximum reconnection backoff (60 seconds).
const MAX_BACKOFF: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let access_token = env::var("HEDGE_UPSTOX_ACCESS_TOKEN")
        .context("HEDGE_UPSTOX_ACCESS_TOKEN must be set")?;

    let instruments_raw =
        env::var("HEDGE_UPSTOX_INSTRUMENTS").unwrap_or_else(|_| DEFAULT_INSTRUMENTS.to_string());
    let instruments: Vec<String> = instruments_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let nats_url = env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());

    info!(nats_url = %nats_url, instruments = ?instruments, "upstox-feed starting");

    // Connect to NATS
    let nats = NatsClient::connect(&nats_url)
        .await
        .context("NATS connect")?;
    info!("connected to NATS");

    // Reconnection loop with exponential backoff
    let mut backoff = Duration::from_secs(1);

    loop {
        match run_feed_loop(&access_token, &instruments, &nats).await {
            Ok(()) => {
                // Clean disconnect (server closed); reset backoff
                warn!("WebSocket closed cleanly; reconnecting...");
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                error!(error = %e, "feed loop error; reconnecting after backoff");
            }
        }

        info!(backoff_secs = backoff.as_secs(), "waiting before reconnect");
        sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

// ---------------------------------------------------------------------------
// Feed loop
// ---------------------------------------------------------------------------

async fn run_feed_loop(
    access_token: &str,
    instruments: &[String],
    nats: &NatsClient,
) -> Result<()> {
    // Step 1: Get authorized WebSocket URL
    let ws_url = get_authorized_ws_url(access_token).await?;
    info!(ws_url = %ws_url, "obtained authorized WebSocket URL");

    // Step 2: Connect to WebSocket
    let (ws_stream, _response) = connect_async(&ws_url)
        .await
        .context("WebSocket connect failed")?;
    info!("WebSocket connected");

    let (mut write, mut read) = ws_stream.split();

    // Step 3: Subscribe to instruments
    let sub_msg = serde_json::json!({
        "guid": "hedge-feed-1",
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": instruments
        }
    });
    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .context("send subscription message")?;
    info!(instruments = ?instruments, "subscribed to instruments");

    // Step 4: Read messages and publish to NATS
    while let Some(msg_result) = read.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "WebSocket read error");
                return Err(e.into());
            }
        };

        match msg {
            Message::Text(text) => {
                if let Err(e) = process_message(&text, nats).await {
                    debug!(error = %e, "failed to process message");
                }
            }
            Message::Binary(data) => {
                // Upstox may send binary frames; try to parse as UTF-8 JSON
                if let Ok(text) = String::from_utf8(data.to_vec()) {
                    if let Err(e) = process_message(&text, nats).await {
                        debug!(error = %e, "failed to process binary message");
                    }
                }
            }
            Message::Ping(_) | Message::Pong(_) => {
                // tokio-tungstenite handles ping/pong automatically
            }
            Message::Close(frame) => {
                info!(frame = ?frame, "WebSocket close frame received");
                return Ok(());
            }
            _ => {}
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Authorize
// ---------------------------------------------------------------------------

async fn get_authorized_ws_url(access_token: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .get(AUTHORIZE_URL)
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Accept", "application/json")
        .send()
        .await
        .context("authorize HTTP request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "authorize request failed with status {}: {}",
            status,
            body
        );
    }

    let body: Value = resp.json().await.context("parse authorize response")?;
    let ws_url = body["data"]["authorizedRedirectUri"]
        .as_str()
        .context("missing data.authorizedRedirectUri in authorize response")?
        .to_string();

    Ok(ws_url)
}

// ---------------------------------------------------------------------------
// Message processing
// ---------------------------------------------------------------------------

async fn process_message(text: &str, nats: &NatsClient) -> Result<()> {
    let msg: Value = serde_json::from_str(text).context("invalid JSON")?;

    let feeds = match msg.get("feeds").and_then(|f| f.as_object()) {
        Some(f) => f,
        None => return Ok(()), // Not a feed message (could be ack, etc.)
    };

    for (instrument_key, feed_data) in feeds {
        let subject_key = instrument_key.replace('|', ".");

        // Extract tick data from ff.marketFF.ltpc
        if let Some(ltpc) = feed_data
            .pointer("/ff/marketFF/ltpc")
        {
            let tick = build_tick(instrument_key, ltpc, &msg);
            let tick_bytes = serde_json::to_vec(&tick)?;
            let subject = format!("md.tick.{}", subject_key);

            nats.raw()
                .publish(subject.clone(), tick_bytes.into())
                .await
                .context("publish tick to NATS")?;

            debug!(
                subject = %subject,
                ltp = ?tick.get("ltp"),
                "published tick"
            );
        }

        // Extract orderbook data from ff.marketFF.marketLevel.bidAskQuote
        if let Some(book_data) = feed_data
            .pointer("/ff/marketFF/marketLevel/bidAskQuote")
        {
            let book = build_book(instrument_key, book_data, &msg);
            let book_bytes = serde_json::to_vec(&book)?;
            let subject = format!("md.book.{}", subject_key);

            nats.raw()
                .publish(subject.clone(), book_bytes.into())
                .await
                .context("publish book to NATS")?;

            debug!(subject = %subject, "published book");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tick / Book builders
// ---------------------------------------------------------------------------

fn build_tick(instrument_key: &str, ltpc: &Value, msg: &Value) -> Value {
    let ltp = ltpc.get("ltp").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ltt_num: u64 = ltpc
        .get("ltt")
        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
        .unwrap_or(0);
    let ltq = ltpc
        .get("ltq")
        .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| v.as_u64()))
        .unwrap_or(0);
    let cp = ltpc.get("cp").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let current_ts: u64 = msg
        .get("currentTs")
        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
        .unwrap_or(0);

    serde_json::json!({
        "instrument": instrument_key,
        "ltp": ltp,
        "ltt": ltt_num,
        "ltq": ltq,
        "close_price": cp,
        "exchange_ts": ltt_num,
        "received_ts": current_ts
    })
}

fn build_book(instrument_key: &str, bid_ask_quote: &Value, msg: &Value) -> Value {
    let current_ts: u64 = msg
        .get("currentTs")
        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64()))
        .unwrap_or(0);

    serde_json::json!({
        "instrument": instrument_key,
        "levels": bid_ask_quote,
        "exchange_ts": current_ts
    })
}
