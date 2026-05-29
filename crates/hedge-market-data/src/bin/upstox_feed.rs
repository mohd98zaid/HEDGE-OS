//! Upstox V2 market-data → NATS bridge (REST polling mode).
//!
//! Streams live LTP and full-depth quotes from Upstox's REST endpoints and
//! publishes normalized ticks and order books onto NATS:
//!
//! * `md.tick.<symbol>`         — last traded price + close price + volume.
//! * `md.book.<symbol>`         — top-5 bid/ask depth ladder.
//! * `md.connection.upstox`     — `connected` / `disconnected` events.
//!
//! ### Why REST polling and not WebSocket protobuf?
//!
//! Upstox V2's `market-data-feed` socket is protobuf-encoded
//! (`MarketFeed.proto`) and would require code-generation, schema vendoring,
//! and varint-frame parsing. The REST endpoints ship the same fields as
//! plain JSON with very generous rate limits (LTP: 500 instruments / call,
//! ~10 calls/sec). At 500 ms cadence for 50 symbols this is well within
//! limits and gets the dashboard showing live data today. WebSocket
//! protobuf streaming is a future optimization, not a blocker for "working
//! application".
//!
//! ### Endpoints
//!
//! * `GET /v2/market-quote/ltp?instrument_key=…` — fast, frequent.
//! * `GET /v2/market-quote/quotes?instrument_key=…` — slower, full depth.
//!
//! Both accept a comma-separated list of `instrument_key` values like
//! `NSE_EQ|INE002A01018` (RELIANCE) or the legacy `NSE_EQ|2885` form.
//!
//! ### Configuration (env)
//!
//! | Variable                       | Default                                 |
//! |--------------------------------|-----------------------------------------|
//! | `HEDGE_UPSTOX_ACCESS_TOKEN`    | (required, no default; 24-hour lifetime) |
//! | `HEDGE_UPSTOX_INSTRUMENTS`     | `NSE_EQ|INE002A01018,NSE_EQ|INE090A01021,NSE_EQ|INE062A01020` |
//! | `HEDGE_UPSTOX_LTP_INTERVAL_MS` | `500`                                   |
//! | `HEDGE_UPSTOX_BOOK_INTERVAL_MS`| `2000`                                  |
//! | `HEDGE_NATS_URL`               | `nats://127.0.0.1:4222`                 |

use std::collections::HashMap;
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

use hedge_bus::NatsClient;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

// Default to a small basket of large-cap NSE equities by ISIN
// (the modern Upstox V2 instrument key form):
//   RELIANCE  = INE002A01018
//   INFY      = INE009A01021
//   SBIN      = INE062A01020
//   HDFCBANK  = INE040A01034
//   ICICIBANK = INE090A01021
const DEFAULT_INSTRUMENTS: &str =
    "NSE_EQ|INE002A01018,NSE_EQ|INE009A01021,NSE_EQ|INE062A01020,NSE_EQ|INE040A01034,NSE_EQ|INE090A01021";

const LTP_URL: &str = "https://api.upstox.com/v2/market-quote/ltp";
const QUOTE_URL: &str = "https://api.upstox.com/v2/market-quote/quotes";

const SOURCE: &str = "upstox";

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Human-readable tracing — these binaries run in dev terminals where
    // the operator wants to *see* what's happening, not pipe to a log
    // aggregator. The Hot_Path engines stay on JSON; only the broker
    // bridges and feed shims use the compact format.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let access_token = env::var("HEDGE_UPSTOX_ACCESS_TOKEN")
        .context("HEDGE_UPSTOX_ACCESS_TOKEN must be set (see .env)")?;

    let instruments_raw =
        env::var("HEDGE_UPSTOX_INSTRUMENTS").unwrap_or_else(|_| DEFAULT_INSTRUMENTS.to_string());
    let instruments: Vec<String> = instruments_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if instruments.is_empty() {
        anyhow::bail!("HEDGE_UPSTOX_INSTRUMENTS resolved to empty list");
    }

    let ltp_interval_ms: u64 = env::var("HEDGE_UPSTOX_LTP_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    let book_interval_ms: u64 = env::var("HEDGE_UPSTOX_BOOK_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000);

    let nats_url = env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());

    info!(
        nats_url = %nats_url,
        instrument_count = instruments.len(),
        ltp_interval_ms,
        book_interval_ms,
        "upstox-feed starting"
    );
    info!("instruments: {}", instruments.join(", "));

    // Connect to NATS.
    let nats = NatsClient::connect(&nats_url)
        .await
        .with_context(|| format!("NATS connect to {}", nats_url))?;
    info!("connected to NATS");

    // HTTP client with sane timeouts.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(3))
        .build()
        .context("build HTTP client")?;

    // One-shot startup probe — if the token is expired or invalid we want
    // to fail loudly *now* instead of grinding through retry loops.
    if let Err(e) = probe_token(&http, &access_token, &instruments[0..1]).await {
        publish_disconnected(&nats, &format!("{:#}", e), 0).await.ok();
        error!("startup probe failed: {:#}", e);
        error!(
            "ACTION: refresh HEDGE_UPSTOX_ACCESS_TOKEN in .env (Upstox tokens expire daily ~03:30 IST)"
        );
        anyhow::bail!(e);
    }
    info!("startup probe ok — token valid, instruments resolved");
    publish_connected(&nats).await.ok();

    // Run two independent pollers.
    let nats_ltp = nats.clone();
    let nats_book = nats.clone();
    let http_ltp = http.clone();
    let http_book = http.clone();
    let token_ltp = access_token.clone();
    let token_book = access_token.clone();
    let instruments_ltp = instruments.clone();
    let instruments_book = instruments.clone();

    let ltp_handle = tokio::spawn(async move {
        run_ltp_loop(
            &http_ltp,
            &token_ltp,
            &instruments_ltp,
            &nats_ltp,
            Duration::from_millis(ltp_interval_ms),
        )
        .await
    });
    let book_handle = tokio::spawn(async move {
        run_book_loop(
            &http_book,
            &token_book,
            &instruments_book,
            &nats_book,
            Duration::from_millis(book_interval_ms),
        )
        .await
    });

    // If either loop exits, log and bring the process down so the
    // supervisor can restart it.
    tokio::select! {
        r = ltp_handle => warn!(?r, "LTP loop exited"),
        r = book_handle => warn!(?r, "book loop exited"),
    }

    publish_disconnected(&nats, "feed loop exited", 0).await.ok();
    Ok(())
}

// ---------------------------------------------------------------------------
// Startup probe
// ---------------------------------------------------------------------------

async fn probe_token(
    http: &reqwest::Client,
    token: &str,
    instruments: &[String],
) -> Result<()> {
    let resp = http
        .get(LTP_URL)
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/json")
        .query(&[("instrument_key", instruments.join(","))])
        .send()
        .await
        .context("HTTP probe failed (network)")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        if status.as_u16() == 401 {
            anyhow::bail!(
                "Upstox returned 401 Unauthorized. Access token is expired or invalid. body={}",
                body
            );
        }
        anyhow::bail!(
            "Upstox probe returned HTTP {}: {}",
            status,
            body
        );
    }
    let _: Value = resp.json().await.context("probe response not JSON")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// LTP polling loop
// ---------------------------------------------------------------------------

async fn run_ltp_loop(
    http: &reqwest::Client,
    token: &str,
    instruments: &[String],
    nats: &NatsClient,
    interval: Duration,
) -> Result<()> {
    let mut next_tick = Instant::now();
    let mut consecutive_errors: u32 = 0;

    loop {
        let now = Instant::now();
        if now < next_tick {
            sleep(next_tick - now).await;
        }
        next_tick = Instant::now() + interval;

        match fetch_ltp(http, token, instruments).await {
            Ok(items) => {
                if consecutive_errors > 0 {
                    info!("LTP feed recovered after {} errors", consecutive_errors);
                    publish_connected(nats).await.ok();
                }
                consecutive_errors = 0;
                for (instrument_key, item) in items {
                    publish_tick(nats, &instrument_key, &item).await;
                }
            }
            Err(e) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                if consecutive_errors == 1 || consecutive_errors.is_power_of_two() {
                    warn!(error = %format!("{:#}", e), consecutive_errors, "LTP fetch failed");
                    publish_disconnected(nats, &format!("ltp: {:#}", e), consecutive_errors)
                        .await
                        .ok();
                }
                // Backoff a bit after sustained failure.
                if consecutive_errors > 5 {
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Book polling loop
// ---------------------------------------------------------------------------

async fn run_book_loop(
    http: &reqwest::Client,
    token: &str,
    instruments: &[String],
    nats: &NatsClient,
    interval: Duration,
) -> Result<()> {
    let mut next_tick = Instant::now();
    let mut consecutive_errors: u32 = 0;

    loop {
        let now = Instant::now();
        if now < next_tick {
            sleep(next_tick - now).await;
        }
        next_tick = Instant::now() + interval;

        match fetch_quotes(http, token, instruments).await {
            Ok(items) => {
                consecutive_errors = 0;
                for (instrument_key, item) in items {
                    publish_book(nats, &instrument_key, &item).await;
                }
            }
            Err(e) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                if consecutive_errors == 1 || consecutive_errors.is_power_of_two() {
                    warn!(error = %format!("{:#}", e), consecutive_errors, "quote fetch failed");
                }
                if consecutive_errors > 5 {
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP fetchers
// ---------------------------------------------------------------------------

async fn fetch_ltp(
    http: &reqwest::Client,
    token: &str,
    instruments: &[String],
) -> Result<HashMap<String, Value>> {
    let resp = http
        .get(LTP_URL)
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/json")
        .query(&[("instrument_key", instruments.join(","))])
        .send()
        .await
        .context("ltp http")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("ltp http {}: {}", status, body);
    }

    let body: Value = resp.json().await.context("ltp json")?;
    extract_data_map(&body, instruments)
}

async fn fetch_quotes(
    http: &reqwest::Client,
    token: &str,
    instruments: &[String],
) -> Result<HashMap<String, Value>> {
    let resp = http
        .get(QUOTE_URL)
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/json")
        .query(&[("instrument_key", instruments.join(","))])
        .send()
        .await
        .context("quote http")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("quote http {}: {}", status, body);
    }

    let body: Value = resp.json().await.context("quote json")?;
    extract_data_map(&body, instruments)
}

/// Upstox returns `{"status":"success","data":{ "<key>": {...} }}` where
/// the keys in `data` are *trading-symbol-named* (e.g. `NSE_EQ:RELIANCE`),
/// not the ISIN-based instrument key we requested. Each value, however,
/// includes an `instrument_token` field that *does* hold the canonical
/// `NSE_EQ|INE002A01018` form. We therefore scan values by
/// `instrument_token` and fall back to a colon-form key match.
///
/// We also inject a synthetic `_data_key` field into each value so the
/// publisher can extract the trading symbol (`RELIANCE`) from the
/// original response key, since the LTP endpoint does not return a
/// top-level `symbol` field.
fn extract_data_map(body: &Value, instruments: &[String]) -> Result<HashMap<String, Value>> {
    let data = body
        .get("data")
        .and_then(|d| d.as_object())
        .context("missing data object in upstox response")?;

    let mut out = HashMap::with_capacity(instruments.len());

    // Index by instrument_token first.
    for (k, v) in data {
        if let Some(tok) = v.get("instrument_token").and_then(|t| t.as_str()) {
            if instruments.iter().any(|i| i == tok) {
                let mut v = v.clone();
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("_data_key".into(), Value::String(k.clone()));
                }
                out.insert(tok.to_string(), v);
            }
        }
    }

    // Anything not matched yet — try the `NSE_EQ:<TRADING_SYMBOL>` form.
    for inst in instruments {
        if out.contains_key(inst) {
            continue;
        }
        let colon_form = inst.replace('|', ":");
        if let Some(v) = data.get(&colon_form) {
            let mut v = v.clone();
            if let Some(obj) = v.as_object_mut() {
                obj.insert("_data_key".into(), Value::String(colon_form.clone()));
            }
            out.insert(inst.clone(), v);
        }
    }

    Ok(out)
}

/// Pull the trading symbol out of the response value. Tries the
/// `symbol` field first (present on quotes endpoint), falls back to
/// parsing it out of the synthetic `_data_key` (`NSE_EQ:RELIANCE`),
/// finally returns `"UNKNOWN"`.
fn extract_trading_symbol(item: &Value) -> &str {
    if let Some(s) = item.get("symbol").and_then(|v| v.as_str()) {
        return s;
    }
    if let Some(s) = item.get("trading_symbol").and_then(|v| v.as_str()) {
        return s;
    }
    if let Some(k) = item.get("_data_key").and_then(|v| v.as_str()) {
        if let Some((_, sym)) = k.split_once(':') {
            return sym;
        }
    }
    "UNKNOWN"
}

// ---------------------------------------------------------------------------
// NATS publishers
// ---------------------------------------------------------------------------

async fn publish_tick(nats: &NatsClient, instrument_key: &str, item: &Value) {
    let symbol = extract_trading_symbol(item);

    // Subject keyed by the trading symbol when available — this is far
    // more useful for the cockpit than `md.tick.NSE_EQ.INE002A01018`. We
    // also publish a copy on the ISIN-keyed subject so subscribers that
    // pin to the canonical instrument_token still see it.
    let subject_symbol = format!("md.tick.{}", symbol);
    let subject_isin = format!("md.tick.{}", instrument_key.replace('|', "."));

    let ltp = item
        .get("last_price")
        .or_else(|| item.get("ltp"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let volume = item.get("volume").and_then(|v| v.as_u64());

    // Convert rupees → paise (the cockpit's Tick schema is integer paise).
    let ltp_paise = (ltp * 100.0).round() as i64;

    // The LTP-only endpoint doesn't ship bid/ask. Use ltp as a placeholder
    // for both sides; the slower /quotes poll will overwrite with real
    // depth on `md.book.<symbol>` and the reducer fills bid/ask from there.
    let bid_paise = ltp_paise;
    let ask_paise = ltp_paise;

    let ts_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);

    // Cockpit MarketEvent schema: { kind: "tick", data: Tick }
    let payload = json!({
        "kind": "tick",
        "data": {
            "symbol": symbol,
            "ltp_paise": ltp_paise,
            "bid_paise": bid_paise,
            "ask_paise": ask_paise,
            "ts_recv_ns": ts_ns,
        }
    });

    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "serialize tick");
            return;
        }
    };

    let raw = nats.raw();
    if let Err(e) = raw.publish(subject_symbol.clone(), bytes.clone().into()).await {
        error!(subject = %subject_symbol, error = %e, "publish tick failed");
    }
    if let Err(e) = raw.publish(subject_isin, bytes.into()).await {
        debug!(error = %e, "publish tick (ISIN subject) failed");
    }

    // Phase B: also publish the 93-byte Tick_v1 binary on
    // `md.tick.bin.<SYM>` so hedge-orderflow / hedge-features /
    // hedge-signals can compute on real Upstox prices. Format must match
    // hedge-features::decode_tick exactly.
    if let Some(bin) = encode_tick_v1(symbol, ltp_paise, bid_paise, ask_paise, ts_ns) {
        let subject_bin = format!("md.tick.bin.{}", symbol);
        if let Err(e) = raw.publish(subject_bin.clone(), bin.into()).await {
            debug!(subject = %subject_bin, error = %e, "publish binary tick failed");
        }
    }

    debug!(subject = %subject_symbol, %symbol, ltp, "tick");
    match volume {
        Some(v) => info!(target: "upstox::tick", "  {:>12} ltp={:>9.2} vol={}", symbol, ltp, v),
        None => info!(target: "upstox::tick", "  {:>12} ltp={:>9.2}", symbol, ltp),
    }
}

/// Encode a `Tick_v1` 93-byte little-endian record matching the layout
/// every Hot_Path engine consumes (see `hedge-features::decode_tick`):
///
/// `correlation_id [16] | symbol u32 | exchange i8 | ltp_paise i64 |
///  bid_paise i64 | ask_paise i64 | ltq u64 | total_buy_qty u64 |
///  total_sell_qty u64 | ts_exchange_ns u64 | ts_recv_ns u64`
///
/// Returns `None` for unknown symbols (id 0). Hot_Path engines drop
/// id-0 ticks; that's the right behaviour — an unknown symbol means the
/// cross-process symbol table needs a new entry.
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
    // correlation_id [16] — synth zero; real ticks carry no upstream
    // correlation, the engine that ingests this tick mints a fresh id.
    buf.extend_from_slice(&[0u8; 16]);
    buf.extend_from_slice(&symbol_id.to_le_bytes()); // u32
    buf.push(0i8 as u8); // exchange = NSE
    buf.extend_from_slice(&ltp_paise.to_le_bytes()); // i64
    buf.extend_from_slice(&bid_paise.to_le_bytes()); // i64
    buf.extend_from_slice(&ask_paise.to_le_bytes()); // i64
    buf.extend_from_slice(&0u64.to_le_bytes()); // ltq — Upstox LTP endpoint doesn't ship it
    buf.extend_from_slice(&0u64.to_le_bytes()); // total_buy_qty — only quotes endpoint has this
    buf.extend_from_slice(&0u64.to_le_bytes()); // total_sell_qty
    buf.extend_from_slice(&(ts_ns as u64).to_le_bytes()); // ts_exchange_ns (use receive ts as proxy)
    buf.extend_from_slice(&(ts_ns as u64).to_le_bytes()); // ts_recv_ns
    debug_assert_eq!(buf.len(), 85);
    Some(buf)
}

async fn publish_book(nats: &NatsClient, instrument_key: &str, item: &Value) {
    let symbol = extract_trading_symbol(item);
    let subject_symbol = format!("md.book.{}", symbol);
    let subject_isin = format!("md.book.{}", instrument_key.replace('|', "."));

    // Pull best bid / best ask out of the L5 depth ladder.
    let buys = item.pointer("/depth/buy").and_then(|v| v.as_array());
    let sells = item.pointer("/depth/sell").and_then(|v| v.as_array());

    let (bid_price, bid_qty) = best_level(buys);
    let (ask_price, ask_qty) = best_level(sells);

    let ts_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);

    // Cockpit MarketEvent schema: { kind: "book", data: BookTopOfBook }
    let payload = json!({
        "kind": "book",
        "data": {
            "symbol": symbol,
            "bid_paise": (bid_price * 100.0).round() as i64,
            "bid_qty": bid_qty,
            "ask_paise": (ask_price * 100.0).round() as i64,
            "ask_qty": ask_qty,
            "ts_ns": ts_ns,
        }
    });

    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "serialize book");
            return;
        }
    };
    let raw = nats.raw();
    if let Err(e) = raw.publish(subject_symbol.clone(), bytes.clone().into()).await {
        error!(subject = %subject_symbol, error = %e, "publish book failed");
    }
    if let Err(e) = raw.publish(subject_isin, bytes.into()).await {
        debug!(error = %e, "publish book (ISIN subject) failed");
    }
}

/// Extract `(price, qty)` from the first level of a depth ladder.
fn best_level(levels: Option<&Vec<Value>>) -> (f64, u64) {
    let level = match levels.and_then(|l| l.first()) {
        Some(l) => l,
        None => return (0.0, 0),
    };
    let price = level.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let qty = level.get("quantity").and_then(|v| v.as_u64()).unwrap_or(0);
    (price, qty)
}

async fn publish_connected(nats: &NatsClient) -> Result<()> {
    let ts_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    // Dual-shape payload: legacy flat fields for the supervisor's
    // `MdConnectionEvent` decoder (source/status="reconnected"/attempt) AND
    // cockpit-shaped {kind, data} discriminated-union fields. Both decoders
    // can read this without conflict because the field names don't collide.
    let payload = json!({
        // --- legacy flat shape (supervisor + adapter.rs ConnectionEvent) ---
        "source": SOURCE,
        "status": "reconnected",
        "reason": Value::Null,
        "attempt": 0u32,
        "at": chrono::Utc::now().to_rfc3339(),
        // --- cockpit MarketEvent shape ---
        "kind": "connection",
        "data": {
            "source": SOURCE,
            "status": "ok",
            "ts_ns": ts_ns,
        }
    });
    let bytes = serde_json::to_vec(&payload)?;
    nats.raw()
        .publish(format!("md.connection.{}", SOURCE), bytes.into())
        .await?;
    Ok(())
}

async fn publish_disconnected(nats: &NatsClient, reason: &str, attempt: u32) -> Result<()> {
    let ts_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let cockpit_status = if attempt > 5 { "down" } else { "degraded" };
    // Dual-shape payload (see publish_connected for rationale).
    let payload = json!({
        // --- legacy flat shape (supervisor + adapter.rs ConnectionEvent) ---
        "source": SOURCE,
        "status": "disconnected",
        "reason": reason,
        "attempt": attempt,
        "at": chrono::Utc::now().to_rfc3339(),
        // --- cockpit MarketEvent shape ---
        "kind": "connection",
        "data": {
            "source": SOURCE,
            "status": cockpit_status,
            "ts_ns": ts_ns,
        }
    });
    let bytes = serde_json::to_vec(&payload)?;
    nats.raw()
        .publish(format!("md.connection.{}", SOURCE), bytes.into())
        .await?;
    Ok(())
}
