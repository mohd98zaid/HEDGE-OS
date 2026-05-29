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

// Phase C (task C.11) — options-chain polling.
const OPTION_CHAIN_URL: &str = "https://api.upstox.com/v2/option/chain";
const OPTION_CONTRACT_URL: &str = "https://api.upstox.com/v2/option/contract";

// Default index underlyings to poll for open-interest ladders. The
// underlying instrument keys are index keys (`NSE_INDEX|<name>`), distinct
// from the equity basket above so the synthetic equity OI and the real
// index OI coexist on different `md.oi.<UNDERLYING>` subjects.
const DEFAULT_OI_UNDERLYINGS: &str = "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank";

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

    // Options-chain (OI) polling config (task C.11).
    let oi_underlyings_raw =
        env::var("HEDGE_UPSTOX_OI_UNDERLYINGS").unwrap_or_else(|_| DEFAULT_OI_UNDERLYINGS.to_string());
    let oi_underlyings: Vec<String> = oi_underlyings_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let oi_interval_ms: u64 = env::var("HEDGE_UPSTOX_OI_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000);

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

    // Options-chain (OI) poller — only spawned when underlyings are
    // configured. Read-only REST, separate cadence (default 5s).
    let oi_handle = if oi_underlyings.is_empty() {
        info!("options-chain poller disabled (HEDGE_UPSTOX_OI_UNDERLYINGS empty)");
        None
    } else {
        info!("options-chain underlyings: {}", oi_underlyings.join(", "));
        let nats_oi = nats.clone();
        let http_oi = http.clone();
        let token_oi = access_token.clone();
        Some(tokio::spawn(async move {
            run_oi_loop(
                &http_oi,
                &token_oi,
                &oi_underlyings,
                &nats_oi,
                Duration::from_millis(oi_interval_ms),
            )
            .await
        }))
    };

    // If either core loop exits, log and bring the process down so the
    // supervisor can restart it. The OI loop is best-effort — its exit is
    // logged but does not by itself tear down the feed.
    match oi_handle {
        Some(oi_handle) => {
            tokio::select! {
                r = ltp_handle => warn!(?r, "LTP loop exited"),
                r = book_handle => warn!(?r, "book loop exited"),
                r = oi_handle => warn!(?r, "OI loop exited"),
            }
        }
        None => {
            tokio::select! {
                r = ltp_handle => warn!(?r, "LTP loop exited"),
                r = book_handle => warn!(?r, "book loop exited"),
            }
        }
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
// Options-chain (OI) polling loop  (task C.11)
// ---------------------------------------------------------------------------

/// Poll the Upstox option-chain endpoint for each configured underlying and
/// publish a `md.oi.<UNDERLYING>` ladder matching the cockpit `OpenInterest`
/// shape. Read-only REST; runs on its own cadence (default 5s).
///
/// The nearest non-expired weekly expiry is discovered per underlying via
/// the `/v2/option/contract` endpoint and cached. When the cached expiry
/// rolls into the past (weekly rotation) it is transparently re-discovered.
async fn run_oi_loop(
    http: &reqwest::Client,
    token: &str,
    underlyings: &[String],
    nats: &NatsClient,
    interval: Duration,
) -> Result<()> {
    // Per-underlying cached expiry date (`YYYY-MM-DD`).
    let mut expiry_cache: HashMap<String, String> = HashMap::new();
    let mut next_tick = Instant::now();
    let mut consecutive_errors: u32 = 0;

    loop {
        let now = Instant::now();
        if now < next_tick {
            sleep(next_tick - now).await;
        }
        next_tick = Instant::now() + interval;

        for underlying in underlyings {
            // Ensure we have a fresh (non-expired) expiry for this key.
            let expiry = match resolve_expiry(http, token, underlying, &mut expiry_cache).await {
                Ok(e) => e,
                Err(e) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    if consecutive_errors == 1 || consecutive_errors.is_power_of_two() {
                        warn!(underlying = %underlying, error = %format!("{:#}", e), "option expiry resolve failed");
                    }
                    continue;
                }
            };

            match fetch_option_chain(http, token, underlying, &expiry).await {
                Ok(chain) => {
                    consecutive_errors = 0;
                    publish_oi(nats, underlying, &expiry, &chain).await;
                }
                Err(e) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    if consecutive_errors == 1 || consecutive_errors.is_power_of_two() {
                        warn!(underlying = %underlying, error = %format!("{:#}", e), "option chain fetch failed");
                    }
                    // A 4xx on a stale expiry → drop the cache so the next
                    // pass re-discovers.
                    expiry_cache.remove(underlying);
                }
            }
        }

        if consecutive_errors > 5 {
            sleep(Duration::from_secs(2)).await;
        }
    }
}

/// Return a cached non-expired expiry for `underlying`, discovering and
/// caching one if absent or if the cached value is in the past.
async fn resolve_expiry(
    http: &reqwest::Client,
    token: &str,
    underlying: &str,
    cache: &mut HashMap<String, String>,
) -> Result<String> {
    let today = today_ymd();
    if let Some(cached) = cache.get(underlying) {
        if cached.as_str() >= today.as_str() {
            return Ok(cached.clone());
        }
    }
    let expiry = discover_nearest_expiry(http, token, underlying, &today).await?;
    cache.insert(underlying.to_string(), expiry.clone());
    info!(underlying = %underlying, %expiry, "resolved option expiry");
    Ok(expiry)
}

/// Query `/v2/option/contract` for `underlying` and return the nearest
/// expiry date `>= today` (lexicographic compare works for `YYYY-MM-DD`).
async fn discover_nearest_expiry(
    http: &reqwest::Client,
    token: &str,
    underlying: &str,
    today: &str,
) -> Result<String> {
    let resp = http
        .get(OPTION_CONTRACT_URL)
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/json")
        .query(&[("instrument_key", underlying)])
        .send()
        .await
        .context("option contract http")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("option contract http {}: {}", status, body);
    }

    let body: Value = resp.json().await.context("option contract json")?;
    let data = body
        .get("data")
        .and_then(|d| d.as_array())
        .context("option contract: missing data array")?;

    let mut expiries: Vec<String> = data
        .iter()
        .filter_map(|c| c.get("expiry").and_then(|e| e.as_str()).map(String::from))
        .filter(|e| e.as_str() >= today)
        .collect();
    expiries.sort();
    expiries.dedup();

    expiries
        .into_iter()
        .next()
        .context("no future expiry found for underlying")
}

/// Fetch one option chain (`/v2/option/chain`) for `underlying` + `expiry`.
/// Returns the raw `data` array (one entry per strike).
async fn fetch_option_chain(
    http: &reqwest::Client,
    token: &str,
    underlying: &str,
    expiry: &str,
) -> Result<Vec<Value>> {
    let resp = http
        .get(OPTION_CHAIN_URL)
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/json")
        .query(&[("instrument_key", underlying), ("expiry_date", expiry)])
        .send()
        .await
        .context("option chain http")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("option chain http {}: {}", status, body);
    }

    let body: Value = resp.json().await.context("option chain json")?;
    let data = body
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .context("option chain: missing data array")?;
    Ok(data)
}

/// Build the cockpit `OpenInterest` payload from a raw Upstox chain array
/// and publish on `md.oi.<UNDERLYING>`.
///
/// Each Upstox chain entry looks like:
/// ```json
/// { "strike_price": 22000, "underlying_spot_price": 22050.5,
///   "call_options": { "market_data": { "oi": 1234, "prev_oi": 1200 } },
///   "put_options":  { "market_data": { "oi": 2345, "prev_oi": 2300 } } }
/// ```
/// Strikes are sorted ascending and trimmed to the 11 nearest the spot so
/// the panel stays readable.
async fn publish_oi(nats: &NatsClient, underlying: &str, expiry: &str, chain: &[Value]) {
    if chain.is_empty() {
        return;
    }
    let display_symbol = underlying_display(underlying);
    let subject = format!("md.oi.{}", subject_token(&display_symbol));

    let spot_paise = chain
        .iter()
        .find_map(|c| c.get("underlying_spot_price").and_then(|v| v.as_f64()))
        .map(|s| (s * 100.0).round() as i64)
        .unwrap_or(0);

    // Map every strike to the cockpit OpenInterestStrike shape.
    let mut strikes: Vec<(i64, Value)> = chain
        .iter()
        .filter_map(|c| {
            let strike_rupees = c.get("strike_price").and_then(|v| v.as_f64())?;
            let strike_paise = (strike_rupees * 100.0).round() as i64;
            let (call_oi, call_prev) = oi_pair(c.get("call_options"));
            let (put_oi, put_prev) = oi_pair(c.get("put_options"));
            let strike = json!({
                "strike_paise": strike_paise,
                "call_oi": call_oi,
                "put_oi": put_oi,
                "call_chg_oi": call_oi as i64 - call_prev as i64,
                "put_chg_oi": put_oi as i64 - put_prev as i64,
            });
            Some((strike_paise, strike))
        })
        .collect();

    strikes.sort_by_key(|(p, _)| *p);

    // Trim to the 11 strikes nearest the spot to keep the ladder compact.
    let trimmed: Vec<Value> = if spot_paise > 0 && strikes.len() > 11 {
        let mut by_dist = strikes.clone();
        by_dist.sort_by_key(|(p, _)| (p - spot_paise).abs());
        let mut nearest: Vec<(i64, Value)> = by_dist.into_iter().take(11).collect();
        nearest.sort_by_key(|(p, _)| *p);
        nearest.into_iter().map(|(_, s)| s).collect()
    } else {
        strikes.into_iter().map(|(_, s)| s).collect()
    };

    let ts_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);

    let payload = json!({
        "kind": "oi",
        "data": {
            "symbol": display_symbol,
            "expiry": expiry,
            "strikes": trimmed,
            "ts_ns": ts_ns,
        }
    });

    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "serialize oi");
            return;
        }
    };
    if let Err(e) = nats.raw().publish(subject.clone(), bytes.into()).await {
        error!(subject = %subject, error = %e, "publish oi failed");
    } else {
        info!(target: "upstox::oi", "  {:>10} {} strikes={}", display_symbol, expiry, trimmed.len());
    }
}

/// Extract `(oi, prev_oi)` from a `call_options` / `put_options` node.
fn oi_pair(side: Option<&Value>) -> (u64, u64) {
    let md = match side.and_then(|s| s.get("market_data")) {
        Some(m) => m,
        None => return (0, 0),
    };
    let oi = md.get("oi").and_then(|v| v.as_f64()).unwrap_or(0.0) as u64;
    let prev = md.get("prev_oi").and_then(|v| v.as_f64()).unwrap_or(0.0) as u64;
    (oi, prev)
}

/// Human-readable underlying name from an instrument key
/// (`NSE_INDEX|Nifty 50` → `Nifty 50`).
fn underlying_display(instrument_key: &str) -> String {
    instrument_key
        .split_once('|')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| instrument_key.to_string())
}

/// NATS subjects cannot contain spaces — collapse them so
/// `Nifty 50` → `Nifty50` for the `md.oi.<token>` subject. The
/// human-readable name is preserved in the payload's `symbol` field.
fn subject_token(display: &str) -> String {
    display.split_whitespace().collect::<Vec<_>>().concat()
}

/// Today's date as `YYYY-MM-DD` (UTC).
fn today_ymd() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
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

    // Phase B: also publish the 85-byte Tick_v1 binary on
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

/// Encode a `Tick_v1` 85-byte little-endian record matching the layout
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
