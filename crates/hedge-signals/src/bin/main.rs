//! Signal_Engine binary entry point.
//!
//! Loads the workspace config, opens NATS + Redis, subscribes to
//! `feat.update.*`, decodes each `FeatureSnapshot_v1` payload, runs the
//! strategy registry, and publishes every emitted `Signal_v1` on
//! `sig.emitted` and the `hedge.hot.signals` Redis Stream.
//!
//! The decode path mirrors the wire layout produced by
//! `hedge_features::engine::encode` (FlatBuffers placeholder shape:
//! every field in declaration order, little-endian, no padding).

use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result};
use hedge_bus::{FlatBuffersCodec, NatsClient, RawBytes, Subject};
use hedge_config::{load_default, load_from_path};
use hedge_schemas::FeatureSnapshot;
use hedge_signals::SignalEngine;
use redis::aio::ConnectionManager;
use tracing::{info, warn};

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    hedge_obs::init_metrics().context("metrics init")?;

    // Configuration: prefer `/etc/hedge/config.yaml` when present,
    // otherwise fall back to the workspace defaults.
    let cfg_path = env::var("HEDGE_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/etc/hedge/config.yaml"));
    let _cfg = if cfg_path.exists() {
        load_from_path(&cfg_path).context("load config from disk")?
    } else {
        warn!(path = %cfg_path.display(), "config not found at path; using workspace defaults");
        load_default().context("load default config")?
    };

    // NATS.
    let nats_url = env::var("HEDGE_NATS_URL").unwrap_or_else(|_| DEFAULT_NATS_URL.to_string());
    let nats = match env::var("HEDGE_NATS_CREDS") {
        Ok(creds) if !creds.is_empty() => {
            info!(creds = %creds, "connecting to NATS with credentials");
            NatsClient::connect_with_creds(&nats_url, PathBuf::from(creds))
                .await
                .context("nats connect with creds")?
        }
        _ => {
            warn!("HEDGE_NATS_CREDS not set; connecting to NATS unauthenticated (dev only)");
            NatsClient::connect(&nats_url).await.context("nats connect")?
        }
    };

    // Redis (optional — engine still publishes to NATS without it).
    let redis_url = env::var("HEDGE_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string());
    let redis = match redis::Client::open(redis_url.clone()) {
        Ok(client) => match ConnectionManager::new(client).await {
            Ok(mgr) => Some(mgr),
            Err(err) => {
                warn!(error = %err, "redis connection-manager unavailable; continuing without Redis Streams");
                None
            }
        },
        Err(err) => {
            warn!(error = %err, "redis client URL invalid; continuing without Redis Streams");
            None
        }
    };

    let engine = match redis {
        Some(mgr) => SignalEngine::new_default(nats.clone()).with_redis(mgr),
        None => SignalEngine::new_default(nats.clone()),
    };

    // Subscribe to `feat.update.*` (wildcard subscription).
    let subject: Subject<RawBytes> = Subject::new("feat.update.*");
    let mut sub = nats
        .subscriber(subject, FlatBuffersCodec)
        .await
        .context("subscribe feat.update.*")?;

    info!("hedge-signals running; subscribed to feat.update.*");

    loop {
        match sub.recv_bytes().await {
            Ok(payload) => {
                let bytes = payload.as_ref();
                match decode_feature_snapshot(bytes) {
                    Some(snap) => {
                        if let Err(err) = engine.ingest_feature_snapshot(&snap).await {
                            warn!(error = %err, "ingest_feature_snapshot failed");
                        }
                    }
                    None => warn!(len = bytes.len(), "discarded malformed feature snapshot"),
                }
            }
            Err(err) => {
                warn!(error = %err, "subscription receive failed; exiting loop");
                break;
            }
        }
    }

    Ok(())
}

/// Decode a `FeatureSnapshot` from the wire layout produced by
/// `hedge_features::engine::encode`. Returns `None` on size mismatch.
///
/// Field order mirrors `schemas/features.fbs` (declaration order):
///
/// `correlation_id [16] | symbol u32 | vwap i64 | atr i64 | ema_fast i64 |
///  ema_slow i64 | ema_slope f32 | realized_vol f32 | momentum f32 |
///  rolling_delta i64 | liquidity_imbalance f32 | orderflow_strength f32 |
///  candle_structure u8 | breakout_pressure f32 | compression_zone f32 |
///  liquidity_sweep f32 | ts_ns u64`.
fn decode_feature_snapshot(bytes: &[u8]) -> Option<FeatureSnapshot> {
    const WIRE_SIZE: usize = 16 + 4 + 4 * 8 + 3 * 4 + 8 + 2 * 4 + 1 + 3 * 4 + 8;
    if bytes.len() != WIRE_SIZE {
        return None;
    }
    let mut offset = 0usize;
    let mut correlation_id = [0u8; 16];
    correlation_id.copy_from_slice(&bytes[offset..offset + 16]);
    offset += 16;
    let symbol = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let vwap = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let atr = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let ema_fast = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let ema_slow = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let ema_slope = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let realized_vol = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let momentum = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let rolling_delta = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let liquidity_imbalance = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let orderflow_strength = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let candle_structure = bytes[offset];
    offset += 1;
    let breakout_pressure = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let compression_zone = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let liquidity_sweep = f32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let ts_ns = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    debug_assert_eq!(offset, WIRE_SIZE);

    Some(FeatureSnapshot {
        correlation_id,
        symbol,
        vwap,
        atr,
        ema_fast,
        ema_slow,
        ema_slope,
        realized_vol,
        momentum,
        rolling_delta,
        liquidity_imbalance,
        orderflow_strength,
        candle_structure,
        breakout_pressure,
        compression_zone,
        liquidity_sweep,
        ts_ns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_features::FEATURE_WIRE_SIZE;

    #[test]
    fn decode_feature_snapshot_round_trips_empty_payload() {
        // Build a known feature snapshot, encode via hedge-features, decode here.
        let snap = FeatureSnapshot {
            correlation_id: [0xAAu8; 16],
            symbol: 7,
            vwap: 100_00,
            atr: 50,
            ema_fast: 100_05,
            ema_slow: 99_95,
            ema_slope: 0.5,
            realized_vol: 0.001,
            momentum: 0.01,
            rolling_delta: 5,
            liquidity_imbalance: 0.2,
            orderflow_strength: 0.3,
            candle_structure: 1,
            breakout_pressure: 0.7,
            compression_zone: 0.6,
            liquidity_sweep: 0.0,
            ts_ns: 12345,
        };
        let raw = hedge_features::encode(&snap);
        assert_eq!(raw.len(), FEATURE_WIRE_SIZE);
        let decoded = decode_feature_snapshot(raw.as_slice()).expect("decode");
        assert_eq!(decoded.correlation_id, snap.correlation_id);
        assert_eq!(decoded.symbol, snap.symbol);
        assert_eq!(decoded.vwap, snap.vwap);
        assert_eq!(decoded.atr, snap.atr);
        assert_eq!(decoded.ema_fast, snap.ema_fast);
        assert_eq!(decoded.ema_slow, snap.ema_slow);
        assert!((decoded.ema_slope - snap.ema_slope).abs() < f32::EPSILON);
        assert!((decoded.realized_vol - snap.realized_vol).abs() < f32::EPSILON);
        assert_eq!(decoded.candle_structure, snap.candle_structure);
        assert_eq!(decoded.ts_ns, snap.ts_ns);
    }

    #[test]
    fn decode_feature_snapshot_rejects_size_mismatch() {
        assert!(decode_feature_snapshot(&[]).is_none());
        assert!(decode_feature_snapshot(&[0u8; 16]).is_none());
        assert!(decode_feature_snapshot(&vec![0u8; FEATURE_WIRE_SIZE - 1]).is_none());
    }
}
