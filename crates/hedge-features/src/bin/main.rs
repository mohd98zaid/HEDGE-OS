//! Feature_Extraction_Engine binary.
//!
//! Loads the workspace config, opens NATS, subscribes to `md.tick.*`,
//! and runs the engine. Each incoming `Tick` is decoded from the wire,
//! folded into the per-symbol `FeatureState`, and re-emitted on
//! `feat.update.<sym>` with the per-stage latency record published on
//! `obs.latency.FeatureExtraction`.
//!
//! ### Decoding ticks on the receive path
//!
//! The Market_Data_Engine encodes `Tick` via the placeholder
//! `tick_to_raw_bytes` (a fixed-layout little-endian byte string of
//! every field in declaration order). We mirror the same layout here
//! so `feat.update.<sym>` is fed by the round-trip of the same wire
//! shape. Once the typed FlatBuffers codecs land in task 4.2, both
//! sides switch in lockstep without changing the engine code.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use hedge_bus::{FlatBuffersCodec, NatsClient, RawBytes, Subject};
use hedge_config::{load_default, load_from_path};
use hedge_features::FeatureExtractionEngine;
use hedge_obs::{init_metrics, tracer::NoopEmitter};
use hedge_schemas::Tick;
use tracing::{info, warn};

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    init_metrics().context("metrics init")?;

    // Configuration: prefer `/etc/hedge/config.yaml` if it exists, else
    // fall back to the workspace defaults.
    let cfg_path = env::var("HEDGE_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/etc/hedge/config.yaml"));
    let _cfg = if cfg_path.exists() {
        load_from_path(&cfg_path).context("load config from disk")?
    } else {
        warn!(
            path = %cfg_path.display(),
            "config not found at path; using workspace defaults",
        );
        load_default().context("load default config")?
    };

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

    let emitter = Arc::new(NoopEmitter);
    let engine = FeatureExtractionEngine::new(nats.clone(), emitter);

    // Subscribe to `md.tick.*` (wildcard subscription).
    let subject: Subject<RawBytes> = Subject::new("md.tick.*");
    let mut sub = nats
        .subscriber(subject, FlatBuffersCodec)
        .await
        .context("subscribe md.tick.*")?;

    info!("hedge-features running; subscribed to md.tick.*");

    loop {
        match sub.recv_bytes().await {
            Ok(payload) => {
                let bytes = payload.as_ref();
                match decode_tick(bytes) {
                    Some(tick) => {
                        if let Err(err) = engine.ingest_tick(&tick).await {
                            warn!(error = %err, "ingest_tick failed");
                        }
                    }
                    None => {
                        warn!(len = bytes.len(), "discarded malformed tick payload");
                    }
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

/// Decode a `Tick` from the wire layout produced by the
/// Market_Data_Engine's `tick_to_raw_bytes`. Returns `None` on size
/// mismatch.
///
/// Field order (declaration order in `schemas/tick.fbs`):
///
/// `correlation_id [16] | symbol u32 | exchange i8 | ltp_paise i64 |
///  bid_paise i64 | ask_paise i64 | ltq u64 | total_buy_qty u64 |
///  total_sell_qty u64 | ts_exchange_ns u64 | ts_recv_ns u64`
fn decode_tick(bytes: &[u8]) -> Option<Tick> {
    const TICK_WIRE_SIZE: usize = 16 + 4 + 1 + 8 * 8;
    if bytes.len() != TICK_WIRE_SIZE {
        return None;
    }
    let mut offset = 0usize;
    let mut correlation_id = [0u8; 16];
    correlation_id.copy_from_slice(&bytes[offset..offset + 16]);
    offset += 16;

    let symbol = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?);
    offset += 4;
    let exchange = bytes[offset] as i8;
    offset += 1;
    let ltp_paise = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let bid_paise = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let ask_paise = i64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let ltq = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let total_buy_qty = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let total_sell_qty = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let ts_exchange_ns = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let ts_recv_ns = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
    offset += 8;
    debug_assert_eq!(offset, TICK_WIRE_SIZE);

    Some(Tick {
        correlation_id,
        symbol,
        exchange,
        ltp_paise,
        bid_paise,
        ask_paise,
        ltq,
        total_buy_qty,
        total_sell_qty,
        ts_exchange_ns,
        ts_recv_ns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_tick_round_trips_fixed_layout() {
        // We rebuild a tick byte-for-byte the way the Market_Data_Engine
        // would and assert the decoder recovers the exact same struct.
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0xAAu8; 16]);
        buf.extend_from_slice(&7u32.to_le_bytes());
        buf.push(0i8 as u8);
        buf.extend_from_slice(&100_50i64.to_le_bytes());
        buf.extend_from_slice(&100_00i64.to_le_bytes());
        buf.extend_from_slice(&101_00i64.to_le_bytes());
        buf.extend_from_slice(&5u64.to_le_bytes());
        buf.extend_from_slice(&100u64.to_le_bytes());
        buf.extend_from_slice(&80u64.to_le_bytes());
        buf.extend_from_slice(&123u64.to_le_bytes());
        buf.extend_from_slice(&456u64.to_le_bytes());

        let t = decode_tick(&buf).expect("decode");
        assert_eq!(t.correlation_id, [0xAAu8; 16]);
        assert_eq!(t.symbol, 7);
        assert_eq!(t.exchange, 0);
        assert_eq!(t.ltp_paise, 100_50);
        assert_eq!(t.bid_paise, 100_00);
        assert_eq!(t.ask_paise, 101_00);
        assert_eq!(t.ltq, 5);
        assert_eq!(t.total_buy_qty, 100);
        assert_eq!(t.total_sell_qty, 80);
        assert_eq!(t.ts_exchange_ns, 123);
        assert_eq!(t.ts_recv_ns, 456);
    }

    #[test]
    fn decode_tick_rejects_size_mismatch() {
        assert!(decode_tick(&[0u8; 16]).is_none());
        assert!(decode_tick(&[]).is_none());
    }
}
