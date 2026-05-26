//! Market_Data_Engine binary.
//!
//! Loads the workspace configuration, opens a NATS connection (with
//! optional credentials from `HEDGE_NATS_CREDS`), instantiates the engine
//! with the placeholder NSE adapter, and runs.
//!
//! In production the placeholder protocol parser is swapped for the
//! vendor binary frame; the engine plumbing remains unchanged. BSE and
//! options-chain adapters bolt on the same way once their upstream URLs
//! are configured.

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use hedge_bus::NatsClient;
use hedge_config::{load_default, load_from_path};
use hedge_market_data::{
    BreadthAggregator, Distributor, Exchange, LiveWsAdapter, MarketDataEngine,
    NseProtocolPlaceholder, SymbolInterner,
};
use hedge_obs::{init_metrics, tracer::NoopEmitter};
use tracing::{info, warn};

/// Default NATS URL when `HEDGE_NATS_URL` is unset.
const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

/// Default upstream WebSocket URL when `HEDGE_NSE_WS_URL` is unset.
///
/// The placeholder protocol parses the canonical JSON form, so any feed
/// that ships JSON ticks works unchanged. The default is unreachable so
/// `cargo run` without configuration fails closed instead of silently
/// connecting to the wrong endpoint.
const DEFAULT_NSE_URL: &str = "wss://example.invalid/nse/ticks";

#[tokio::main]
async fn main() -> Result<()> {
    // Logging. `tracing-subscriber` picks up `RUST_LOG` if set.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    // Metrics registry — idempotent.
    init_metrics().context("metrics init")?;

    // Configuration: prefer `/etc/hedge/config.yaml` if it exists, else fall
    // back to the workspace defaults. The Hot_Path pinning requirement
    // (R32 § Configuration) is enforced by the engine binary calling
    // `pinned::global().install(cfg)` at startup; we omit that here
    // because the SIGHUP-reload story is per-binary and outside the scope
    // of task 10.1.
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
        load_default()
    };

    // NATS connect. If `HEDGE_NATS_CREDS` is set, use credentials-based
    // connect for ACL enforcement (R21.3, R30.6); otherwise fall back to
    // the no-auth connector for local development.
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

    // Engine wiring.
    let interner = Arc::new(SymbolInterner::new());
    let distributor = Arc::new(Distributor::new());
    let emitter = Arc::new(NoopEmitter);

    let engine = MarketDataEngine::new(
        nats.clone(),
        Arc::clone(&interner),
        Arc::clone(&distributor),
        emitter,
    );

    // Dial the NSE upstream. The placeholder parser accepts any canonical
    // JSON tick; in production it is a binary parser bound to the actual
    // NSE TBT feed.
    let nse_url = env::var("HEDGE_NSE_WS_URL").unwrap_or_else(|_| DEFAULT_NSE_URL.to_string());
    info!(url = %nse_url, "dialing NSE upstream");

    let adapter = LiveWsAdapter::connect(
        engine.nats(),
        "nse_l1",
        nse_url.clone(),
        NseProtocolPlaceholder,
    )
    .await
    .context("initial NSE connect")?;

    // Empty sector map and prev_close map at startup; the
    // BreadthAggregator no-ops on unknown sectors and still recomputes
    // volatility from the live tick stream. Production deployments seed
    // these from the previous-day memory engine.
    let breadth = BreadthAggregator::new(HashMap::new(), HashMap::new());
    let _adapter_handle = engine.spawn_adapter(adapter, Exchange::Nse, breadth);

    info!("market_data engine running; awaiting traffic");
    // Park the runtime forever — adapter tasks do the work.
    std::future::pending::<()>().await;

    // Unreachable, but `main` requires a `Result` return shape.
    #[allow(unreachable_code)]
    Ok(())
}
