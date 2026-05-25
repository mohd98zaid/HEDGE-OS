//! `hedge-position` — Position_Engine binary entry point (task 16.1).
//!
//! At runtime this binary:
//!
//! 1. Initialises a JSON `tracing-subscriber` (full `hedge-obs` wiring is
//!    centralised in the session manager, task 43.1).
//! 2. Loads `hedge_config::HedgeConfig` from `/etc/hedge/config.yaml`
//!    (falling back to the in-tree defaults when the file is absent so
//!    development workflows keep working).
//! 3. Constructs a [`PositionEngine`](hedge_position::PositionEngine)
//!    seeded with `capital.base_inr × 100`.
//! 4. Will spawn two tokio tasks once the upstream producers are online:
//!    * **fill consumer** — reads the `hedge.hot.fills` Redis Stream
//!      consumer-group `position_engine` and folds each fill into the
//!      engine via `on_fill`. Per-fill latency must be ≤ 5 ms (R8.2).
//!    * **tick consumer** — subscribes to every `md.tick.*` NATS subject
//!      and folds ticks into the engine via `on_tick`. Throttled to ≤
//!      10 emissions/second/symbol per the task spec.
//!
//! The wire glue is deliberately thin so the engine stays unit-testable.
//! Today the binary boots the engine and idles — enough for
//! `cargo run -p hedge-position --bin hedge-position` to demonstrate
//! startup. The fully integrated end-to-end path is exercised by the
//! replay regression suite (task 47).

use anyhow::{Context, Result};
use hedge_config::{load_default, HedgeConfig};
use hedge_position::PositionEngine;
use tracing::{info, warn};

const SERVICE_NAME: &str = "hedge-position";

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Bare-bones JSON logging.
    tracing_subscriber::fmt::fmt()
        .json()
        .with_target(true)
        .try_init()
        .ok();

    // 2. Configuration. The full path-loading wiring is centralised in the
    //    session manager (task 43.1); the binary uses defaults when run
    //    standalone so `cargo run -p hedge-position` works without an
    //    `/etc/hedge/config.yaml`.
    let config: HedgeConfig = load_default();
    let base_capital_paise: i64 = i64::from(config.capital.base_inr) * 100;
    info!(
        target: SERVICE_NAME,
        base_capital_inr = config.capital.base_inr,
        "Position_Engine starting"
    );

    // 3. Engine.
    let _engine = PositionEngine::new(base_capital_paise);

    // 4. Wire integration deferred to task 17.x (broker_adapters bring up
    //    the fill stream) and task 10.x extension (live tick subscription).
    warn!(
        target: SERVICE_NAME,
        "wire integration (Redis fills consumer, NATS tick subscriber) deferred"
    );

    // Idle until the supervisor sends SIGTERM / Ctrl+C.
    tokio::signal::ctrl_c()
        .await
        .context("install ctrl_c handler")?;
    info!(target: SERVICE_NAME, "Position_Engine shutting down");
    Ok(())
}
