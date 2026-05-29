//! E2E smoke test: spin up the coordinator against a local NATS server
//! and assert that within 10 seconds at least one envelope arrives on
//! every cockpit-subscribed subject pattern (REQ-1.1, REQ-12.1).
//!
//! ### Requirements
//!
//! This test requires a NATS server on `127.0.0.1:4222`. The repository's
//! `start.bat` brings one up via Docker; if it isn't running the test
//! is skipped (rather than failing) so contributors without Docker can
//! still run the suite.

use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::time::timeout;

const NATS_URL: &str = "nats://127.0.0.1:4222";

/// Subjects we *require* to see at least one event from within the smoke
/// timeout. The rare-event family (`risk.killswitch.activated`,
/// `risk.target.reached`, `ai.psych.intervention`, `exec.broker.failover`,
/// `obs.budget.breach.*`) is intentionally excluded — those are
/// 5–15-minute-cadence Poisson events and would force a multi-minute
/// integration test for no real signal.
const SUBJECT_PATTERNS: &[&str] = &[
    "md.tick.>",
    "md.book.>",
    "md.oi.>",
    "md.breadth.sector",
    "md.breadth.volatility",
    "md.connection.>",
    "of.event.>",
    "of.heatmap.>",
    "feat.update.>",
    "ai.psych.stability",
    "pos.risk_state",
    "obs.latency.>",
    "ops.action.replay",
];

/// Smoke-test deadline. Every required subject in `SUBJECT_PATTERNS` is
/// emitted at ≥0.2 Hz cadence so 30 s is plenty.
const SMOKE_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synth_publishes_on_every_cockpit_subject() -> Result<()> {
    // Skip cleanly if NATS isn't reachable — keep the suite green for
    // contributors without docker compose up.
    let nats = match hedge_bus::NatsClient::connect(NATS_URL).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!(
                "[smoke] NATS unreachable at {}: {} — test skipped",
                NATS_URL, e
            );
            return Ok(());
        }
    };

    // 1. Subscribe BEFORE the synth starts so the first publishes are caught.
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    for pattern in SUBJECT_PATTERNS {
        let mut sub = nats.raw().subscribe(pattern.to_string()).await?;
        let tx = tx.clone();
        let pat = pattern.to_string();
        tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                let _ = tx.send(format!("{}|{}", pat, msg.subject));
            }
        });
    }
    drop(tx);

    // 2. Start the coordinator on a separate task. We use a fresh NATS
    //    client so the synth's suppression-subscriber sees its own subjects
    //    correctly. We hold onto the JoinHandle so panic-on-failure aborts
    //    the spawned task (the spawned task in turn kill_on_drops the
    //    child binary).
    let synth_nats = hedge_bus::NatsClient::connect(NATS_URL).await?;
    let synth_handle = tokio::spawn(async move {
        if let Err(e) = hedge_demo_synth_smoke::run_coordinator(synth_nats).await {
            eprintln!("[smoke] coordinator exited: {}", e);
        }
    });

    // 3. Wait for at least one event on every pattern.
    let target: std::collections::HashSet<&'static str> =
        SUBJECT_PATTERNS.iter().copied().collect();

    let recv = async move {
        let mut remaining = target;
        while !remaining.is_empty() {
            match rx.recv().await {
                Some(line) => {
                    if let Some((pat, _)) = line.split_once('|') {
                        remaining.remove(pat);
                    }
                }
                None => break,
            }
        }
        remaining
    };

    let leftover = match timeout(SMOKE_TIMEOUT, recv).await {
        Ok(set) => set,
        Err(_) => SUBJECT_PATTERNS.iter().copied().collect(),
    };

    if !leftover.is_empty() {
        synth_handle.abort();
        let mut leftover_vec: Vec<&str> = leftover.into_iter().collect();
        leftover_vec.sort();
        panic!(
            "synth did not publish on these subjects within {:?}: {:?}",
            SMOKE_TIMEOUT, leftover_vec
        );
    }
    synth_handle.abort();
    Ok(())
}

/// Small re-exposure shim because integration tests cannot directly call
/// the binary's private `coordinator::run`. The shim lives in `lib.rs` of
/// a tiny helper module beside the binary; if the binary stays
/// bin-only, the test driver re-implements a minimal coordinator by
/// spawning the binary as a child process. For now we keep the same crate
/// graph by adding a `[lib]` re-export module.
mod hedge_demo_synth_smoke {
    use anyhow::Result;
    use hedge_bus::NatsClient;

    pub async fn run_coordinator(nats: NatsClient) -> Result<()> {
        let _ = nats;
        // Locate the synth binary. Tests can't depend on
        // `CARGO_BIN_EXE_hedge-demo-synth` (only set for crates that
        // declare both `[[bin]]` and `[[test]]` and we may run via
        // `cargo test --test`, which doesn't always populate it).
        // Use the deterministic release path the workspace builds to.
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let candidates = [
            format!("{}/../../target/release/hedge-demo-synth.exe", manifest_dir),
            format!("{}/../../target/release/hedge-demo-synth", manifest_dir),
        ];
        let exe = candidates
            .iter()
            .find(|p| std::path::Path::new(p).exists())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "synth binary not found; run `cargo build --release -p hedge-demo-synth` first"
                )
            })?
            .clone();
        let mut child = tokio::process::Command::new(exe)
            .env("HEDGE_DEMO_SYNTH", "on")
            .env("HEDGE_NATS_URL", super::NATS_URL)
            .env("RUST_LOG", "warn")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| anyhow::anyhow!("spawn synth: {}", e))?;
        let _ = child.wait().await;
        Ok(())
    }
}
