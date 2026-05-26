//! `replay-regression` — nightly replay-regression harness (task 59.1).
//!
//! Replays a recorded canonical session **twice** through
//! [`hedge_replay::Player`], collects the emitted record stream from
//! each run, and asserts:
//!
//! 1. Both runs return the same total record count.
//! 2. The two record sequences are byte-equal when serialised through
//!    [`hedge_replay::encode_record`] (this exercises the `Signal_v1`,
//!    `RiskDecision`, `OrderIntent_v1` / `OrderSubmitted`, and
//!    `OrderState_v1` / `Fill` / `OrderModified` / `OrderCancelled`
//!    payloads the design's Testing Strategy calls out).
//! 3. The per-[`RecordKind`] histograms are identical across runs.
//! 4. The deterministic [`Player::rng_mut`] stream is identical
//!    across runs given the same `--seed`.
//!
//! Any divergence exits the process with a non-zero status, so the
//! `.github/workflows/nightly.yml::replay-regression` job fails closed
//! per Property 12 (Replay Determinism).
//!
//! ```bash
//! # Default: ./tests/fixtures/replay/canonical, session 20251130
//! cargo run --bin replay-regression --release
//!
//! cargo run --bin replay-regression --release -- \
//!     --dir tests/fixtures/replay/canonical \
//!     --session-id 20251130 \
//!     --seed 42
//! ```

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use hedge_config::ReplaySpeed;
use hedge_core::SessionId;
use hedge_replay::{encode_record, AISource, Player, PlayerConfig, RecordKind, ReplayRecord};
use rand::RngCore;

const DEFAULT_DIR: &str = "tests/fixtures/replay/canonical";
const DEFAULT_SESSION_ID: u64 = 20251130;
const DEFAULT_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;
/// Number of RNG samples to compare across runs. Picked far above the
/// `ChaCha20` block size (1024 bytes ≙ 128 × `u64`) so any state
/// divergence shows up immediately.
const RNG_COMPARE_SAMPLES: usize = 1024;

struct Args {
    dir: PathBuf,
    session_id: u64,
    seed: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut iter = std::env::args().skip(1);
    let mut dir: Option<PathBuf> = None;
    let mut session_id: Option<u64> = None;
    let mut seed: Option<u64> = None;
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--dir" => {
                let v = iter
                    .next()
                    .ok_or_else(|| "--dir requires a value".to_string())?;
                dir = Some(PathBuf::from(v));
            }
            "--session-id" => {
                let v = iter
                    .next()
                    .ok_or_else(|| "--session-id requires a value".to_string())?;
                session_id = Some(
                    v.parse::<u64>()
                        .map_err(|e| format!("invalid --session-id: {e}"))?,
                );
            }
            "--seed" => {
                let v = iter
                    .next()
                    .ok_or_else(|| "--seed requires a value".to_string())?;
                seed = Some(
                    v.parse::<u64>()
                        .map_err(|e| format!("invalid --seed: {e}"))?,
                );
            }
            "-h" | "--help" => {
                println!(
                    "replay-regression — replay a canonical session twice \
                     and diff.\n\nUSAGE:\n    replay-regression [--dir <path>] \
                     [--session-id <u64>] [--seed <u64>]\n"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Args {
        dir: dir.unwrap_or_else(|| PathBuf::from(DEFAULT_DIR)),
        session_id: session_id.unwrap_or(DEFAULT_SESSION_ID),
        seed: seed.unwrap_or(DEFAULT_SEED),
    })
}

/// Stable string for a [`RecordKind`] so the histogram has
/// human-readable keys when CI prints it on failure.
fn kind_label(kind: &RecordKind) -> &'static str {
    match kind {
        RecordKind::Tick => "Tick",
        RecordKind::OrderBook => "OrderBook",
        RecordKind::OpenInterest => "OpenInterest",
        RecordKind::NewsEvent => "NewsEvent",
        RecordKind::SignalEmitted => "SignalEmitted",
        RecordKind::RiskDecision => "RiskDecision",
        RecordKind::OrderSubmitted => "OrderSubmitted",
        RecordKind::OrderModified => "OrderModified",
        RecordKind::OrderCancelled => "OrderCancelled",
        RecordKind::Fill => "Fill",
        RecordKind::TraderAction => "TraderAction",
        RecordKind::AIDecision(AISource::Ranking) => "AIDecision/Ranking",
        RecordKind::AIDecision(AISource::Regime) => "AIDecision/Regime",
        RecordKind::AIDecision(AISource::News) => "AIDecision/News",
        RecordKind::AIDecision(AISource::Psychology) => "AIDecision/Psychology",
        RecordKind::AIDecision(AISource::Priority) => "AIDecision/Priority",
        RecordKind::AIDecision(AISource::Journal) => "AIDecision/Journal",
        RecordKind::AIDecision(AISource::Governance) => "AIDecision/Governance",
        RecordKind::AIDecision(AISource::Other) => "AIDecision/Other",
        RecordKind::MarketConditionSnapshot => "MarketConditionSnapshot",
    }
}

/// One full play of the canonical session.
struct PlayResult {
    /// Records in `sequence_no` order.
    records: Vec<ReplayRecord>,
    /// Frequency of each `RecordKind` variant.
    histogram: BTreeMap<&'static str, u64>,
    /// First `RNG_COMPARE_SAMPLES` `u64` outputs of the seeded RNG.
    rng_samples: Vec<u64>,
}

fn play_once(
    dir: &std::path::Path,
    session_id: u64,
    seed: u64,
) -> Result<PlayResult, String> {
    let cfg = PlayerConfig {
        segment_dir: dir.to_path_buf(),
        default_speed: ReplaySpeed::Max,
        rng_seed: seed,
    };
    let mut player = Player::open(SessionId::new(session_id), cfg)
        .map_err(|e| format!("Player::open({session_id}) failed: {e}"))?;

    let total = player.total_records();
    let mut records: Vec<ReplayRecord> = Vec::with_capacity(total);
    let mut histogram: BTreeMap<&'static str, u64> = BTreeMap::new();

    while let Some(rec) = player.step() {
        *histogram.entry(kind_label(&rec.kind)).or_insert(0) += 1;
        records.push(rec);
    }

    if records.len() != total {
        return Err(format!(
            "player advertised {} records but step() yielded {}",
            total,
            records.len()
        ));
    }

    // Deterministic RNG comparison (Property 12 §3 — a stochastic
    // component pulling from `Player::rng_mut` must produce identical
    // streams across runs given the same seed).
    let mut rng_samples = Vec::with_capacity(RNG_COMPARE_SAMPLES);
    for _ in 0..RNG_COMPARE_SAMPLES {
        rng_samples.push(player.rng_mut().next_u64());
    }

    Ok(PlayResult {
        records,
        histogram,
        rng_samples,
    })
}

fn diff_streams(a: &PlayResult, b: &PlayResult) -> Result<usize, String> {
    if a.records.len() != b.records.len() {
        return Err(format!(
            "record-count divergence: run1={}, run2={}",
            a.records.len(),
            b.records.len()
        ));
    }
    if a.histogram != b.histogram {
        return Err(format!(
            "RecordKind histogram divergence:\n  run1: {:?}\n  run2: {:?}",
            a.histogram, b.histogram
        ));
    }
    if a.rng_samples != b.rng_samples {
        // Find the first divergent sample for a useful CI log.
        let first_div = a
            .rng_samples
            .iter()
            .zip(b.rng_samples.iter())
            .position(|(x, y)| x != y)
            .unwrap_or(usize::MAX);
        return Err(format!(
            "Player::rng_mut() stream divergence at sample {} \
             (run1=0x{:016x}, run2=0x{:016x})",
            first_div,
            a.rng_samples
                .get(first_div)
                .copied()
                .unwrap_or_default(),
            b.rng_samples
                .get(first_div)
                .copied()
                .unwrap_or_default(),
        ));
    }
    // Byte-level diff of the encoded record stream. Encoding through
    // `encode_record` exercises the same wire form the segment writer
    // produces, which means a divergence here implies a determinism
    // bug in the player or the recorder — both fail-closed.
    for (i, (r1, r2)) in a.records.iter().zip(b.records.iter()).enumerate() {
        if r1 != r2 {
            return Err(format!(
                "ReplayRecord divergence at sequence_no {i}:\n  run1: {r1:?}\n  run2: {r2:?}",
            ));
        }
        let e1 = encode_record(r1);
        let e2 = encode_record(r2);
        if e1 != e2 {
            return Err(format!(
                "encoded-bytes divergence at sequence_no {i} \
                 (run1.len={}, run2.len={})",
                e1.len(),
                e2.len()
            ));
        }
    }
    Ok(a.records.len())
}

fn run(args: Args) -> Result<(), String> {
    // Sanity: the canonical fixture must exist. We treat a missing
    // fixture as a hard failure rather than silently passing — the
    // nightly job's pre-flight is responsible for either committing
    // the fixture or regenerating it via `gen-canonical-replay`.
    let session_dir = args.dir.join(args.session_id.to_string());
    if !session_dir.exists() {
        return Err(format!(
            "canonical session directory does not exist: {} \
             (run `gen-canonical-replay --out {} --session-id {}` first)",
            session_dir.display(),
            args.dir.display(),
            args.session_id
        ));
    }

    println!(
        "==> replay-regression: dir={} session={} seed={:#x}",
        args.dir.display(),
        args.session_id,
        args.seed
    );

    let run1 = play_once(&args.dir, args.session_id, args.seed)?;
    println!(
        "    run1: {} records, histogram={:?}",
        run1.records.len(),
        run1.histogram
    );

    let run2 = play_once(&args.dir, args.session_id, args.seed)?;
    println!(
        "    run2: {} records, histogram={:?}",
        run2.records.len(),
        run2.histogram
    );

    let n = diff_streams(&run1, &run2)?;
    println!("OK: {n} records identical across both runs (byte-level + histogram + RNG)");
    Ok(())
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("FAIL: {e}");
            ExitCode::FAILURE
        }
    }
}
