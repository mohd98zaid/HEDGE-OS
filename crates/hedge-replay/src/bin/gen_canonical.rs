//! `gen-canonical-replay` — deterministic canonical-session generator.
//!
//! Writes a small synthetic recorded session under
//! `<out_dir>/<session_id>/seg-NNNN.rkyv` for the nightly replay
//! regression harness (task 59.1). The generator:
//!
//! * uses [`Recorder::record_raw`] so every timestamp comes from a
//!   deterministic counter rather than `now_ns()` / `Utc::now()`,
//! * emits a fixed sequence of [`RecordKind`] variants covering
//!   Signal_v1, RiskDecision, OrderIntent_v1 (`OrderSubmitted`), and
//!   OrderState_v1 transitions (`OrderModified`, `OrderCancelled`,
//!   `Fill`) — the four streams the design's Testing Strategy
//!   requires the regression to diff verbatim,
//! * pads each record with a deterministic payload so the on-disk
//!   bytes are reproducible across machines.
//!
//! ```bash
//! # Default: write to ./tests/fixtures/replay/canonical/, session_id 20251130
//! cargo run --bin gen-canonical-replay --release
//!
//! # Custom output directory and session id
//! cargo run --bin gen-canonical-replay --release -- \
//!     --out tests/fixtures/replay/canonical \
//!     --session-id 20251130 \
//!     --records 64
//! ```
//!
//! The output of two invocations with the same arguments is
//! byte-identical, which is what makes the replay-regression diff
//! meaningful (Property 12 — Replay Determinism).

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use hedge_core::SessionId;
use hedge_replay::{
    AISource, RecordKind, Recorder, RecorderConfig, ReplayRecord, DEFAULT_MAX_SEGMENT_BYTES,
};

const DEFAULT_OUT_DIR: &str = "tests/fixtures/replay/canonical";
const DEFAULT_SESSION_ID: u64 = 20251130;
const DEFAULT_RECORD_COUNT: u64 = 64;
/// Fixed monotonic anchor (5 ms after process start). Picked far from
/// zero so the wall-clock field, which is `i64` ns since epoch, has a
/// realistic-looking value when humans inspect the dump.
const ANCHOR_MONOTONIC_NS: u64 = 5_000_000;
/// Fixed wall-clock anchor: 2025-11-30 09:15:00.000 UTC, expressed in
/// nanoseconds since the Unix epoch. Picked to land inside an Indian
/// trading-session window when an operator inspects the dump.
const ANCHOR_WALLCLOCK_NS: i64 = 1_764_493_200_000_000_000;
/// Inter-record gap in nanoseconds (1 ms). Matches the 1 ms tick
/// cadence the recorder/player tests use elsewhere in the crate.
const TICK_GAP_NS: u64 = 1_000_000;

struct Args {
    out: PathBuf,
    session_id: u64,
    records: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut iter = std::env::args().skip(1);
    let mut out: Option<PathBuf> = None;
    let mut session_id: Option<u64> = None;
    let mut records: Option<u64> = None;
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--out" => {
                let v = iter
                    .next()
                    .ok_or_else(|| "--out requires a value".to_string())?;
                out = Some(PathBuf::from(v));
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
            "--records" => {
                let v = iter
                    .next()
                    .ok_or_else(|| "--records requires a value".to_string())?;
                records = Some(
                    v.parse::<u64>()
                        .map_err(|e| format!("invalid --records: {e}"))?,
                );
            }
            "-h" | "--help" => {
                println!(
                    "gen-canonical-replay — write a deterministic canonical \
                     session.\n\nUSAGE:\n    gen-canonical-replay [--out <path>] \
                     [--session-id <u64>] [--records <u64>]\n"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(Args {
        out: out.unwrap_or_else(|| PathBuf::from(DEFAULT_OUT_DIR)),
        session_id: session_id.unwrap_or(DEFAULT_SESSION_ID),
        records: records.unwrap_or(DEFAULT_RECORD_COUNT),
    })
}

/// Pick a [`RecordKind`] cyclically so the canonical session covers
/// every kind the regression diffs. Order is fixed across invocations
/// — that is what makes the fixture deterministic.
///
/// The cycle is picked to mirror a realistic Hot_Path round trip:
/// `Tick → Tick → SignalEmitted → RiskDecision → OrderSubmitted →
/// OrderModified → Fill → OrderCancelled → AIDecision(Ranking) →
/// MarketConditionSnapshot`. Ten variants per cycle gives every
/// emission at least 6 samples in a 64-record session.
fn kind_for_index(i: u64) -> RecordKind {
    match i % 10 {
        0 | 1 => RecordKind::Tick,
        2 => RecordKind::SignalEmitted,
        3 => RecordKind::RiskDecision,
        4 => RecordKind::OrderSubmitted,
        5 => RecordKind::OrderModified,
        6 => RecordKind::Fill,
        7 => RecordKind::OrderCancelled,
        8 => RecordKind::AIDecision(AISource::Ranking),
        _ => RecordKind::MarketConditionSnapshot,
    }
}

/// Deterministic payload: 16 bytes derived from the sequence number
/// and record kind discriminant. Cheap, reproducible, and easy to
/// inspect in a hex dump.
fn payload_for(seq: u64, kind: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&[kind; 4]);
    // Trailing 4 bytes: a tiny LCG so the bytes vary across the
    // payload but stay deterministic per sequence.
    let mut x = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ kind as u64;
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    buf.extend_from_slice(&(x as u32).to_be_bytes());
    debug_assert_eq!(buf.len(), 16);
    buf
}

fn kind_discriminant(kind: &RecordKind) -> u8 {
    match kind {
        RecordKind::Tick => 0,
        RecordKind::OrderBook => 1,
        RecordKind::OpenInterest => 2,
        RecordKind::NewsEvent => 3,
        RecordKind::SignalEmitted => 4,
        RecordKind::RiskDecision => 5,
        RecordKind::OrderSubmitted => 6,
        RecordKind::OrderModified => 7,
        RecordKind::OrderCancelled => 8,
        RecordKind::Fill => 9,
        RecordKind::TraderAction => 10,
        RecordKind::AIDecision(_) => 11,
        RecordKind::MarketConditionSnapshot => 12,
    }
}

async fn run(args: Args) -> Result<(), String> {
    // Make sure the parent directory exists. `Recorder::new` is lazy
    // and will create the per-session directory on first append, so
    // we only need to create the root.
    std::fs::create_dir_all(&args.out)
        .map_err(|e| format!("create out dir {}: {}", args.out.display(), e))?;

    // Wipe any existing files for this session id so a regeneration
    // is truly idempotent — the canonical fixture is supposed to be
    // exactly what this generator produces, nothing more.
    let session_dir = args.out.join(args.session_id.to_string());
    if session_dir.exists() {
        std::fs::remove_dir_all(&session_dir)
            .map_err(|e| format!("clear session dir {}: {}", session_dir.display(), e))?;
    }

    let cfg = RecorderConfig {
        segment_dir: args.out.clone(),
        max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
    };
    let session = SessionId::new(args.session_id);
    let mut recorder = Recorder::new(session, cfg);

    for i in 0..args.records {
        let kind = kind_for_index(i);
        let disc = kind_discriminant(&kind);
        let record = ReplayRecord {
            session_id: args.session_id,
            sequence_no: i,
            monotonic_ns: ANCHOR_MONOTONIC_NS + i * TICK_GAP_NS,
            wallclock_utc: ANCHOR_WALLCLOCK_NS + (i as i64) * (TICK_GAP_NS as i64),
            kind,
            payload: payload_for(i, disc),
        };
        recorder
            .record_raw(record)
            .await
            .map_err(|e| format!("record_raw seq {i}: {e}"))?;
    }
    recorder
        .flush()
        .map_err(|e| format!("flush recorder: {e}"))?;

    println!(
        "wrote {} records to {} (session {})",
        args.records,
        session_dir.display(),
        args.session_id
    );
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
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: build tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
