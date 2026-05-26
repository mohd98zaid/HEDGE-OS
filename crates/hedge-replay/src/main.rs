//! `hedge-replay` — Replay_Engine binary entry point.
//!
//! Minimal CLI for inspecting a recorded session and stepping through
//! it. The full UI control plane is exposed via the `replay.command.*`
//! NATS subjects in [`hedge_replay::command`]; this binary is intended
//! for operator triage when the UI is not available.
//!
//! ```bash
//! hedge-replay list                # list every recorded session
//! hedge-replay info <session_id>   # show segment count / record count
//! hedge-replay dump <session_id>   # print each record's seq/kind to stdout
//! ```
//!
//! All commands take an optional `--dir <path>` flag (defaults to the
//! `hedge-config` `replay.segment_dir`).

use std::path::PathBuf;
use std::process::ExitCode;

use hedge_replay::{list_sessions, RecordKind, ReplayRecord, SegmentReader};

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let mut dir: Option<PathBuf> = None;
    let mut positional: Vec<String> = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--dir" => {
                let v = args
                    .next()
                    .ok_or_else(|| "--dir requires a value".to_string())?;
                dir = Some(PathBuf::from(v));
            }
            "-h" | "--help" => return Ok(Args::Help),
            _ => positional.push(a),
        }
    }
    let dir = dir.unwrap_or_else(|| {
        let cfg = hedge_config::load_default();
        PathBuf::from(cfg.replay.segment_dir)
    });
    let mut iter = positional.into_iter();
    let cmd = iter
        .next()
        .ok_or_else(|| "missing command (try `hedge-replay --help`)".to_string())?;
    match cmd.as_str() {
        "list" => Ok(Args::List { dir }),
        "info" => {
            let session = iter
                .next()
                .ok_or_else(|| "info requires <session_id>".to_string())?;
            let session_id = session
                .parse::<u64>()
                .map_err(|e| format!("invalid session_id: {e}"))?;
            Ok(Args::Info { dir, session_id })
        }
        "dump" => {
            let session = iter
                .next()
                .ok_or_else(|| "dump requires <session_id>".to_string())?;
            let session_id = session
                .parse::<u64>()
                .map_err(|e| format!("invalid session_id: {e}"))?;
            Ok(Args::Dump { dir, session_id })
        }
        other => Err(format!("unknown command: {other}")),
    }
}

enum Args {
    List { dir: PathBuf },
    Info { dir: PathBuf, session_id: u64 },
    Dump { dir: PathBuf, session_id: u64 },
    Help,
}

fn run(args: Args) -> Result<(), String> {
    match args {
        Args::Help => {
            println!(
                "hedge-replay — Replay_Engine inspection tool\n\
                 \n\
                 USAGE:\n\
                 \thedge-replay [--dir <path>] <command>\n\
                 \n\
                 COMMANDS:\n\
                 \tlist                List every recorded session.\n\
                 \tinfo <session_id>   Show segment count / record count.\n\
                 \tdump <session_id>   Print each record's sequence_no, monotonic_ns, and kind.\n"
            );
            Ok(())
        }
        Args::List { dir } => {
            let sessions =
                list_sessions(&dir).map_err(|e| format!("list sessions failed: {e}"))?;
            if sessions.is_empty() {
                println!("(no sessions recorded under {})", dir.display());
            } else {
                for s in sessions {
                    println!("{s}");
                }
            }
            Ok(())
        }
        Args::Info { dir, session_id } => {
            let reader = SegmentReader::open_session(&dir, session_id)
                .map_err(|e| format!("open session: {e}"))?;
            let segments = reader.segment_count();
            let all = reader
                .read_all()
                .map_err(|e| format!("read records: {e}"))?;
            println!("session_id    : {session_id}");
            println!("segments      : {segments}");
            println!("records       : {}", all.len());
            if let Some(first) = all.first() {
                println!("first_seq     : {}", first.sequence_no);
                println!("first_mono_ns : {}", first.monotonic_ns);
            }
            if let Some(last) = all.last() {
                println!("last_seq      : {}", last.sequence_no);
                println!("last_mono_ns  : {}", last.monotonic_ns);
            }
            Ok(())
        }
        Args::Dump { dir, session_id } => {
            let reader = SegmentReader::open_session(&dir, session_id)
                .map_err(|e| format!("open session: {e}"))?;
            let all = reader
                .read_all()
                .map_err(|e| format!("read records: {e}"))?;
            for r in all {
                println!(
                    "{:>8} {:>20} {} (payload: {} bytes)",
                    r.sequence_no,
                    r.monotonic_ns,
                    fmt_kind(&r),
                    r.payload.len()
                );
            }
            Ok(())
        }
    }
}

fn fmt_kind(r: &ReplayRecord) -> &'static str {
    match r.kind {
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
        RecordKind::AIDecision(_) => "AIDecision",
        RecordKind::MarketConditionSnapshot => "MarketConditionSnapshot",
    }
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
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
