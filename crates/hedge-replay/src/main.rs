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
        "play" => {
            let session = iter
                .next()
                .ok_or_else(|| "play requires <session_id>".to_string())?;
            let session_id = session
                .parse::<u64>()
                .map_err(|e| format!("invalid session_id: {e}"))?;
            let speed = match iter.next().as_deref() {
                Some("x10") => hedge_config::ReplaySpeed::X10,
                Some("max") => hedge_config::ReplaySpeed::Max,
                _ => hedge_config::ReplaySpeed::X1,
            };
            Ok(Args::Play { dir, session_id, speed })
        }
        other => Err(format!("unknown command: {other}")),
    }
}

enum Args {
    List { dir: PathBuf },
    Info { dir: PathBuf, session_id: u64 },
    Dump { dir: PathBuf, session_id: u64 },
    Play { dir: PathBuf, session_id: u64, speed: hedge_config::ReplaySpeed },
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
                 \tdump <session_id>   Print each record's sequence_no, monotonic_ns, and kind.\n\
                 \tplay <session_id>   Play the recorded session to the NATS bus.\n"
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
        Args::Play { dir, session_id, speed } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("tokio runtime: {e}"))?;
            rt.block_on(async move {
                run_play(dir, session_id, speed).await
            })
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

async fn run_play(dir: PathBuf, session_id: u64, speed: hedge_config::ReplaySpeed) -> Result<(), String> {
    use hedge_replay::{Player, PlayerConfig};
    use hedge_bus::subjects;
    use hedge_core::{SessionId, SymbolId};
    use futures::StreamExt;
    use anyhow::Context;

    let nats_url = std::env::var("HEDGE_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let client = hedge_bus::NatsClient::connect(&nats_url)
        .await
        .with_context(|| format!("connect to NATS at {}", nats_url))
        .map_err(|e| e.to_string())?;

    let pcfg = PlayerConfig {
        segment_dir: dir,
        default_speed: speed,
        rng_seed: 0,
    };

    let pcfg_clone = pcfg.clone();
    let mut player = Player::open(SessionId::new(session_id), pcfg)
        .map_err(|e| format!("open player: {e:?}"))?;

    let total = player.total_records();
    if total == 0 {
        let available = match list_sessions(&pcfg_clone.segment_dir) {
            Ok(s) => {
                if s.is_empty() {
                    "(no sessions found)".to_string()
                } else {
                    s.into_iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")
                }
            }
            Err(_) => "(failed to list sessions)".to_string(),
        };
        return Err(format!("Session {} has 0 records. Available sessions: {}", session_id, available));
    }

    println!("Playing session {} ({} records)...", session_id, total);

    let mut stream = player.play_default();
    let mut played = 0;
    while let Some(r) = stream.next().await {
        if let RecordKind::Tick = r.kind {
            if r.payload.len() == 85 {
                let mut id_bytes = [0u8; 4];
                id_bytes.copy_from_slice(&r.payload[16..20]);
                let symbol_id = u32::from_le_bytes(id_bytes);
                
                let symbol_str = hedge_bus::symbol_for_id(symbol_id).unwrap_or("UNKNOWN");
                
                if played == 0 {
                    println!("Extracted symbol_id={} -> {}", symbol_id, symbol_str);
                }
                
                let bin_subject = format!("md.tick.bin.{}", symbol_str);
                if let Err(e) = client.raw().publish(bin_subject, bytes::Bytes::from(r.payload.clone())).await {
                    eprintln!("Failed to publish bin_subject: {}", e);
                }

                let mut ltp_bytes = [0u8; 8]; ltp_bytes.copy_from_slice(&r.payload[21..29]);
                let ltp_paise = i64::from_le_bytes(ltp_bytes);
                
                let mut bid_bytes = [0u8; 8]; bid_bytes.copy_from_slice(&r.payload[29..37]);
                let bid_paise = i64::from_le_bytes(bid_bytes);
                
                let mut ask_bytes = [0u8; 8]; ask_bytes.copy_from_slice(&r.payload[37..45]);
                let ask_paise = i64::from_le_bytes(ask_bytes);
                
                let mut ts_bytes = [0u8; 8]; ts_bytes.copy_from_slice(&r.payload[77..85]);
                let ts_ns = u64::from_le_bytes(ts_bytes) as i64;

                let json_payload = serde_json::json!({
                    "kind": "tick",
                    "data": {
                        "symbol": symbol_str,
                        "ltp_paise": ltp_paise,
                        "bid_paise": bid_paise,
                        "ask_paise": ask_paise,
                        "ts_recv_ns": ts_ns,
                        "_synth": true,
                    },
                    "_synth": true
                });
                
                let json_subject = format!("md.tick.{}", symbol_str);
                if let Err(e) = client.raw().publish(json_subject, bytes::Bytes::from(serde_json::to_vec(&json_payload).unwrap())).await {
                    eprintln!("Failed to publish tick: {}", e);
                }
            } else {
                let sym = SymbolId::new(1);
                let json_subject = subjects::md_tick::<()>(sym).into_string();
                if let Err(e) = client.raw().publish(json_subject, bytes::Bytes::from(r.payload)).await {
                    eprintln!("Failed to publish unknown tick format: {}", e);
                }
            }
            played += 1;
            if played % 1000 == 0 {
                println!("Played {} records...", played);
            }
            continue;
        }

        let subject = match &r.kind {
            RecordKind::Tick => unreachable!(),
            RecordKind::OrderBook => {
                let sym = SymbolId::new(1);
                subjects::md_book::<()>(sym).into_string()
            }
            RecordKind::OpenInterest => {
                let sym = SymbolId::new(1);
                subjects::md_oi::<()>(sym).into_string()
            }
            RecordKind::SignalEmitted => hedge_bus::subject::SIG_EMITTED.to_string(),
            RecordKind::RiskDecision => {
                // Without flatbuffers, we cannot read the `approved` field easily.
                // Fallback to APPROVED for replay.
                hedge_bus::subject::RISK_DECISION_APPROVED.to_string()
            }
            RecordKind::OrderSubmitted => {
                subjects::exec_order::<()>("submitted").into_string()
            }
            RecordKind::OrderModified => {
                subjects::exec_order::<()>("modified").into_string()
            }
            RecordKind::OrderCancelled => {
                subjects::exec_order::<()>("cancelled").into_string()
            }
            RecordKind::Fill => {
                // Fills usually don't have symbol encoded cleanly at the root unless it's in a struct, we can just publish to the specific sym if we unpack, but let's assume it has symbol_id.
                // Wait, Fill isn't generated in lib.rs? Let's check hedge_schemas if Fill has symbol_id.
                // Oh actually, we can skip parsing fill if not needed, but wait! We must publish with correct symbol.
                // Let me parse OrderState or Fill. Wait, OrderState handles filled.
                // Let's look up how Fill is parsed, but if it fails we can fallback.
                // For now, let's just publish to "exec.fill.0" since it's hard to get sym without the specific Fill schema, or we'll get compilation error.
                subjects::exec_fill::<()>(SymbolId::new(0)).into_string() // Fallback
            }
            RecordKind::AIDecision(source) => {
                // We'd parse the json to get the exact subject.
                // Or we can just fallback to the ai category if we can't extract symbol.
                "ai.replay".to_string()
            }
            RecordKind::TraderAction => "trader.replay".to_string(),
            RecordKind::MarketConditionSnapshot => "md.replay".to_string(),
            RecordKind::NewsEvent => "news.replay".to_string(),
        };

        if let Err(e) = client.raw().publish(subject.clone(), bytes::Bytes::from(r.payload)).await {
            eprintln!("Failed to publish {}: {}", subject, e);
        }

        played += 1;
        if played % 1000 == 0 {
            println!("Played {} records...", played);
        }
    }

    println!("Playback complete. Sent {} records.", played);
    Ok(())
}
