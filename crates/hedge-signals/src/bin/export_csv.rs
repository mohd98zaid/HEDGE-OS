use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use anyhow::{Context, Result};

use hedge_replay::SegmentReader;
use hedge_schemas::Tick;
use hedge_bus::symbol_for_id;
use chrono::{Local, TimeZone};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <SESSION_ID> [DIR]", args[0]);
        std::process::exit(1);
    }

    let session_id = args[1].parse::<u64>().context("Invalid session ID")?;
    let dir = args.get(2).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("./replay"));

    let reader = SegmentReader::open_session(dir, session_id).context("Failed to open session")?;
    let records = reader.read_all().context("Failed to read segments")?;

    let file = File::create("market_data_top15.csv")?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "timestamp,symbol,price,volume")?;

    for r in records {
        if let hedge_replay::RecordKind::Tick = r.kind {
            if r.payload.len() != 85 {
                continue;
            }
            let bytes = r.payload.as_slice();

            let symbol_id = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
            let ltp_paise = i64::from_le_bytes(bytes[21..29].try_into().unwrap());
            let ltq = u64::from_le_bytes(bytes[45..53].try_into().unwrap());
            let ts_exchange_ns = u64::from_le_bytes(bytes[69..77].try_into().unwrap());

            let sym_name = symbol_for_id(symbol_id).unwrap_or("");
            let allowed_stocks = [
                "Nifty 50", "Nifty Bank", "Nifty Next 50", "Nifty Fin Service", "Nifty 100",
                "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS",
                "ITC", "LT", "SBIN", "BHARTIARTL", "KOTAKBANK"
            ];
            if !allowed_stocks.iter().any(|&s| sym_name.contains(s)) {
                continue;
            }

            let price = ltp_paise as f64 / 100.0;
            let dt = Local.timestamp_opt((ts_exchange_ns / 1_000_000_000) as i64, 0).unwrap();
            let ts_str = dt.format("%Y-%m-%d %H:%M:%S").to_string();

            writeln!(writer, "{},{},{:.2},{}", ts_str, sym_name, price, ltq)?;
        }
    }

    writer.flush()?;
    println!("Exported to market_data_top15.csv successfully!");

    Ok(())
}
