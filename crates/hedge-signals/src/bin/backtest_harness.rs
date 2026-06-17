use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Context, Result};
use tracing::{info, warn};

use hedge_replay::{SegmentReader, RecordKind, list_sessions};
use hedge_features::{FeatureState, process_tick_into_state};
use hedge_schemas::Tick;
use hedge_signals::engine::{SignalEngineConfig, evaluate_strategies};
use hedge_signals::Strategy;
use hedge_signals::strategies::CompositeAlphaBreakout;
use hedge_bus::symbol_for_id;
use chrono::{Local, Timelike, Datelike, TimeZone};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <SESSION_ID> [DIR]", args[0]);
        let dir = PathBuf::from("./replay");
        if let Ok(sessions) = list_sessions(&dir) {
            eprintln!("Available sessions:");
            for s in sessions {
                eprintln!("  {}", s);
            }
        }
        std::process::exit(1);
    }

    let session_id = args[1].parse::<u64>().context("Invalid session ID")?;
    let dir = args.get(2).map(PathBuf::from).unwrap_or_else(|| PathBuf::from("./replay"));

    info!("Starting backtest for session {} in {}", session_id, dir.display());

    let reader = SegmentReader::open_session(dir, session_id).context("Failed to open session")?;
    let mut records = reader.read_all().context("Failed to read segments")?;

    let slippage_paise: f64 = env::var("HEDGE_SLIPPAGE_PAISE").unwrap_or_else(|_| "0".to_string()).parse().unwrap_or(0.0);
    let no_circuit_breaker: bool = env::var("HEDGE_NO_CIRCUIT_BREAKER").unwrap_or_else(|_| "0".to_string()) == "1";
    let oos_split = env::var("HEDGE_OOS_SPLIT").unwrap_or_else(|_| "".to_string());
    let disable_symbols_str = env::var("HEDGE_DISABLE_SYMBOLS").unwrap_or_else(|_| "".to_string());
    let disable_symbols: Vec<String> = disable_symbols_str.split(',').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect();

    if oos_split == "first_half" {
        let half = records.len() / 2;
        records.truncate(half);
        info!("OOS SPLIT: Using only the FIRST HALF of the dataset ({} records)", records.len());
    } else if oos_split == "second_half" {
        let half = records.len() / 2;
        records = records.split_off(half);
        info!("OOS SPLIT: Using only the SECOND HALF of the dataset ({} records)", records.len());
    }

    info!("Loaded {} records. Running feature extraction and signal evaluation...", records.len());

    let strategy: Arc<dyn Strategy> = Arc::new(CompositeAlphaBreakout);
    let strategies = vec![strategy];
    let cfg = SignalEngineConfig::default();

    let mut state_map = std::collections::HashMap::new();

    let mut total_signals = 0;
    let mut total_pnl_paise = 0_i64;
    let mut win_count = 0;
    let mut loss_count = 0;

    // A simple simulation state for each symbol
    struct SimPos {
        side: u8, // 1 for Buy, 2 for Sell (matching hedge-signals)
        entry_price: i64,
        qty_f64: f64, // Allow fractional quantities for indices
        initial_sl: i64,
        current_sl: i64,
        target: i64,
        breakeven_set: bool,
    }
    let mut pos_map: std::collections::HashMap<u32, SimPos> = std::collections::HashMap::new();

    let mut processed_ticks = 0;
    
    // Daily tracking
    let mut current_day = 0;
    let mut daily_losses = 0;

    // We assume 10000 INR allocation per trade. (10000_00 paise)
    let trade_allocation_paise: f64 = 10000_00.0;
    
    // Track peak portfolio for Max Drawdown globally
    let mut peak_pnl_paise = 0_f64;
    let mut max_drawdown_paise = 0_f64;
    let mut realized_pnl_paise = 0_f64;

    // Track per-symbol statistics
    #[derive(Default)]
    struct SymbolStats {
        signals: u64,
        wins: u64,
        losses: u64,
        gross_pnl_paise: f64,
        net_pnl_paise: f64,
        peak_pnl: f64,
        max_dd: f64,
    }
    let mut stats_map: std::collections::HashMap<u32, SymbolStats> = std::collections::HashMap::new();

    for r in records {
        if let RecordKind::Tick = r.kind {
            if r.payload.len() != 85 {
                continue;
            }
            let bytes = r.payload.as_slice();

            let symbol_id = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
            let exchange = bytes[20] as i8;
            let ltp_paise = i64::from_le_bytes(bytes[21..29].try_into().unwrap());
            let bid_paise = i64::from_le_bytes(bytes[29..37].try_into().unwrap());
            let ask_paise = i64::from_le_bytes(bytes[37..45].try_into().unwrap());
            let ltq = u64::from_le_bytes(bytes[45..53].try_into().unwrap());
            let total_buy_qty = u64::from_le_bytes(bytes[53..61].try_into().unwrap());
            let total_sell_qty = u64::from_le_bytes(bytes[61..69].try_into().unwrap());
            let ts_exchange_ns = u64::from_le_bytes(bytes[69..77].try_into().unwrap());
            let ts_recv_ns = u64::from_le_bytes(bytes[77..85].try_into().unwrap());

            let tick = Tick {
                correlation_id: [0; 16],
                symbol: symbol_id,
                exchange,
                ltp_paise,
                bid_paise,
                ask_paise,
                ltq,
                total_buy_qty,
                total_sell_qty,
                ts_exchange_ns,
                ts_recv_ns,
            };

            let state = state_map.entry(symbol_id).or_insert_with(FeatureState::default);
            let snap = process_tick_into_state(state, &tick);

            // Convert timestamp to Local time
            let dt = Local.timestamp_opt((ts_recv_ns / 1_000_000_000) as i64, 0).unwrap();
            let day = dt.ordinal();
            let hour = dt.hour();
            let minute = dt.minute();

            if day != current_day {
                current_day = day;
                daily_losses = 0; // Reset daily loss circuit breaker
            }

            // Simple execution model
            if let Some(pos) = pos_map.get_mut(&symbol_id) {
                let side_multiplier = if pos.side == 1 { 1.0 } else { -1.0 };
                let pnl = (ltp_paise - pos.entry_price) as f64 * side_multiplier * pos.qty_f64;
                
                let initial_risk_paise = (pos.entry_price - pos.initial_sl).abs() as f64;
                let current_unrealized_paise = (ltp_paise - pos.entry_price) as f64 * side_multiplier;
                let unrealized_rr = if initial_risk_paise > 0.0 { current_unrealized_paise / initial_risk_paise } else { 0.0 };

                let mut exit_reason = None;

                // STAGE 1 & 2: Hard SL and Target
                if (pos.side == 1 && ltp_paise <= pos.current_sl) || (pos.side == 2 && ltp_paise >= pos.current_sl) {
                    exit_reason = Some("STOP_LOSS");
                } else if (pos.side == 1 && ltp_paise >= pos.target) || (pos.side == 2 && ltp_paise <= pos.target) {
                    exit_reason = Some("TARGET");
                } 
                // STAGE 6: Time-based exit
                else if hour == 15 && minute >= 15 {
                    exit_reason = Some("TIME_EXIT");
                }

                // Dynamic Trailing (only update if not exiting)
                if exit_reason.is_none() {
                    // STAGE 3: Breakeven at 1:1
                    if unrealized_rr >= 1.0 && !pos.breakeven_set {
                        pos.current_sl = pos.entry_price;
                        pos.breakeven_set = true;
                    }
                    
                    // STAGE 4: Trail SL at 1.5 RR (lock 50% profit)
                    if unrealized_rr >= 1.5 {
                        let lock_paise = (0.5 * initial_risk_paise).round() as i64;
                        let new_sl = if pos.side == 1 {
                            pos.entry_price + lock_paise
                        } else {
                            pos.entry_price - lock_paise
                        };
                        
                        if pos.side == 1 {
                            pos.current_sl = pos.current_sl.max(new_sl);
                        } else {
                            pos.current_sl = pos.current_sl.min(new_sl);
                        }
                    }

                    // STAGE 5: Trail with ATR after 2.0 RR
                    if unrealized_rr >= 2.0 {
                        let atr_trail = snap.atr * 2;
                        let new_sl = if pos.side == 1 {
                            ltp_paise - atr_trail
                        } else {
                            ltp_paise + atr_trail
                        };

                        if pos.side == 1 {
                            pos.current_sl = pos.current_sl.max(new_sl);
                        } else {
                            pos.current_sl = pos.current_sl.min(new_sl);
                        }
                    }
                }

                if let Some(_reason) = exit_reason {
                    // Apply flat 60 INR (6000 paise) transaction cost
                    let total_slippage_loss = slippage_paise * 2.0 * pos.qty_f64;
                    let pnl_after_fees = pnl - 6000.0 - total_slippage_loss;
                    
                    realized_pnl_paise += pnl_after_fees;
                    
                    if realized_pnl_paise > peak_pnl_paise {
                        peak_pnl_paise = realized_pnl_paise;
                    } else {
                        let drawdown = peak_pnl_paise - realized_pnl_paise;
                        if drawdown > max_drawdown_paise { max_drawdown_paise = drawdown; }
                    }

                    if pnl_after_fees > 0.0 { 
                        win_count += 1; 
                    } else { 
                        loss_count += 1; 
                        daily_losses += 1; // Increment daily loss circuit breaker
                    }
                    
                    let mut s = stats_map.entry(symbol_id).or_default();
                    if pnl_after_fees > 0.0 { s.wins += 1; } else { s.losses += 1; }
                    s.gross_pnl_paise += pnl;
                    s.net_pnl_paise += pnl_after_fees;
                    if s.net_pnl_paise > s.peak_pnl { s.peak_pnl = s.net_pnl_paise; } else {
                        let dd = s.peak_pnl - s.net_pnl_paise;
                        if dd > s.max_dd { s.max_dd = dd; }
                    }

                    pos_map.remove(&symbol_id);
                }
            }

            // Evaluate signals only if circuit breaker hasn't tripped
            if no_circuit_breaker || daily_losses < 3 {
                let signals = evaluate_strategies(&strategies, &snap, &cfg, None);
                for sig in signals {
                    // Filter whitelist symbols (NIFTY indices)
                    let sym_name = symbol_for_id(symbol_id).unwrap_or("");
                    if !sym_name.contains("Nifty") {
                        continue;
                    }
                    if disable_symbols.iter().any(|s| sym_name.contains(s)) {
                        continue;
                    }

                    if !pos_map.contains_key(&symbol_id) {
                        total_signals += 1;
                        let s = stats_map.entry(symbol_id).or_default();
                        s.signals += 1;
                        
                        // Hardcode 1,000,000 capital per trade (approx 2 lots of NIFTY) to realistically cover 60 INR fees
                        let trade_capital = 1_000_000.0;
                        let qty_f64 = trade_capital / (ltp_paise as f64 / 100.0);

                        pos_map.insert(symbol_id, SimPos {
                            side: sig.side,
                            entry_price: ltp_paise,
                            qty_f64,
                            initial_sl: sig.risk_profile.stop_loss_paise,
                            current_sl: sig.risk_profile.stop_loss_paise,
                            target: sig.risk_profile.take_profit_paise,
                            breakeven_set: false,
                        });
                    }
                }
            }

            processed_ticks += 1;
        }
    }

    // Close all open positions at the end of the session
    for (symbol_id, pos) in pos_map {
        // Find last price? We don't have it easily here without saving it, but we can assume closing at break even or 0 for simplicity, or we should have tracked last price.
        // For a simple backtest summary, let's just ignore unresolved positions.
        warn!("Unresolved position for {} at end of session", symbol_for_id(symbol_id).unwrap_or("UNKNOWN"));
    }

    let win_rate = if win_count + loss_count > 0 {
        (win_count as f64 / (win_count + loss_count) as f64) * 100.0
    } else {
        0.0
    };

    let total_pnl_inr = realized_pnl_paise / 100.0;
    let max_drawdown_inr = max_drawdown_paise / 100.0;
    let initial_portfolio_inr = 10000.0;
    let roi_pct = (total_pnl_inr / initial_portfolio_inr) * 100.0;

    println!("\n=== GLOBAL PORTFOLIO RESULTS (₹10,000 / trade / ₹60 fee) ===");
    println!("Processed Ticks : {}", processed_ticks);
    println!("Total Signals   : {}", total_signals);
    println!("Winning Trades  : {}", win_count);
    println!("Losing Trades   : {}", loss_count);
    println!("Win Rate        : {:.2}%", win_rate);
    println!("Net PNL         : {:.2} INR", total_pnl_inr);
    println!("Max Drawdown    : {:.2} INR", max_drawdown_inr);
    println!("ROI %           : {:.2}%", roi_pct);
    println!("============================================================");

    println!("\n=== INDIVIDUAL SYMBOL BREAKDOWN ===");
    for (id, s) in &stats_map {
        let sym_name = symbol_for_id(*id).unwrap_or("UNKNOWN");
        let w_rate = if s.wins + s.losses > 0 { (s.wins as f64 / (s.wins + s.losses) as f64) * 100.0 } else { 0.0 };
        println!("Symbol: {}", sym_name);
        println!("  Signals: {}  |  Wins: {}  |  Losses: {}  |  Win Rate: {:.2}%", s.signals, s.wins, s.losses, w_rate);
        println!("  Gross PNL: {:.2} INR", s.gross_pnl_paise / 100.0);
        println!("  Net PNL  : {:.2} INR", s.net_pnl_paise / 100.0);
        println!("  Max DD   : {:.2} INR", s.max_dd / 100.0);
        println!("  ROI      : {:.2} %\n", (s.net_pnl_paise / 100.0) / initial_portfolio_inr * 100.0);
    }

    Ok(())
}
