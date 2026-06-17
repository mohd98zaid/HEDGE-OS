//! End-to-end synthetic trade lifecycle.
//!
//! For every `sig.emitted` event observed on the in-process [`SignalBus`],
//! this generator produces:
//!
//!   `ai.rank.<corr_id>`            (200–800 ms after signal)
//!   `risk.decision.approved` or `risk.decision.rejected`  (immediately after rank)
//!   `risk.cooldown.<sym>`          (sometimes)
//!   `exec.order.submitted`         (200–500 ms after approval)
//!   `exec.fill.<sym>` × N          (1–3 s later)
//!   `exec.order.filled` summary
//!   `pos.update.<sym>`             (per fill)
//!   `pos.risk_state` aggregate     (separate 1 Hz loop)
//!   `exec.trade.closed`            (when round-trip closes)
//!
//! Plus low-rate kill_switch + target_reached events from a separate
//! background loop.

use std::collections::HashMap;
use std::time::Duration;

use hedge_bus::NatsClient;
use serde_json::{json, Value};
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::debug;

use crate::derive::{now_ns, synth_tag};
use crate::rng::{stream, Mulberry32};
use crate::signal_bus::{SignalBus, SignalEvent};
use crate::suppression::SuppressionRegistry;

#[derive(Clone, Debug)]
struct OpenPosition {
    qty_signed: i64, // positive = long, negative = short
    avg_price_paise: i64,
}

pub async fn run(
    nats: NatsClient,
    suppression: SuppressionRegistry,
    bus: SignalBus,
) -> anyhow::Result<()> {
    // Background loops — kill switch / target reached / pos.risk_state.
    let nats_a = nats.clone();
    let sup_a = suppression.clone();
    let kill_handle =
        tokio::spawn(async move { run_kill_and_target(nats_a, sup_a).await });
    let nats_b = nats.clone();
    let sup_b = suppression.clone();
    let agg_handle =
        tokio::spawn(async move { run_portfolio_aggregate(nats_b, sup_b).await });

    let mut rng = Mulberry32::for_stream(stream::EXEC);
    let mut positions: HashMap<&'static str, OpenPosition> = HashMap::new();

    while let Some(sig) = bus.recv().await {
        let nats = nats.clone();
        let suppression = suppression.clone();
        // We process the lifecycle inline (not spawned) to keep RNG
        // determinism tight; sig arrival rate is far below 1 Hz so this
        // never queues up.
        process_signal(&nats, &suppression, &mut rng, &mut positions, sig).await?;
    }

    let _ = tokio::try_join!(kill_handle, agg_handle);
    Ok(())
}

async fn process_signal(
    nats: &NatsClient,
    suppression: &SuppressionRegistry,
    rng: &mut Mulberry32,
    positions: &mut HashMap<&'static str, OpenPosition>,
    sig: SignalEvent,
) -> anyhow::Result<()> {
    // 1. ai.rank.<corr_id>
    sleep(Duration::from_millis(rng.range_i64(200, 800) as u64)).await;
    let confidence = rng.range_f64(0.45, 0.95);
    let rank_subject = format!("ai.rank.{}", sig.correlation_id);
    if suppression.allow_publish(&rank_subject) {
        let payload = synth_tag(json!({
            "correlation_id": sig.correlation_id,
            "strategy": sig.strategy,
            "symbol": sig.symbol,
            "side": sig.side,
            "base_probability": sig.base_probability,
            "confidence": sig.confidence,
            "trade_confidence_score": confidence,
            "factors": {
                "orderflow": rng.range_f64(0.3, 0.95),
                "technical_strength": rng.range_f64(0.3, 0.95),
                "news_sentiment": rng.range_f64(0.2, 0.9),
                "market_regime": rng.range_f64(0.3, 0.85),
                "trader_discipline": rng.range_f64(0.5, 0.95),
            },
            "shadow": false,
            "explanation": format!(
                "synthetic rank for {}/{} ({:.0}% confidence)",
                sig.symbol, sig.strategy, confidence * 100.0
            ),
            "ts_ns": now_ns(),
        }));
        publish(nats, &rank_subject, &payload).await;
    }

    // 2. Risk decision: approve ~70%, reject ~30%.
    let approved = confidence > 0.55 && rng.next_f64() < 0.7;
    let qty = if approved {
        rng.range_i64(10, 100)
    } else {
        0
    };
    let risk_subject = if approved {
        "risk.decision.approved".to_string()
    } else {
        "risk.decision.rejected".to_string()
    };
    if suppression.allow_publish(&risk_subject) {
        let rationale = if approved {
            "approved"
        } else {
            ["below_priority_floor", "cooldown_active", "size_zero"]
                [rng.range_i64(0, 3) as usize]
        };
        let payload = synth_tag(json!({
            "kind": "decision",
            "data": {
                "correlation_id": sig.correlation_id,
                "approved": approved,
                "rationale_code": if approved { 0 } else { 1 + rng.range_i64(0, 5) },
                "rationale": rationale,
                "sized_quantity": qty,
                "ts_ns": now_ns(),
            }
        }));
        publish(nats, &risk_subject, &payload).await;
    }

    // 3. Cooldown sometimes after approval/rejection.
    if rng.next_f64() < 0.4 {
        let sub = format!("risk.cooldown.{}", sig.symbol);
        if suppression.allow_publish(&sub) {
            let payload = synth_tag(json!({
                "kind": "cooldown",
                "data": {
                    "symbol": sig.symbol,
                    "until_ts_ns": now_ns() + 30_000_000_000_i64,
                }
            }));
            publish(nats, &sub, &payload).await;
        }
    }

    if !approved {
        return Ok(());
    }

    // 4. Exec submit.
    sleep(Duration::from_millis(rng.range_i64(200, 500) as u64)).await;
    let broker_id = format!("UPSTOX-{:08x}", rng.next_u32());
    let exec_submit = "exec.order.submitted".to_string();
    if suppression.allow_publish(&exec_submit) {
        let payload = synth_tag(json!({
            "kind": "order",
            "data": {
                "correlation_id": sig.correlation_id,
                "broker_order_id": broker_id,
                "symbol": sig.symbol,
                "state": "Submitted",
                "filled_qty": 0,
                "ts_ns": now_ns(),
            }
        }));
        publish(nats, &exec_submit, &payload).await;
    }

    // 5. Fills (1–3 partials).
    let n_fills = rng.range_i64(1, 4);
    let mut filled: i64 = 0;
    let mut weighted_price_paise: i64 = 0;
    for i in 0..n_fills {
        sleep(Duration::from_millis(rng.range_i64(400, 1500) as u64)).await;
        let chunk = if i == n_fills - 1 {
            qty - filled
        } else {
            (qty / n_fills).max(1)
        };
        if chunk <= 0 {
            break;
        }
        let fill_price = sig.ltp_paise + rng.range_i64(-15, 15);
        weighted_price_paise += fill_price * chunk;
        filled += chunk;

        let fill_subject = format!("exec.fill.{}", sig.symbol);
        if suppression.allow_publish(&fill_subject) {
            let payload = synth_tag(json!({
                "kind": "fill",
                "data": {
                    "correlation_id": sig.correlation_id,
                    "broker_order_id": broker_id,
                    "symbol": sig.symbol,
                    "state": if filled >= qty { "Filled" } else { "PartiallyFilled" },
                    "filled_qty": filled,
                    "avg_fill_paise": weighted_price_paise / filled.max(1),
                    "ts_ns": now_ns(),
                }
            }));
            publish(nats, &fill_subject, &payload).await;
        }
    }

    // 6. Update synthetic position book and emit pos.update.<sym>.
    let avg_fill = weighted_price_paise / filled.max(1);
    let signed_qty = if sig.side == "buy" { filled } else { -filled };
    let pos = positions
        .entry(sig.symbol)
        .or_insert(OpenPosition { qty_signed: 0, avg_price_paise: avg_fill });
    let new_qty = pos.qty_signed + signed_qty;
    if new_qty != 0 && pos.qty_signed.signum() == new_qty.signum() {
        // adding to existing position — recompute weighted avg
        let prev_notional = pos.qty_signed.unsigned_abs() as i64 * pos.avg_price_paise;
        let added_notional = signed_qty.unsigned_abs() as i64 * avg_fill;
        pos.avg_price_paise =
            (prev_notional + added_notional) / new_qty.unsigned_abs().max(1) as i64;
    } else if new_qty == 0 || new_qty.signum() != pos.qty_signed.signum() {
        // closed or reversed — emit trade.closed
        let pnl_paise = (avg_fill - pos.avg_price_paise) * pos.qty_signed.signum()
            * pos.qty_signed.unsigned_abs() as i64;
        let closed_subject = "exec.trade.closed".to_string();
        if suppression.allow_publish(&closed_subject) {
            let payload = synth_tag(json!({
                "kind": "trade.closed",
                "data": {
                    "correlation_id": sig.correlation_id,
                    "symbol": sig.symbol,
                    "pnl_inr": (pnl_paise as f64) / 100.0,
                    "ts_ns": now_ns(),
                }
            }));
            publish(nats, &closed_subject, &payload).await;
        }
        pos.avg_price_paise = avg_fill;
    }
    pos.qty_signed = new_qty;

    let pos_subject = format!("pos.update.{}", sig.symbol);
    if suppression.allow_publish(&pos_subject) {
        let payload = synth_tag(json!({
            "kind": "pos.update",
            "data": {
                "symbol": sig.symbol,
                "quantity": pos.qty_signed,
                "avg_price_paise": pos.avg_price_paise,
                "realised_pnl_inr": 0,
                "unrealised_pnl_inr": 0,
                "ts_ns": now_ns(),
            }
        }));
        publish(nats, &pos_subject, &payload).await;
    }

    Ok(())
}

async fn run_kill_and_target(
    nats: NatsClient,
    suppression: SuppressionRegistry,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::RISK);
    loop {
        // 5–15 minutes between events.
        let gap = rng.range_i64(300_000, 900_000);
        sleep(Duration::from_millis(gap as u64)).await;
        let sub = if rng.next_f64() < 0.5 {
            "risk.killswitch.activated"
        } else {
            "risk.target.reached"
        };
        if !suppression.allow_publish(sub) {
            continue;
        }
        let payload = if sub == "risk.killswitch.activated" {
            synth_tag(json!({
                "kind": "killswitch",
                "data": {
                    "active": true,
                    "reason": "synthetic-stress-test",
                    "ts_ns": now_ns(),
                }
            }))
        } else {
            synth_tag(json!({
                "kind": "target.reached",
                "data": {
                    "ts_ns": now_ns(),
                    "pnl_inr": rng.range_f64(2_000.0, 10_000.0),
                }
            }))
        };
        publish(&nats, sub, &payload).await;
    }
}

async fn run_portfolio_aggregate(
    nats: NatsClient,
    suppression: SuppressionRegistry,
) -> anyhow::Result<()> {
    let mut rng = Mulberry32::for_stream(stream::POSITION);
    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let subject = "pos.risk_state".to_string();
    let mut pnl_drift: f64 = 0.0;
    loop {
        ticker.tick().await;
        if !suppression.allow_publish(&subject) {
            continue;
        }
        pnl_drift += rng.range_f64(-50.0, 50.0);
        let payload = synth_tag(json!({
            "kind": "pos.risk_state",
            "data": {
                "gross_exposure_inr": rng.range_f64(50_000.0, 200_000.0),
                "portfolio_pnl_inr": pnl_drift,
                "drawdown_inr": (pnl_drift.min(0.0)).abs(),
                "ts_ns": now_ns(),
            }
        }));
        publish(&nats, &subject, &payload).await;
    }
}

async fn publish(nats: &NatsClient, subject: &str, payload: &Value) {
    match serde_json::to_vec(payload) {
        Ok(bytes) => {
            if let Err(e) = nats.raw().publish(subject.to_string(), bytes.into()).await {
                debug!(subject = %subject, error = %e, "synth publish failed");
            }
        }
        Err(e) => debug!(subject = %subject, error = %e, "synth serialize failed"),
    }
}
