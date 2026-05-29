//! Helper functions every generator uses.
//!
//! Most of these are tiny pure functions: `_synth` envelope wrappers,
//! ts_ns producers, and a few statistics derived from the rolling LTP
//! board.

use chrono::Utc;
use serde_json::{json, Value};

use crate::ltp_board::{LtpBoard, Quote};

/// Wall-clock nanoseconds since the Unix epoch.
#[inline]
pub fn now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// Wall-clock milliseconds since the Unix epoch.
#[inline]
pub fn now_ms() -> u64 {
    let ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    if ns < 0 {
        0
    } else {
        (ns as u64) / 1_000_000
    }
}

/// Tag any JSON value with `_synth: true` at the top level so downstream
/// suppression and the cockpit synth-badge selector can identify it.
#[inline]
pub fn synth_tag(mut v: Value) -> Value {
    if let Value::Object(ref mut map) = v {
        map.insert("_synth".to_string(), Value::Bool(true));
    }
    v
}

/// Construct a cockpit-shaped `MarketEvent::Tick` payload tagged as synth.
pub fn build_tick_envelope(symbol: &str, q: Quote) -> Value {
    synth_tag(json!({
        "kind": "tick",
        "data": {
            "symbol": symbol,
            "ltp_paise": q.ltp_paise,
            "bid_paise": q.bid_paise,
            "ask_paise": q.ask_paise,
            "ts_recv_ns": q.ts_ns,
        }
    }))
}

/// Construct a cockpit-shaped `MarketEvent::Book` payload tagged as synth.
pub fn build_book_envelope(symbol: &str, q: Quote, bid_qty: u64, ask_qty: u64) -> Value {
    synth_tag(json!({
        "kind": "book",
        "data": {
            "symbol": symbol,
            "bid_paise": q.bid_paise,
            "bid_qty": bid_qty,
            "ask_paise": q.ask_paise,
            "ask_qty": ask_qty,
            "ts_ns": q.ts_ns,
        }
    }))
}

/// Build the ticking quote for a fallback tick generator: starts from the
/// anchor (or last known LTP), takes a small Gaussian-ish step using the
/// provided RNG. Returns the new quote and persists it onto the board.
pub fn step_quote(
    board: &LtpBoard,
    symbol: &str,
    anchor_paise: i64,
    rng: &mut crate::rng::Mulberry32,
) -> Quote {
    let prev = board.get(symbol).map(|q| q.ltp_paise).unwrap_or(anchor_paise);
    // Step of ±0.05% biased to mean-revert toward the anchor.
    let bps = rng.range_f64(-5.0, 5.0); // 5 bp = 0.05%
    let revert = (anchor_paise - prev) as f64 * 0.0005;
    let delta_paise = ((prev as f64) * (bps / 10_000.0)) + revert;
    let ltp = (prev as f64 + delta_paise).round() as i64;
    let spread = ((prev / 2_000).max(5)) as i64; // ~0.05% spread, at least 5 paise
    let q = Quote {
        ltp_paise: ltp.max(1),
        bid_paise: (ltp - spread / 2).max(1),
        ask_paise: ltp + spread / 2,
        ts_ns: now_ns(),
    };
    board.set(symbol, q);
    q
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::{stream, Mulberry32};

    #[test]
    fn synth_tag_marks_object() {
        let v = synth_tag(json!({"a":1}));
        assert_eq!(v.get("_synth"), Some(&Value::Bool(true)));
        assert_eq!(v.get("a"), Some(&json!(1)));
    }

    #[test]
    fn tick_envelope_has_required_fields() {
        let q = Quote {
            ltp_paise: 100_00,
            bid_paise: 99_95,
            ask_paise: 100_05,
            ts_ns: 1234,
        };
        let v = build_tick_envelope("FOO", q);
        assert_eq!(v["kind"], "tick");
        assert_eq!(v["_synth"], true);
        assert_eq!(v["data"]["symbol"], "FOO");
        assert_eq!(v["data"]["ltp_paise"], 100_00);
        assert_eq!(v["data"]["bid_paise"], 99_95);
        assert_eq!(v["data"]["ask_paise"], 100_05);
        assert_eq!(v["data"]["ts_recv_ns"], 1234);
    }

    #[test]
    fn step_quote_progresses_and_records() {
        let board = LtpBoard::new();
        let mut rng = Mulberry32::for_stream(stream::TICK);
        let q1 = step_quote(&board, "FOO", 100_00, &mut rng);
        let q2 = step_quote(&board, "FOO", 100_00, &mut rng);
        assert!(q1.ltp_paise > 0);
        assert!(q2.ltp_paise > 0);
        // Bid <= LTP <= Ask is the only invariant that must hold.
        assert!(q1.bid_paise <= q1.ltp_paise && q1.ltp_paise <= q1.ask_paise);
    }
}
