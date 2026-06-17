use serde_json::{json, Value};

/// Temporary shim to decode Hot_Path binary payloads into JSON (Task 4.x).
pub fn decode_payload_shim(subject: &str, bytes: &[u8]) -> Value {
    // If it's already JSON, just return it but wrapped in the right kind envelope if needed.
    if let Ok(v) = serde_json::from_slice::<Value>(bytes) {
        if subject.starts_with("of.heatmap.") {
            let symbol_str = subject.strip_prefix("of.heatmap.").unwrap_or("UNKNOWN");
            let cells = if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
                let mut out = Vec::new();
                for row in rows {
                    let bid_price = row.get("bid_price_paise").and_then(|p| p.as_i64()).unwrap_or(0);
                    let ask_price = row.get("ask_price_paise").and_then(|p| p.as_i64()).unwrap_or(0);
                    let bid_qty = row.get("bid_qty").and_then(|q| q.as_u64()).unwrap_or(0);
                    let ask_qty = row.get("ask_qty").and_then(|q| q.as_u64()).unwrap_or(0);

                    if bid_qty > 0 {
                        out.push(json!({ "price_paise": bid_price, "buy_qty": bid_qty, "sell_qty": 0 }));
                    }
                    if ask_qty > 0 {
                        out.push(json!({ "price_paise": ask_price, "buy_qty": 0, "sell_qty": ask_qty }));
                    }
                }
                out.sort_by_key(|c| c.get("price_paise").unwrap().as_i64().unwrap());
                out
            } else {
                Vec::new()
            };

            return json!({
                "kind": "heatmap",
                "data": {
                    "symbol": symbol_str,
                    "cells": cells,
                    "ts_ns": v.get("ts_ns").unwrap_or(&json!(0))
                }
            });
        }
        if subject.starts_with("of.event.") {
            return json!({ "kind": "event", "data": v });
        }
        // AI signals and news already have the right shape or don't need envelopes
        return v;
    }

    // Binary payload decoding
    if subject.starts_with("md.tick.") && bytes.len() == 77 {
        return decode_tick(bytes, subject);
    }

    if subject.starts_with("sig.emitted") && bytes.len() == 66 {
        return decode_signal(bytes);
    }

    // Default fallback
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    json!({ "_raw_b64": b64 })
}

fn decode_tick(bytes: &[u8], subject: &str) -> Value {
    let symbol_str = subject.strip_prefix("md.tick.").unwrap_or("UNKNOWN");
    let ltp = i64::from_le_bytes(bytes[21..29].try_into().unwrap());
    let bid = i64::from_le_bytes(bytes[29..37].try_into().unwrap());
    let ask = i64::from_le_bytes(bytes[37..45].try_into().unwrap());
    let ts_recv_ns = u64::from_le_bytes(bytes[69..77].try_into().unwrap());

    json!({
        "kind": "tick",
        "data": {
            "symbol": symbol_str,
            "ltp_paise": ltp,
            "bid_paise": bid,
            "ask_paise": ask,
            "ts_recv_ns": ts_recv_ns,
        }
    })
}

fn decode_signal(bytes: &[u8]) -> Value {
    let strategy = bytes[16];
    let symbol = u32::from_le_bytes(bytes[17..21].try_into().unwrap());
    let side = bytes[21];
    let conf = f32::from_le_bytes(bytes[26..30].try_into().unwrap());
    let ts_ns = u64::from_le_bytes(bytes[58..66].try_into().unwrap());

    json!({
        "kind": "signal",
        "data": {
            "strategy": strategy,
            "symbol": symbol,
            "side": side,
            "confidence": conf,
            "ts_ns": ts_ns
        }
    })
}
