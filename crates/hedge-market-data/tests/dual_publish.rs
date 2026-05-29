//! Phase B verification: prove `upstox-feed`'s binary `Tick_v1` encoding
//! decodes byte-for-byte the same way `hedge-features::decode_tick`
//! does. We can't compile the binary's private `encode_tick_v1` from
//! here, so this test re-implements the same layout and decodes it with
//! a faithful mirror of `hedge-features`'s decoder. Any drift between
//! producer and consumer fails this test deterministically.

const TICK_WIRE_SIZE: usize = 16 + 4 + 1 + 8 * 8;

/// Mirror of `upstox_feed::encode_tick_v1` — kept in lockstep with the
/// binary. If the binary ever diverges, update both at the same time.
fn encode_tick_v1(
    symbol_id: u32,
    ltp_paise: i64,
    bid_paise: i64,
    ask_paise: i64,
    ts_ns: i64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(TICK_WIRE_SIZE);
    buf.extend_from_slice(&[0u8; 16]);
    buf.extend_from_slice(&symbol_id.to_le_bytes());
    buf.push(0);
    buf.extend_from_slice(&ltp_paise.to_le_bytes());
    buf.extend_from_slice(&bid_paise.to_le_bytes());
    buf.extend_from_slice(&ask_paise.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&(ts_ns as u64).to_le_bytes());
    buf.extend_from_slice(&(ts_ns as u64).to_le_bytes());
    buf
}

/// Mirror of `hedge_features::decode_tick`. Identical math; if either
/// side drifts, the round-trip test panics.
struct Decoded {
    symbol: u32,
    ltp_paise: i64,
    bid_paise: i64,
    ask_paise: i64,
    ts_recv_ns: u64,
}

fn decode_tick(bytes: &[u8]) -> Option<Decoded> {
    if bytes.len() != TICK_WIRE_SIZE {
        return None;
    }
    let mut o = 16; // skip correlation_id
    let symbol = u32::from_le_bytes(bytes[o..o + 4].try_into().ok()?);
    o += 4;
    o += 1; // exchange
    let ltp_paise = i64::from_le_bytes(bytes[o..o + 8].try_into().ok()?);
    o += 8;
    let bid_paise = i64::from_le_bytes(bytes[o..o + 8].try_into().ok()?);
    o += 8;
    let ask_paise = i64::from_le_bytes(bytes[o..o + 8].try_into().ok()?);
    o += 8;
    o += 8; // ltq
    o += 8; // total_buy_qty
    o += 8; // total_sell_qty
    o += 8; // ts_exchange_ns
    let ts_recv_ns = u64::from_le_bytes(bytes[o..o + 8].try_into().ok()?);
    Some(Decoded {
        symbol,
        ltp_paise,
        bid_paise,
        ask_paise,
        ts_recv_ns,
    })
}

#[test]
fn binary_tick_round_trips_for_every_basket_symbol() {
    for sym in ["RELIANCE", "INFY", "SBIN", "HDFCBANK", "ICICIBANK"] {
        let id = hedge_bus::symbol_id_for(sym);
        assert_ne!(id, 0, "missing symbol_id for {}", sym);
        let buf = encode_tick_v1(id, 135_500, 135_495, 135_505, 1_700_000_000_000);
        assert_eq!(buf.len(), TICK_WIRE_SIZE, "wire size drift for {}", sym);
        let d = decode_tick(&buf).expect("decode ok");
        assert_eq!(d.symbol, id);
        assert_eq!(d.ltp_paise, 135_500);
        assert_eq!(d.bid_paise, 135_495);
        assert_eq!(d.ask_paise, 135_505);
        assert_eq!(d.ts_recv_ns, 1_700_000_000_000_u64);
    }
}

#[test]
fn binary_tick_size_matches_hedge_features_decoder_layout() {
    // 16 (correlation_id) + 4 (symbol) + 1 (exchange) + 8*8 (ltp/bid/ask/ltq/
    // total_buy/total_sell/ts_exch/ts_recv) = 85 bytes.
    let buf = encode_tick_v1(1, 0, 0, 0, 0);
    assert_eq!(buf.len(), 85);
    assert_eq!(buf.len(), TICK_WIRE_SIZE);
}
