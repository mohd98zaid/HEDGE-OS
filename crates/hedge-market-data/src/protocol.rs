//! Protocol parsers for upstream market data feeds.
//!
//! In production each Indian broker / exchange feed ships its own binary
//! wire format (NSE TBT, BSE EOBI, etc.). The shape we care about for the
//! Hot_Path is identical across feeds — bid/ask, last traded price, last
//! traded quantity, total buy/sell quantity, exchange timestamp — so this
//! crate exposes a single [`MarketDataProtocol`] trait that production
//! parsers will implement once vendor SDKs are integrated.
//!
//! The placeholder implementations below all parse the **canonical JSON**
//! development form that we use for replay scenarios and unit tests:
//!
//! ```json
//! {
//!   "symbol": "RELIANCE",
//!   "ltp": 250000,
//!   "bid": 249950,
//!   "ask": 250050,
//!   "ltq": 100,
//!   "total_buy_qty": 5000,
//!   "total_sell_qty": 4500,
//!   "ts_exchange_ms": 1700000000000
//! }
//! ```
//!
//! Prices are integers in **paise** (₹/100). The schema is identical for
//! NSE, BSE, and the options-chain placeholder; the three implementations
//! exist so that consumers can wire one [`crate::adapter::LiveWsAdapter`]
//! per upstream feed today and swap each parser for its production
//! binary equivalent later without touching the engine plumbing.

use serde::Deserialize;

use crate::error::MarketDataError;

/// Raw, pre-normalization tick payload returned by every protocol parser.
///
/// `RawTick` is intentionally the *minimum* set of fields shared across
/// every Indian-market feed we will integrate. The [`crate::normalizer::TickNormalizer`]
/// converts this into the FlatBuffers `Tick_v1` wire form by:
///
/// * resolving `symbol` to a [`hedge_core::SymbolId`] via the symbol interner,
/// * stamping `ts_recv_ns = hedge_core::now_ns()`,
/// * minting a fresh per-tick [`hedge_core::CorrelationId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTick {
    /// Exchange ticker symbol as a UTF-8 string (e.g. `"RELIANCE"`).
    pub symbol: String,
    /// Last traded price, in paise.
    pub ltp_paise: i64,
    /// Best bid price, in paise.
    pub bid_paise: i64,
    /// Best ask price, in paise.
    pub ask_paise: i64,
    /// Last traded quantity (shares / contracts).
    pub ltq: u64,
    /// Aggregate buy quantity across all visible levels.
    pub total_buy_qty: u64,
    /// Aggregate sell quantity across all visible levels.
    pub total_sell_qty: u64,
    /// Exchange-side timestamp in milliseconds since the epoch.
    pub ts_exchange_ms: u64,
}

/// Canonical JSON wire form used by every placeholder parser.
///
/// The Hot_Path normalizer never sees this struct — it is internal to the
/// `serde_json` decode step. Keeping it as a separate type lets us add
/// production-only fields later without polluting [`RawTick`].
#[derive(Debug, Deserialize)]
struct CanonicalJsonTick {
    symbol: String,
    ltp: i64,
    bid: i64,
    ask: i64,
    ltq: u64,
    total_buy_qty: u64,
    total_sell_qty: u64,
    ts_exchange_ms: u64,
}

impl From<CanonicalJsonTick> for RawTick {
    fn from(j: CanonicalJsonTick) -> Self {
        Self {
            symbol: j.symbol,
            ltp_paise: j.ltp,
            bid_paise: j.bid,
            ask_paise: j.ask,
            ltq: j.ltq,
            total_buy_qty: j.total_buy_qty,
            total_sell_qty: j.total_sell_qty,
            ts_exchange_ms: j.ts_exchange_ms,
        }
    }
}

/// Vendor-agnostic parser for a single tick payload.
///
/// Implementations are stateless and `Send + Sync` so a single parser handle
/// can be cloned cheaply into per-symbol adapter tasks.
pub trait MarketDataProtocol: Send + Sync {
    /// Parse a raw wire payload into a [`RawTick`].
    fn parse(&self, raw: &[u8]) -> Result<RawTick, MarketDataError>;

    /// Stable name used in metrics labels, structured logs, and error
    /// messages.
    fn name(&self) -> &'static str;
}

/// Helper used by every placeholder parser. Parses the canonical JSON form
/// and tags any error with the implementation's [`MarketDataProtocol::name`].
fn parse_canonical_json(
    protocol_name: &'static str,
    raw: &[u8],
) -> Result<RawTick, MarketDataError> {
    let parsed: CanonicalJsonTick = serde_json::from_slice(raw)
        .map_err(|e| MarketDataError::parse(protocol_name, e))?;
    Ok(parsed.into())
}

/// Placeholder NSE tick protocol parser.
///
/// In production this would decode the NSE TBT (tick-by-tick) binary frame
/// into a `RawTick`. Today it accepts the canonical JSON form so the
/// engine plumbing can be exercised end-to-end without a vendor SDK.
///
/// TODO: production protocol — replace with vendor-specific binary parser.
/// See: <https://www.nseindia.com/all-reports#cm_circulars> (TBT specifications)
/// and the `nse-itch` reference notes in `docs/protocols/nse_tbt.md`.
#[derive(Default, Debug, Clone, Copy)]
pub struct NseProtocolPlaceholder;

impl MarketDataProtocol for NseProtocolPlaceholder {
    fn parse(&self, raw: &[u8]) -> Result<RawTick, MarketDataError> {
        parse_canonical_json(self.name(), raw)
    }

    fn name(&self) -> &'static str {
        "NseProtocolPlaceholder"
    }
}

/// Placeholder BSE tick protocol parser.
///
/// In production this would decode the BSE EOBI (Enhanced Order Book
/// Interface) binary frame into a `RawTick`. Today it accepts the canonical
/// JSON form so the engine plumbing can be exercised end-to-end without a
/// vendor SDK.
///
/// TODO: production protocol — replace with vendor-specific binary parser.
/// See: <https://www.bseindia.com/markets/marketinfo/DispNewNoticesCirculars.aspx>
/// (EOBI specifications) and `docs/protocols/bse_eobi.md`.
#[derive(Default, Debug, Clone, Copy)]
pub struct BseProtocolPlaceholder;

impl MarketDataProtocol for BseProtocolPlaceholder {
    fn parse(&self, raw: &[u8]) -> Result<RawTick, MarketDataError> {
        parse_canonical_json(self.name(), raw)
    }

    fn name(&self) -> &'static str {
        "BseProtocolPlaceholder"
    }
}

/// Placeholder options-chain protocol parser.
///
/// In production this would decode the broker-specific options-chain
/// frame (Zerodha Kite, Dhan, Shoonya all ship their own format) into a
/// `RawTick` for the underlying instrument. Today it accepts the canonical
/// JSON form so the engine plumbing can be exercised end-to-end without
/// integrating a broker SDK.
///
/// TODO: production protocol — replace with vendor-specific binary parser.
/// See: <https://kite.trade/docs/connect/v3/websocket/> (Kite WebSocket
/// streaming) and `docs/protocols/options_chain.md`.
#[derive(Default, Debug, Clone, Copy)]
pub struct OptionsChainProtocolPlaceholder;

impl MarketDataProtocol for OptionsChainProtocolPlaceholder {
    fn parse(&self, raw: &[u8]) -> Result<RawTick, MarketDataError> {
        parse_canonical_json(self.name(), raw)
    }

    fn name(&self) -> &'static str {
        "OptionsChainProtocolPlaceholder"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &[u8] = br#"{
        "symbol": "RELIANCE",
        "ltp": 250000,
        "bid": 249950,
        "ask": 250050,
        "ltq": 100,
        "total_buy_qty": 5000,
        "total_sell_qty": 4500,
        "ts_exchange_ms": 1700000000000
    }"#;

    #[test]
    fn nse_placeholder_parses_canonical_json() {
        let p = NseProtocolPlaceholder;
        let tick = p.parse(SAMPLE).expect("parse ok");
        assert_eq!(tick.symbol, "RELIANCE");
        assert_eq!(tick.ltp_paise, 250_000);
        assert_eq!(tick.bid_paise, 249_950);
        assert_eq!(tick.ask_paise, 250_050);
        assert_eq!(tick.ltq, 100);
        assert_eq!(tick.total_buy_qty, 5_000);
        assert_eq!(tick.total_sell_qty, 4_500);
        assert_eq!(tick.ts_exchange_ms, 1_700_000_000_000);
        assert_eq!(p.name(), "NseProtocolPlaceholder");
    }

    #[test]
    fn bse_placeholder_parses_canonical_json() {
        let p = BseProtocolPlaceholder;
        let tick = p.parse(SAMPLE).expect("parse ok");
        assert_eq!(tick.symbol, "RELIANCE");
        assert_eq!(p.name(), "BseProtocolPlaceholder");
    }

    #[test]
    fn options_chain_placeholder_parses_canonical_json() {
        let p = OptionsChainProtocolPlaceholder;
        let tick = p.parse(SAMPLE).expect("parse ok");
        assert_eq!(tick.symbol, "RELIANCE");
        assert_eq!(p.name(), "OptionsChainProtocolPlaceholder");
    }

    #[test]
    fn placeholder_rejects_garbage_with_protocol_tagged_error() {
        let p = NseProtocolPlaceholder;
        let err = p.parse(b"not json").unwrap_err();
        match err {
            MarketDataError::Parse { protocol_name, .. } => {
                assert_eq!(protocol_name, "NseProtocolPlaceholder");
            }
            other => panic!("expected Parse, got {:?}", other),
        }
    }

    #[test]
    fn placeholder_rejects_missing_field() {
        let p = NseProtocolPlaceholder;
        let payload = br#"{"symbol":"X","ltp":1,"bid":1,"ask":1,"ltq":1,"total_buy_qty":1,"total_sell_qty":1}"#;
        let err = p.parse(payload).unwrap_err();
        assert!(matches!(err, MarketDataError::Parse { .. }));
    }
}
