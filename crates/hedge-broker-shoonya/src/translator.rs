//! Translation between [`hedge_broker_api::OrderIntent`] and the
//! Shoonya / Finvasia API JSON payload.
//!
//! Shoonya's NorenAPI documentation lives at
//! <https://api.shoonya.com/NorenWebApi.html>. The order endpoint
//! accepts a single field `jData` which is a JSON-encoded string,
//! plus a `jKey` field carrying the session token.

use hedge_broker_api::{OrderIntent, OrderType, Side};
use serde::Serialize;

/// JSON body inside `jData` for `POST /NorenWClientTP/PlaceOrder`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ShoonyaPlaceOrderJData {
    /// User id (set from credentials).
    pub uid: String,
    /// Account id (often the same as `uid`).
    pub actid: String,
    /// Exchange (`NSE` / `BSE`).
    pub exch: &'static str,
    /// Trading symbol (broker-specific format, e.g. `RELIANCE-EQ`).
    pub tsym: String,
    /// Quantity as a decimal string.
    pub qty: String,
    /// Limit price (rupees, decimal string). `"0"` for market orders.
    pub prc: String,
    /// Product code: `I` (intraday MIS), `C` (CNC), `M` (margin).
    pub prd: &'static str,
    /// Transaction type: `B` (Buy) or `S` (Sell).
    pub trantype: &'static str,
    /// Price type: `MKT` / `LMT` / `SL-LMT` / `SL-MKT`.
    pub prctyp: &'static str,
    /// Validity: `DAY` / `IOC`.
    pub ret: &'static str,
}

/// JSON body inside `jData` for `POST /NorenWClientTP/ModifyOrder`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ShoonyaModifyOrderJData {
    /// User id.
    pub uid: String,
    /// Order number (broker-side id).
    pub norenordno: String,
    /// New quantity (decimal string), if changing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
    /// New price (decimal rupee string), if changing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prc: Option<String>,
    /// Price type if a price is being changed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prctyp: Option<&'static str>,
    /// Trading symbol — Shoonya's modify endpoint requires this even
    /// when neither qty nor price changes.
    pub tsym: String,
    /// Exchange.
    pub exch: &'static str,
}

/// Translate an [`OrderIntent`] to a Shoonya `jData` JSON body.
pub fn intent_to_shoonya_jdata(
    intent: &OrderIntent,
    user_id: &str,
    account_id: &str,
    tradingsymbol_resolver: impl Fn(u32) -> String,
) -> ShoonyaPlaceOrderJData {
    let exch = match intent.exchange {
        hedge_broker_api::Exchange::Nse => "NSE",
        hedge_broker_api::Exchange::Bse => "BSE",
    };
    let trantype = match intent.side {
        Side::Buy => "B",
        Side::Sell => "S",
    };
    let (prctyp, prc) = match intent.order_type {
        OrderType::Market => ("MKT", "0".to_string()),
        OrderType::Limit => ("LMT", paise_to_rupee_string(intent.limit_paise)),
    };
    ShoonyaPlaceOrderJData {
        uid: user_id.to_owned(),
        actid: account_id.to_owned(),
        exch,
        tsym: tradingsymbol_resolver(intent.symbol_raw),
        qty: intent.quantity.raw().to_string(),
        prc,
        prd: "I",
        trantype,
        prctyp,
        ret: "DAY",
    }
}

/// Translate an [`hedge_broker_api::OrderModification`] to Shoonya
/// `jData`. The caller must pass the original `tsym` and `exch` because
/// Shoonya requires both on modify.
pub fn modification_to_shoonya_jdata(
    m: &hedge_broker_api::OrderModification,
    user_id: &str,
    tsym: String,
    exch: &'static str,
) -> ShoonyaModifyOrderJData {
    ShoonyaModifyOrderJData {
        uid: user_id.to_owned(),
        norenordno: m.broker_order_id.clone(),
        qty: m.new_quantity.map(|q| q.raw().to_string()),
        prc: m.new_limit_paise.map(paise_to_rupee_string),
        prctyp: m.new_limit_paise.map(|_| "LMT"),
        tsym,
        exch,
    }
}

/// Format paise as a rupee decimal string (two decimals).
pub fn paise_to_rupee_string(paise: i64) -> String {
    let neg = paise < 0;
    let abs = paise.unsigned_abs();
    let rupees = abs / 100;
    let frac = abs % 100;
    if neg {
        format!("-{}.{:02}", rupees, frac)
    } else {
        format!("{}.{:02}", rupees, frac)
    }
}

/// Default tradingsymbol resolver: emits `SYM-{id}-EQ`.
pub fn default_tradingsymbol_resolver(symbol_raw: u32) -> String {
    format!("SYM-{}-EQ", symbol_raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_broker_api::Exchange;
    use hedge_core::{CorrelationId, Qty};

    fn intent(side: Side, ot: OrderType, qty: u64, limit_paise: i64) -> OrderIntent {
        OrderIntent {
            correlation_id: CorrelationId::NIL,
            symbol_raw: 7,
            side,
            quantity: Qty::new(qty),
            order_type: ot,
            limit_paise,
            exchange: Exchange::Nse,
        }
    }

    #[test]
    fn limit_buy_translates_to_jdata() {
        let j = intent_to_shoonya_jdata(
            &intent(Side::Buy, OrderType::Limit, 5, 100_50),
            "USER1",
            "ACT1",
            default_tradingsymbol_resolver,
        );
        assert_eq!(j.uid, "USER1");
        assert_eq!(j.actid, "ACT1");
        assert_eq!(j.exch, "NSE");
        assert_eq!(j.tsym, "SYM-7-EQ");
        assert_eq!(j.qty, "5");
        assert_eq!(j.prc, "100.50");
        assert_eq!(j.prd, "I");
        assert_eq!(j.trantype, "B");
        assert_eq!(j.prctyp, "LMT");
        assert_eq!(j.ret, "DAY");
    }

    #[test]
    fn market_sell_translates_to_jdata() {
        let j = intent_to_shoonya_jdata(
            &intent(Side::Sell, OrderType::Market, 10, 0),
            "U",
            "A",
            default_tradingsymbol_resolver,
        );
        assert_eq!(j.trantype, "S");
        assert_eq!(j.prctyp, "MKT");
        assert_eq!(j.prc, "0");
    }

    #[test]
    fn modify_translates_to_jdata() {
        let m = hedge_broker_api::OrderModification {
            broker_order_id: "NN-1".into(),
            new_quantity: Some(Qty::new(7)),
            new_limit_paise: Some(99_75),
        };
        let j = modification_to_shoonya_jdata(&m, "USER1", "RELIANCE-EQ".into(), "NSE");
        assert_eq!(j.uid, "USER1");
        assert_eq!(j.norenordno, "NN-1");
        assert_eq!(j.qty.as_deref(), Some("7"));
        assert_eq!(j.prc.as_deref(), Some("99.75"));
        assert_eq!(j.prctyp, Some("LMT"));
        assert_eq!(j.tsym, "RELIANCE-EQ");
        assert_eq!(j.exch, "NSE");
    }
}
