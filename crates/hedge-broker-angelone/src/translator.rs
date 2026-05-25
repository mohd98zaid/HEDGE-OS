//! Translation between [`hedge_broker_api::OrderIntent`] and the
//! Angel One SmartAPI JSON payload.
//!
//! SmartAPI documentation lives at
//! <https://smartapi.angelbroking.com/docs>. The order endpoint
//! accepts a JSON body and is the path implemented here.

use hedge_broker_api::{OrderIntent, OrderType, Side};
use serde::Serialize;

/// JSON body for `POST /rest/secure/angelbroking/order/v1/placeOrder`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SmartApiPlaceOrderBody {
    /// `BUY` / `SELL`.
    pub transactiontype: &'static str,
    /// Order variety: `NORMAL` / `STOPLOSS` / `AMO` / `ROBO`.
    pub variety: &'static str,
    /// Order type: `MARKET` / `LIMIT` / `STOPLOSS_LIMIT` / `STOPLOSS_MARKET`.
    pub ordertype: &'static str,
    /// Product type: `INTRADAY` / `DELIVERY` / `MARGIN` / `BO` / `CO`.
    pub producttype: &'static str,
    /// Validity (`DAY` / `IOC`).
    pub duration: &'static str,
    /// Price (rupees, decimal string). `"0"` for market orders.
    pub price: String,
    /// Quantity (decimal string).
    pub quantity: String,
    /// Trading symbol (e.g. `"RELIANCE-EQ"`).
    pub tradingsymbol: String,
    /// Symbol token (numeric id assigned by Angel One). Stubbed by the
    /// default resolver as `format!("{symbol_raw}")`.
    pub symboltoken: String,
    /// Exchange (`NSE` / `BSE` / `NFO`).
    pub exchange: &'static str,
}

/// JSON body for `POST /rest/secure/angelbroking/order/v1/modifyOrder`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SmartApiModifyOrderBody {
    /// Order id to modify.
    pub orderid: String,
    /// Order variety: must match the original placement.
    pub variety: &'static str,
    /// New price, if changing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// New quantity, if changing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<String>,
    /// Order type — defaults to `LIMIT` when a price is supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordertype: Option<&'static str>,
}

/// JSON body for `POST /rest/secure/angelbroking/order/v1/cancelOrder`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SmartApiCancelOrderBody {
    /// Order id.
    pub orderid: String,
    /// Variety (must match placement).
    pub variety: &'static str,
}

/// Translate an [`OrderIntent`] to a SmartAPI place-order body.
pub fn intent_to_smartapi_body(
    intent: &OrderIntent,
    symbol_token_resolver: impl Fn(u32) -> String,
    tradingsymbol_resolver: impl Fn(u32) -> String,
) -> SmartApiPlaceOrderBody {
    let exchange = match intent.exchange {
        hedge_broker_api::Exchange::Nse => "NSE",
        hedge_broker_api::Exchange::Bse => "BSE",
    };
    let transactiontype = match intent.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let (ordertype, price) = match intent.order_type {
        OrderType::Market => ("MARKET", "0".to_string()),
        OrderType::Limit => ("LIMIT", paise_to_rupee_string(intent.limit_paise)),
    };
    SmartApiPlaceOrderBody {
        transactiontype,
        variety: "NORMAL",
        ordertype,
        producttype: "INTRADAY",
        duration: "DAY",
        price,
        quantity: intent.quantity.raw().to_string(),
        tradingsymbol: tradingsymbol_resolver(intent.symbol_raw),
        symboltoken: symbol_token_resolver(intent.symbol_raw),
        exchange,
    }
}

/// Translate an [`hedge_broker_api::OrderModification`] to a SmartAPI
/// modify body.
pub fn modification_to_smartapi_body(
    m: &hedge_broker_api::OrderModification,
) -> SmartApiModifyOrderBody {
    SmartApiModifyOrderBody {
        orderid: m.broker_order_id.clone(),
        variety: "NORMAL",
        price: m.new_limit_paise.map(paise_to_rupee_string),
        quantity: m.new_quantity.map(|q| q.raw().to_string()),
        ordertype: m.new_limit_paise.map(|_| "LIMIT"),
    }
}

/// Format paise as a rupee decimal string.
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

/// Default symbol-token resolver: emits the raw integer as a string.
pub fn default_symbol_token_resolver(symbol_raw: u32) -> String {
    symbol_raw.to_string()
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
    fn limit_buy_translates_to_smartapi_body() {
        let body = intent_to_smartapi_body(
            &intent(Side::Buy, OrderType::Limit, 5, 100_50),
            default_symbol_token_resolver,
            default_tradingsymbol_resolver,
        );
        assert_eq!(body.transactiontype, "BUY");
        assert_eq!(body.variety, "NORMAL");
        assert_eq!(body.ordertype, "LIMIT");
        assert_eq!(body.producttype, "INTRADAY");
        assert_eq!(body.duration, "DAY");
        assert_eq!(body.price, "100.50");
        assert_eq!(body.quantity, "5");
        assert_eq!(body.tradingsymbol, "SYM-7-EQ");
        assert_eq!(body.symboltoken, "7");
        assert_eq!(body.exchange, "NSE");
    }

    #[test]
    fn market_sell_translates_to_smartapi_body() {
        let body = intent_to_smartapi_body(
            &intent(Side::Sell, OrderType::Market, 3, 0),
            default_symbol_token_resolver,
            default_tradingsymbol_resolver,
        );
        assert_eq!(body.transactiontype, "SELL");
        assert_eq!(body.ordertype, "MARKET");
        assert_eq!(body.price, "0");
    }

    #[test]
    fn modify_with_price_sets_order_type_limit() {
        let m = hedge_broker_api::OrderModification {
            broker_order_id: "AO-1".into(),
            new_quantity: None,
            new_limit_paise: Some(99_75),
        };
        let body = modification_to_smartapi_body(&m);
        assert_eq!(body.orderid, "AO-1");
        assert_eq!(body.ordertype, Some("LIMIT"));
        assert_eq!(body.price.as_deref(), Some("99.75"));
        assert!(body.quantity.is_none());
    }

    #[test]
    fn modify_with_only_quantity() {
        let m = hedge_broker_api::OrderModification {
            broker_order_id: "AO-1".into(),
            new_quantity: Some(Qty::new(7)),
            new_limit_paise: None,
        };
        let body = modification_to_smartapi_body(&m);
        assert_eq!(body.quantity.as_deref(), Some("7"));
        assert!(body.ordertype.is_none());
        assert!(body.price.is_none());
    }
}
