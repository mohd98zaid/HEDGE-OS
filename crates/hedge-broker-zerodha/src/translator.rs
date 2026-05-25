//! Translation between [`hedge_broker_api::OrderIntent`] and the
//! Kite Connect REST payload.
//!
//! The Kite Connect REST surface is documented at
//! <https://kite.trade/docs/connect/v3/orders/>. The fields we surface
//! are the minimum subset required to place a regular ("MIS"/"CNC")
//! order. Production features such as order tags, validity types, and
//! after-market orders are out of scope for task 17.1; the translator
//! emits well-formed defaults that the live integration can extend.

use hedge_broker_api::{OrderIntent, OrderType, Side};
use serde::Serialize;

/// Body of `POST /orders/{variety}` (form-urlencoded). All fields are
/// strings on the wire because Kite expects `application/x-www-form-urlencoded`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct KitePlaceOrderForm {
    /// Stock exchange (`NSE` / `BSE`).
    pub exchange: &'static str,
    /// Trading symbol — the symbol resolver converts a `SymbolId` to
    /// the broker's tradingsymbol. Stubbed here as `SYM-{id}` so the
    /// REST surface is well-formed even before the resolver lands.
    pub tradingsymbol: String,
    /// `BUY` / `SELL`.
    pub transaction_type: &'static str,
    /// Order quantity (whole units).
    pub quantity: String,
    /// `MARKET` / `LIMIT`.
    pub order_type: &'static str,
    /// Limit price as a decimal string in rupees (e.g. `"100.50"`).
    /// Empty string for market orders.
    pub price: String,
    /// Product code; `MIS` for intraday cash and `CNC` for delivery.
    /// Default `MIS` for the Hot_Path.
    pub product: &'static str,
    /// Validity of the order. Default `DAY`.
    pub validity: &'static str,
}

/// Translate a broker-agnostic [`OrderIntent`] to the Kite REST form body.
///
/// `tradingsymbol_resolver` is invoked once with the integer `symbol_raw`
/// and is expected to return the broker's tradingsymbol. The default
/// resolver used by the lib emits `SYM-{id}` — production integrations
/// will replace it via [`KiteClient::with_symbol_resolver`].
pub fn intent_to_kite_form(
    intent: &OrderIntent,
    tradingsymbol_resolver: impl Fn(u32) -> String,
) -> KitePlaceOrderForm {
    let exchange = match intent.exchange {
        hedge_broker_api::Exchange::Nse => "NSE",
        hedge_broker_api::Exchange::Bse => "BSE",
    };
    let transaction_type = match intent.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let (order_type, price) = match intent.order_type {
        OrderType::Market => ("MARKET", String::new()),
        OrderType::Limit => ("LIMIT", paise_to_rupee_string(intent.limit_paise)),
    };
    KitePlaceOrderForm {
        exchange,
        tradingsymbol: tradingsymbol_resolver(intent.symbol_raw),
        transaction_type,
        quantity: intent.quantity.raw().to_string(),
        order_type,
        price,
        product: "MIS",
        validity: "DAY",
    }
}

/// Body of `PUT /orders/{variety}/{order_id}` (form-urlencoded). Only
/// the fields we know are mutable are surfaced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Default)]
pub struct KiteModifyOrderForm {
    /// New quantity, if changing.
    pub quantity: Option<String>,
    /// New limit price (rupees, decimal string), if changing.
    pub price: Option<String>,
}

/// Translate a [`hedge_broker_api::OrderModification`] to the Kite
/// modify body.
pub fn modification_to_kite_form(
    m: &hedge_broker_api::OrderModification,
) -> KiteModifyOrderForm {
    KiteModifyOrderForm {
        quantity: m.new_quantity.map(|q| q.raw().to_string()),
        price: m.new_limit_paise.map(paise_to_rupee_string),
    }
}

/// Format a paise count as a rupee decimal string with two decimals.
/// Negative values are formatted with a leading `-` though Kite never
/// accepts negative prices.
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

/// Default symbol resolver used until a production resolver is wired up.
/// Emits `SYM-{id}` so the REST surface is well-formed even before the
/// real resolver lands.
pub fn default_symbol_resolver(symbol_raw: u32) -> String {
    format!("SYM-{}", symbol_raw)
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
    fn paise_to_rupee_string_formats_two_decimals() {
        assert_eq!(paise_to_rupee_string(0), "0.00");
        assert_eq!(paise_to_rupee_string(5), "0.05");
        assert_eq!(paise_to_rupee_string(50), "0.50");
        assert_eq!(paise_to_rupee_string(100), "1.00");
        assert_eq!(paise_to_rupee_string(150), "1.50");
        assert_eq!(paise_to_rupee_string(123_45), "123.45");
        assert_eq!(paise_to_rupee_string(-100), "-1.00");
    }

    #[test]
    fn limit_buy_translates_to_kite_form() {
        let f = intent_to_kite_form(
            &intent(Side::Buy, OrderType::Limit, 5, 100_50),
            default_symbol_resolver,
        );
        assert_eq!(f.exchange, "NSE");
        assert_eq!(f.tradingsymbol, "SYM-7");
        assert_eq!(f.transaction_type, "BUY");
        assert_eq!(f.quantity, "5");
        assert_eq!(f.order_type, "LIMIT");
        assert_eq!(f.price, "100.50");
        assert_eq!(f.product, "MIS");
        assert_eq!(f.validity, "DAY");
    }

    #[test]
    fn market_sell_translates_to_kite_form() {
        let f = intent_to_kite_form(
            &intent(Side::Sell, OrderType::Market, 12, 0),
            default_symbol_resolver,
        );
        assert_eq!(f.transaction_type, "SELL");
        assert_eq!(f.quantity, "12");
        assert_eq!(f.order_type, "MARKET");
        assert!(f.price.is_empty());
    }

    #[test]
    fn modification_translates_to_kite_form() {
        let m = hedge_broker_api::OrderModification {
            broker_order_id: "abc".into(),
            new_quantity: Some(Qty::new(7)),
            new_limit_paise: Some(99_75),
        };
        let f = modification_to_kite_form(&m);
        assert_eq!(f.quantity.as_deref(), Some("7"));
        assert_eq!(f.price.as_deref(), Some("99.75"));
    }

    #[test]
    fn modification_empty_fields_translate_to_none() {
        let m = hedge_broker_api::OrderModification {
            broker_order_id: "abc".into(),
            new_quantity: None,
            new_limit_paise: None,
        };
        let f = modification_to_kite_form(&m);
        assert!(f.quantity.is_none());
        assert!(f.price.is_none());
    }
}
