//! Translation between [`hedge_broker_api::OrderIntent`] and the
//! Dhan API JSON payload.
//!
//! The Dhan order-placement endpoint is documented at
//! <https://dhanhq.co/docs/v2/orders/>. Bodies are JSON
//! (`application/json`).

use hedge_broker_api::{OrderIntent, OrderType, Side};
use serde::Serialize;

/// JSON body for `POST /v2/orders`. Dhan's surface is broader than the
/// Hot_Path uses; we surface the minimum required fields and leave
/// optional knobs at sensible defaults.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DhanPlaceOrderBody {
    /// Dhan client id (set by the adapter from credentials, not the
    /// translator).
    #[serde(rename = "dhanClientId")]
    pub dhan_client_id: String,
    /// `BUY` / `SELL`.
    #[serde(rename = "transactionType")]
    pub transaction_type: &'static str,
    /// Exchange segment (`NSE_EQ` / `BSE_EQ`). The Hot_Path is cash
    /// equity for now — futures/options segments will be added when
    /// the symbol resolver knows their lot multiplier.
    #[serde(rename = "exchangeSegment")]
    pub exchange_segment: &'static str,
    /// `INTRADAY` / `CNC` / `MARGIN`. Default `INTRADAY`.
    #[serde(rename = "productType")]
    pub product_type: &'static str,
    /// `MARKET` / `LIMIT`.
    #[serde(rename = "orderType")]
    pub order_type: &'static str,
    /// Validity (`DAY` / `IOC`). Default `DAY`.
    pub validity: &'static str,
    /// Dhan security id. Stubbed by the default resolver as
    /// `format!("{symbol_raw}")`.
    #[serde(rename = "securityId")]
    pub security_id: String,
    /// Quantity (whole units).
    pub quantity: u64,
    /// Limit price (rupees, two decimals). `0.0` for market orders.
    pub price: f64,
}

/// JSON body for `PUT /v2/orders/{orderId}`.
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct DhanModifyOrderBody {
    /// Order id this modification targets.
    #[serde(rename = "orderId")]
    pub order_id: String,
    /// New order type, if changing. Defaults to `LIMIT` when a price is
    /// supplied.
    #[serde(rename = "orderType", skip_serializing_if = "Option::is_none")]
    pub order_type: Option<&'static str>,
    /// New quantity, if changing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<u64>,
    /// New limit price (rupees), if changing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
}

/// Translate an [`OrderIntent`] to a Dhan place-order body.
pub fn intent_to_dhan_body(
    intent: &OrderIntent,
    dhan_client_id: &str,
    security_id_resolver: impl Fn(u32) -> String,
) -> DhanPlaceOrderBody {
    let exchange_segment = match intent.exchange {
        hedge_broker_api::Exchange::Nse => "NSE_EQ",
        hedge_broker_api::Exchange::Bse => "BSE_EQ",
    };
    let transaction_type = match intent.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let (order_type, price) = match intent.order_type {
        OrderType::Market => ("MARKET", 0.0),
        OrderType::Limit => ("LIMIT", paise_to_rupee_f64(intent.limit_paise)),
    };
    DhanPlaceOrderBody {
        dhan_client_id: dhan_client_id.to_owned(),
        transaction_type,
        exchange_segment,
        product_type: "INTRADAY",
        order_type,
        validity: "DAY",
        security_id: security_id_resolver(intent.symbol_raw),
        quantity: intent.quantity.raw(),
        price,
    }
}

/// Translate an [`hedge_broker_api::OrderModification`] to a Dhan
/// modify body. The `order_type` field is set to `Some("LIMIT")` when
/// the caller supplies a new price (Dhan's documented modify rules
/// require `orderType` whenever `price` is being changed).
pub fn modification_to_dhan_body(
    m: &hedge_broker_api::OrderModification,
) -> DhanModifyOrderBody {
    DhanModifyOrderBody {
        order_id: m.broker_order_id.clone(),
        order_type: m.new_limit_paise.map(|_| "LIMIT"),
        quantity: m.new_quantity.map(|q| q.raw()),
        price: m.new_limit_paise.map(paise_to_rupee_f64),
    }
}

/// Convert a paise count to a rupee f64 (two-decimal precision).
pub fn paise_to_rupee_f64(paise: i64) -> f64 {
    (paise as f64) / 100.0
}

/// Default symbol resolver: emits the integer id as a string.
pub fn default_security_id_resolver(symbol_raw: u32) -> String {
    symbol_raw.to_string()
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
    fn limit_buy_translates_to_dhan_body() {
        let body = intent_to_dhan_body(
            &intent(Side::Buy, OrderType::Limit, 10, 100_50),
            "CL123",
            default_security_id_resolver,
        );
        assert_eq!(body.dhan_client_id, "CL123");
        assert_eq!(body.transaction_type, "BUY");
        assert_eq!(body.exchange_segment, "NSE_EQ");
        assert_eq!(body.product_type, "INTRADAY");
        assert_eq!(body.order_type, "LIMIT");
        assert_eq!(body.validity, "DAY");
        assert_eq!(body.security_id, "7");
        assert_eq!(body.quantity, 10);
        assert!((body.price - 100.50).abs() < 1e-6);
    }

    #[test]
    fn market_sell_translates_to_dhan_body() {
        let body = intent_to_dhan_body(
            &intent(Side::Sell, OrderType::Market, 3, 0),
            "CL999",
            default_security_id_resolver,
        );
        assert_eq!(body.transaction_type, "SELL");
        assert_eq!(body.order_type, "MARKET");
        assert_eq!(body.price, 0.0);
    }

    #[test]
    fn modify_with_only_quantity() {
        let m = hedge_broker_api::OrderModification {
            broker_order_id: "ord-1".into(),
            new_quantity: Some(Qty::new(5)),
            new_limit_paise: None,
        };
        let body = modification_to_dhan_body(&m);
        assert_eq!(body.order_id, "ord-1");
        assert_eq!(body.quantity, Some(5));
        assert!(body.order_type.is_none());
        assert!(body.price.is_none());
    }

    #[test]
    fn modify_with_price_sets_order_type_limit() {
        let m = hedge_broker_api::OrderModification {
            broker_order_id: "ord-1".into(),
            new_quantity: None,
            new_limit_paise: Some(99_75),
        };
        let body = modification_to_dhan_body(&m);
        assert_eq!(body.order_type, Some("LIMIT"));
        let price = body.price.unwrap();
        assert!((price - 99.75).abs() < 1e-6);
    }
}
