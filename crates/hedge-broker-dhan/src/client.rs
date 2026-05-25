//! Async REST client for Dhan API v2 (<https://dhanhq.co/docs/v2/>).
//!
//! Auth is `access-token` header + `Content-Type: application/json`.

use std::time::Duration;

use hedge_broker_api::BrokerError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;

/// Production base URL.
pub const DHAN_API_BASE: &str = "https://api.dhan.co";

/// Default per-request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Static credentials.
#[derive(Clone, Debug)]
pub struct DhanCredentials {
    /// Dhan-issued client id (printed on the trading account).
    pub client_id: String,
    /// Long-lived access token from the Dhan partner portal.
    pub access_token: String,
}

impl DhanCredentials {
    /// Construct from owned strings.
    pub fn new(client_id: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            access_token: access_token.into(),
        }
    }

    /// Returns `Ok(())` when both fields are non-empty.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.client_id.is_empty() {
            return Err("dhan client_id is empty");
        }
        if self.access_token.is_empty() {
            return Err("dhan access_token is empty");
        }
        Ok(())
    }
}

/// Async REST client.
pub struct DhanClient {
    inner: Client,
    base_url: String,
    creds: DhanCredentials,
}

impl DhanClient {
    /// Construct against the production base URL.
    pub fn new(creds: DhanCredentials) -> Result<Self, BrokerError> {
        Self::with_base(creds, DHAN_API_BASE)
    }

    /// Construct against a custom base URL.
    pub fn with_base(
        creds: DhanCredentials,
        base_url: impl Into<String>,
    ) -> Result<Self, BrokerError> {
        let inner = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(|e| BrokerError::Internal(format!("reqwest build: {e}")))?;
        Ok(Self {
            inner,
            base_url: base_url.into(),
            creds,
        })
    }

    /// Borrow the credentials.
    pub fn credentials(&self) -> &DhanCredentials {
        &self.creds
    }

    /// Borrow the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn default_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut h = HeaderMap::new();
        let token = HeaderValue::from_str(&self.creds.access_token)
            .map_err(|e| BrokerError::Auth(format!("invalid access token: {e}")))?;
        h.insert(
            HeaderName::from_static("access-token"),
            token,
        );
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(h)
    }

    /// `POST /v2/orders` — place a new order.
    pub async fn place_order<B: serde::Serialize + ?Sized>(
        &self,
        body: &B,
    ) -> Result<String, BrokerError> {
        let url = format!("{}/v2/orders", self.base_url);
        let resp = self
            .inner
            .post(&url)
            .headers(self.default_headers()?)
            .json(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let body = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &body));
        }
        let parsed: DhanPlaceOrderResponse = serde_json::from_str(&body).map_err(|e| {
            BrokerError::Internal(format!("decode dhan place_order: {e} body={body}"))
        })?;
        Ok(parsed.order_id)
    }

    /// `PUT /v2/orders/{order_id}` — modify a working order.
    pub async fn modify_order<B: serde::Serialize + ?Sized>(
        &self,
        order_id: &str,
        body: &B,
    ) -> Result<(), BrokerError> {
        let url = format!("{}/v2/orders/{}", self.base_url, order_id);
        let resp = self
            .inner
            .put(&url)
            .headers(self.default_headers()?)
            .json(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let body = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &body));
        }
        Ok(())
    }

    /// `DELETE /v2/orders/{order_id}` — cancel a working order.
    pub async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let url = format!("{}/v2/orders/{}", self.base_url, order_id);
        let resp = self
            .inner
            .delete(&url)
            .headers(self.default_headers()?)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let body = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &body));
        }
        Ok(())
    }

    /// `GET /v2/orders/{order_id}` — order status.
    pub async fn order_status(&self, order_id: &str) -> Result<DhanOrderStatusResponse, BrokerError> {
        let url = format!("{}/v2/orders/{}", self.base_url, order_id);
        let resp = self
            .inner
            .get(&url)
            .headers(self.default_headers()?)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let body = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &body));
        }
        serde_json::from_str(&body).map_err(|e| {
            BrokerError::Internal(format!("decode dhan status: {e} body={body}"))
        })
    }

    /// Liveness probe: `GET /v2/orders` listing. Empty list is fine; the
    /// goal is to confirm the access token is accepted.
    pub async fn ping_orders(&self) -> Result<(), BrokerError> {
        let url = format!("{}/v2/orders", self.base_url);
        let resp = self
            .inner
            .get(&url)
            .headers(self.default_headers()?)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &body));
        }
        Ok(())
    }
}

/// `POST /v2/orders` response payload.
#[derive(Clone, Debug, Deserialize)]
pub struct DhanPlaceOrderResponse {
    /// Broker-side order id.
    #[serde(rename = "orderId")]
    pub order_id: String,
    /// `PENDING` / `TRANSIT` / etc. We surface this for completeness;
    /// the canonical state read uses [`DhanClient::order_status`].
    #[serde(default, rename = "orderStatus")]
    pub order_status: Option<String>,
}

/// `GET /v2/orders/{id}` response payload (the fields we need).
#[derive(Clone, Debug, Deserialize)]
pub struct DhanOrderStatusResponse {
    /// Broker-side order id.
    #[serde(rename = "orderId")]
    pub order_id: String,
    /// One of `PENDING`, `TRANSIT`, `TRADED`, `CANCELLED`, `REJECTED`,
    /// `EXPIRED`. We classify these into the FSM in
    /// `dhan_status_to_fsm` (see `lib.rs`).
    #[serde(rename = "orderStatus")]
    pub order_status: String,
    /// Filled quantity.
    #[serde(default, rename = "filledQty")]
    pub filled_qty: u64,
    /// Volume-weighted average fill price.
    #[serde(default, rename = "averageTradedPrice")]
    pub avg_price: f64,
}

fn map_network_err(e: reqwest::Error) -> BrokerError {
    if e.is_timeout() {
        BrokerError::Transient(format!("timeout: {e}"))
    } else if e.is_connect() {
        BrokerError::Network(format!("connect: {e}"))
    } else {
        BrokerError::Network(e.to_string())
    }
}

fn map_status(status: u16, body: &str) -> BrokerError {
    let truncated = if body.len() > 512 {
        let mut t: String = body.chars().take(512).collect();
        t.push('…');
        t
    } else {
        body.to_owned()
    };
    match status {
        401 | 403 => BrokerError::Auth(format!("{status}: {truncated}")),
        429 => BrokerError::Transient(format!("rate limited: {truncated}")),
        500..=599 => BrokerError::Transient(format!("{status}: {truncated}")),
        400 | 404 | 422 => BrokerError::Rejected(format!("{status}: {truncated}")),
        _ => BrokerError::Http {
            status,
            body: truncated,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_empty_creds() {
        assert!(DhanCredentials::new("", "x").validate().is_err());
        assert!(DhanCredentials::new("x", "").validate().is_err());
        assert!(DhanCredentials::new("x", "y").validate().is_ok());
    }

    #[test]
    fn map_status_classifies_correctly() {
        assert!(matches!(map_status(401, "x"), BrokerError::Auth(_)));
        assert!(matches!(map_status(429, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(503, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(422, "x"), BrokerError::Rejected(_)));
        assert!(matches!(map_status(418, "x"), BrokerError::Http { .. }));
    }

    #[test]
    fn place_order_response_decodes() {
        let r: DhanPlaceOrderResponse =
            serde_json::from_str(r#"{"orderId":"ord-7","orderStatus":"PENDING"}"#).unwrap();
        assert_eq!(r.order_id, "ord-7");
        assert_eq!(r.order_status.as_deref(), Some("PENDING"));
    }

    #[test]
    fn status_response_decodes() {
        let r: DhanOrderStatusResponse = serde_json::from_str(
            r#"{"orderId":"ord-7","orderStatus":"TRADED","filledQty":5,"averageTradedPrice":100.5}"#,
        )
        .unwrap();
        assert_eq!(r.order_status, "TRADED");
        assert_eq!(r.filled_qty, 5);
        assert!((r.avg_price - 100.5).abs() < 1e-6);
    }
}
