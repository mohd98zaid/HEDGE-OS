//! Async REST client for Upstox API v2 (<https://upstox.com/developer/api-documentation/>).
//!
//! Auth is `Authorization: Bearer <access_token>` + `Accept: application/json`.

use std::time::Duration;

use hedge_broker_api::BrokerError;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;

/// Production base URL.
pub const UPSTOX_API_BASE: &str = "https://api.upstox.com";

/// Default per-request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Static credentials passed at construction. Mirrors the Upstox
/// developer portal app + the daily OAuth-minted access token.
#[derive(Clone, Debug)]
pub struct UpstoxCredentials {
    /// Developer-portal app API key (one-time setup).
    pub api_key: String,
    /// Developer-portal app secret (one-time setup, used during the
    /// OAuth code-exchange step that mints `access_token`).
    pub api_secret: String,
    /// Daily access token minted via the Upstox login redirect flow.
    /// Valid until ~03:30 IST the following day.
    pub access_token: String,
}

impl UpstoxCredentials {
    /// Construct from owned strings.
    pub fn new(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        access_token: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            access_token: access_token.into(),
        }
    }

    /// Returns `Ok(())` when `api_key` and `access_token` are non-empty.
    /// `api_secret` is only required for the OAuth token-mint step,
    /// which the adapter does not perform — it accepts the
    /// already-minted access token. So an empty `api_secret` is
    /// permitted as long as the access token is present.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.api_key.is_empty() {
            return Err("upstox api_key is empty");
        }
        if self.access_token.is_empty() {
            return Err("upstox access_token is empty");
        }
        Ok(())
    }
}

/// Async REST client.
pub struct UpstoxClient {
    inner: Client,
    base_url: String,
    creds: UpstoxCredentials,
}

impl UpstoxClient {
    /// Construct against the production base URL.
    pub fn new(creds: UpstoxCredentials) -> Result<Self, BrokerError> {
        Self::with_base(creds, UPSTOX_API_BASE)
    }

    /// Construct against a custom base URL (used by tests).
    pub fn with_base(
        creds: UpstoxCredentials,
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
    pub fn credentials(&self) -> &UpstoxCredentials {
        &self.creds
    }

    /// Borrow the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn default_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut h = HeaderMap::new();
        let bearer = format!("Bearer {}", self.creds.access_token);
        let auth = HeaderValue::from_str(&bearer)
            .map_err(|e| BrokerError::Auth(format!("invalid access token: {e}")))?;
        h.insert(AUTHORIZATION, auth);
        h.insert(ACCEPT, HeaderValue::from_static("application/json"));
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(h)
    }

    /// `POST /v2/order/place` — place a new order.
    pub async fn place_order<B: serde::Serialize + ?Sized>(
        &self,
        body: &B,
    ) -> Result<String, BrokerError> {
        let url = format!("{}/v2/order/place", self.base_url);
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
        let parsed: UpstoxPlaceOrderResponse = serde_json::from_str(&body).map_err(|e| {
            BrokerError::Internal(format!("decode upstox place_order: {e} body={body}"))
        })?;
        // Upstox returns `data.order_id` on success. Empty/missing is
        // treated as a rejection at the protocol layer.
        let order_id = parsed
            .data
            .and_then(|d| d.order_id)
            .ok_or_else(|| BrokerError::Rejected(format!("upstox missing order_id: {body}")))?;
        Ok(order_id)
    }

    /// `PUT /v2/order/modify` — modify a working order.
    pub async fn modify_order<B: serde::Serialize + ?Sized>(
        &self,
        body: &B,
    ) -> Result<(), BrokerError> {
        let url = format!("{}/v2/order/modify", self.base_url);
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

    /// `DELETE /v2/order/cancel?order_id=<id>` — cancel a working order.
    pub async fn cancel_order(&self, order_id: &str) -> Result<(), BrokerError> {
        let url = format!("{}/v2/order/cancel?order_id={}", self.base_url, order_id);
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

    /// `GET /v2/order/details?order_id=<id>` — fetch order status.
    pub async fn order_status(
        &self,
        order_id: &str,
    ) -> Result<UpstoxOrderStatusResponse, BrokerError> {
        let url = format!(
            "{}/v2/order/details?order_id={}",
            self.base_url, order_id
        );
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
        serde_json::from_str(&body)
            .map_err(|e| BrokerError::Internal(format!("decode upstox status: {e} body={body}")))
    }

    /// Liveness probe: `GET /v2/user/profile`. The endpoint returns
    /// 200 only when the access token is accepted, which is exactly
    /// what we want for `BrokerAdapter::ready()` (R7.5).
    pub async fn ping_user_profile(&self) -> Result<(), BrokerError> {
        let url = format!("{}/v2/user/profile", self.base_url);
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

// --- Response payloads -----------------------------------------------------

/// `POST /v2/order/place` response wrapper. Upstox wraps every payload
/// in `{ status, data: { ... } }`.
#[derive(Clone, Debug, Deserialize)]
pub struct UpstoxPlaceOrderResponse {
    /// `success` on accept; `error` on reject (mapped earlier by HTTP code).
    #[serde(default)]
    pub status: Option<String>,
    /// Inner data object — may be absent on error responses.
    #[serde(default)]
    pub data: Option<UpstoxOrderData>,
}

/// Inner `data` payload of `POST /v2/order/place`.
#[derive(Clone, Debug, Deserialize)]
pub struct UpstoxOrderData {
    /// Broker-assigned order id.
    #[serde(default)]
    pub order_id: Option<String>,
}

/// `GET /v2/order/details` response. Upstox surfaces an array of order
/// histories under `data`; we model the head element which carries the
/// canonical current state of the order.
#[derive(Clone, Debug, Deserialize)]
pub struct UpstoxOrderStatusResponse {
    /// `success` / `error`.
    #[serde(default)]
    pub status: Option<String>,
    /// Order detail payload.
    pub data: UpstoxOrderDetail,
}

/// Order detail payload — fields the FSM mapper needs.
#[derive(Clone, Debug, Deserialize)]
pub struct UpstoxOrderDetail {
    /// Broker order id.
    pub order_id: String,
    /// One of `open`, `complete`, `cancelled`, `rejected`,
    /// `partially filled`, etc.
    #[serde(default)]
    pub status: String,
    /// Filled quantity to date.
    #[serde(default)]
    pub filled_quantity: u64,
    /// Volume-weighted average fill price (rupees).
    #[serde(default)]
    pub average_price: f64,
}

// --- Error mapping ---------------------------------------------------------

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
    fn validate_rejects_empty_api_key_or_token() {
        assert!(UpstoxCredentials::new("", "secret", "tok").validate().is_err());
        assert!(UpstoxCredentials::new("key", "secret", "").validate().is_err());
        // api_secret is allowed to be empty (it's only used for token mint).
        assert!(UpstoxCredentials::new("key", "", "tok").validate().is_ok());
        assert!(UpstoxCredentials::new("key", "secret", "tok").validate().is_ok());
    }

    #[test]
    fn map_status_classifies_correctly() {
        assert!(matches!(map_status(401, "x"), BrokerError::Auth(_)));
        assert!(matches!(map_status(403, "x"), BrokerError::Auth(_)));
        assert!(matches!(map_status(429, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(503, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(422, "x"), BrokerError::Rejected(_)));
        assert!(matches!(map_status(418, "x"), BrokerError::Http { .. }));
    }

    #[test]
    fn place_order_response_decodes() {
        let r: UpstoxPlaceOrderResponse = serde_json::from_str(
            r#"{"status":"success","data":{"order_id":"230101000000001"}}"#,
        )
        .unwrap();
        assert_eq!(r.status.as_deref(), Some("success"));
        assert_eq!(r.data.unwrap().order_id.as_deref(), Some("230101000000001"));
    }

    #[test]
    fn status_response_decodes() {
        let r: UpstoxOrderStatusResponse = serde_json::from_str(
            r#"{"status":"success","data":{"order_id":"ord-7","status":"complete","filled_quantity":5,"average_price":100.5}}"#,
        )
        .unwrap();
        assert_eq!(r.data.order_id, "ord-7");
        assert_eq!(r.data.status, "complete");
        assert_eq!(r.data.filled_quantity, 5);
        assert!((r.data.average_price - 100.5).abs() < 1e-6);
    }
}
