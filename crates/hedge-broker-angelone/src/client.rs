//! Async REST client for Angel One SmartAPI
//! (<https://smartapi.angelbroking.com/docs>).
//!
//! ### Auth
//!
//! SmartAPI auth flows in two stages:
//!
//! 1. Login by user/password produces a `jwtToken` (access token) and
//!    a `feedToken` (used for the WebSocket binary tick protocol — that
//!    path lives in `hedge-market-data`).
//! 2. Every subsequent request carries:
//!    * `Authorization: Bearer <jwtToken>`
//!    * `X-PrivateKey: <api_key>`
//!    * a set of identification headers SmartAPI requires
//!      (`X-UserType`, `X-SourceID`, `X-ClientLocalIP`,
//!      `X-ClientPublicIP`, `X-MACAddress`).
//!
//! The login flow runs out-of-process; this client takes the **already
//! minted** `jwtToken` plus the API key as credentials and treats them
//! as opaque.
//!
//! **TODO: production protocol** — the SmartAPI WebSocket binary tick
//! protocol is in `hedge-market-data`, not here.

use std::time::Duration;

use hedge_broker_api::BrokerError;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;

/// Production base URL.
pub const SMARTAPI_BASE: &str = "https://apiconnect.angelbroking.com";

/// Default per-request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Static credentials.
#[derive(Clone, Debug)]
pub struct SmartApiCredentials {
    /// API key issued in the SmartAPI portal.
    pub api_key: String,
    /// Active JWT bearer token from the login flow.
    pub jwt_token: String,
    /// SmartAPI client code (the user account id).
    pub client_code: String,
    /// Local IP — required by SmartAPI's identification headers.
    pub local_ip: String,
    /// Public IP — required by SmartAPI's identification headers.
    pub public_ip: String,
    /// MAC address — required by SmartAPI's identification headers.
    pub mac_address: String,
}

impl SmartApiCredentials {
    /// Construct from owned strings.
    pub fn new(
        api_key: impl Into<String>,
        jwt_token: impl Into<String>,
        client_code: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            jwt_token: jwt_token.into(),
            client_code: client_code.into(),
            local_ip: String::new(),
            public_ip: String::new(),
            mac_address: String::new(),
        }
    }

    /// Set the network identification headers SmartAPI requires.
    pub fn with_identification(
        mut self,
        local_ip: impl Into<String>,
        public_ip: impl Into<String>,
        mac_address: impl Into<String>,
    ) -> Self {
        self.local_ip = local_ip.into();
        self.public_ip = public_ip.into();
        self.mac_address = mac_address.into();
        self
    }

    /// Returns `Ok(())` when the minimum required fields are set.
    /// `local_ip`, `public_ip`, and `mac_address` are not strictly
    /// required for ready-fail-closed semantics — SmartAPI accepts
    /// dummy values — so we do not fail validation on those.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.api_key.is_empty() {
            return Err("angelone api_key is empty");
        }
        if self.jwt_token.is_empty() {
            return Err("angelone jwt_token is empty");
        }
        if self.client_code.is_empty() {
            return Err("angelone client_code is empty");
        }
        Ok(())
    }
}

/// Async REST client.
pub struct SmartApiClient {
    inner: Client,
    base_url: String,
    creds: SmartApiCredentials,
}

impl SmartApiClient {
    /// Construct against the production base URL.
    pub fn new(creds: SmartApiCredentials) -> Result<Self, BrokerError> {
        Self::with_base(creds, SMARTAPI_BASE)
    }

    /// Construct against a custom base URL.
    pub fn with_base(
        creds: SmartApiCredentials,
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
    pub fn credentials(&self) -> &SmartApiCredentials {
        &self.creds
    }

    /// Borrow the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn default_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut h = HeaderMap::new();
        h.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.creds.jwt_token))
                .map_err(|e| BrokerError::Auth(format!("invalid jwt: {e}")))?,
        );
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        h.insert(
            HeaderName::from_static("accept"),
            HeaderValue::from_static("application/json"),
        );
        h.insert(
            HeaderName::from_static("x-privatekey"),
            HeaderValue::from_str(&self.creds.api_key)
                .map_err(|e| BrokerError::Auth(format!("invalid api key: {e}")))?,
        );
        h.insert(
            HeaderName::from_static("x-usertype"),
            HeaderValue::from_static("USER"),
        );
        h.insert(
            HeaderName::from_static("x-sourceid"),
            HeaderValue::from_static("WEB"),
        );
        // The IP/MAC headers are required by SmartAPI but accept dummy
        // values; we ship empty strings if the caller did not configure
        // them so the request still goes through.
        h.insert(
            HeaderName::from_static("x-clientlocalip"),
            HeaderValue::from_str(&self.creds.local_ip)
                .unwrap_or_else(|_| HeaderValue::from_static("0.0.0.0")),
        );
        h.insert(
            HeaderName::from_static("x-clientpublicip"),
            HeaderValue::from_str(&self.creds.public_ip)
                .unwrap_or_else(|_| HeaderValue::from_static("0.0.0.0")),
        );
        h.insert(
            HeaderName::from_static("x-macaddress"),
            HeaderValue::from_str(&self.creds.mac_address)
                .unwrap_or_else(|_| HeaderValue::from_static("00:00:00:00:00:00")),
        );
        Ok(h)
    }

    /// `POST /rest/secure/angelbroking/order/v1/placeOrder`.
    pub async fn place_order<B: serde::Serialize + ?Sized>(
        &self,
        body: &B,
    ) -> Result<String, BrokerError> {
        let url = format!(
            "{}/rest/secure/angelbroking/order/v1/placeOrder",
            self.base_url
        );
        let resp = self
            .inner
            .post(&url)
            .headers(self.default_headers()?)
            .json(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: SmartApiEnvelope<SmartApiPlaceOrderData> = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode angel place_order: {e} body={text}")))?;
        parsed.into_data().map(|d| d.order_id)
    }

    /// `POST /rest/secure/angelbroking/order/v1/modifyOrder`.
    pub async fn modify_order<B: serde::Serialize + ?Sized>(
        &self,
        body: &B,
    ) -> Result<(), BrokerError> {
        let url = format!(
            "{}/rest/secure/angelbroking/order/v1/modifyOrder",
            self.base_url
        );
        let resp = self
            .inner
            .post(&url)
            .headers(self.default_headers()?)
            .json(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: SmartApiEnvelope<serde_json::Value> = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode angel modify: {e} body={text}")))?;
        parsed.into_data().map(|_| ())
    }

    /// `POST /rest/secure/angelbroking/order/v1/cancelOrder`.
    pub async fn cancel_order(
        &self,
        order_id: &str,
        variety: &str,
    ) -> Result<(), BrokerError> {
        let url = format!(
            "{}/rest/secure/angelbroking/order/v1/cancelOrder",
            self.base_url
        );
        let body = serde_json::json!({
            "variety": variety,
            "orderid": order_id,
        });
        let resp = self
            .inner
            .post(&url)
            .headers(self.default_headers()?)
            .json(&body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: SmartApiEnvelope<serde_json::Value> = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode angel cancel: {e} body={text}")))?;
        parsed.into_data().map(|_| ())
    }

    /// `GET /rest/secure/angelbroking/order/v1/details/{order_id}`. The
    /// SmartAPI documentation for this endpoint is sparse so the
    /// canonical fields surfaced here are best-effort.
    ///
    /// **TODO: production protocol** — the actual order-details URL
    /// shape varies by deployment; production callers should switch to
    /// `getOrderBook` and filter locally.
    pub async fn order_status(&self, order_id: &str) -> Result<SmartApiOrderStatus, BrokerError> {
        let url = format!(
            "{}/rest/secure/angelbroking/order/v1/details/{}",
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
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: SmartApiEnvelope<SmartApiOrderStatus> = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode angel status: {e} body={text}")))?;
        parsed.into_data()
    }

    /// Liveness probe: `GET /rest/secure/angelbroking/user/v1/getProfile`.
    pub async fn ping_profile(&self) -> Result<(), BrokerError> {
        let url = format!(
            "{}/rest/secure/angelbroking/user/v1/getProfile",
            self.base_url
        );
        let resp = self
            .inner
            .get(&url)
            .headers(self.default_headers()?)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: SmartApiEnvelope<serde_json::Value> = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode angel profile: {e} body={text}")))?;
        parsed.into_data().map(|_| ())
    }
}

/// Generic SmartAPI envelope:
/// `{ "status": true, "message": "...", "errorcode": "...", "data": ... }`.
#[derive(Clone, Debug, Deserialize)]
pub struct SmartApiEnvelope<T> {
    /// `true` on success.
    pub status: bool,
    /// Optional message; populated on errors.
    #[serde(default)]
    pub message: Option<String>,
    /// Optional error code; populated on errors.
    #[serde(default)]
    pub errorcode: Option<String>,
    /// Response payload.
    #[serde(default = "Option::default")]
    pub data: Option<T>,
}

impl<T> SmartApiEnvelope<T> {
    fn into_data(self) -> Result<T, BrokerError> {
        if self.status {
            self.data
                .ok_or_else(|| BrokerError::Internal("smartapi envelope missing data".into()))
        } else {
            let code = self.errorcode.unwrap_or_default();
            let msg = self.message.unwrap_or_else(|| code.clone());
            Err(classify_smartapi_error(&code, &msg))
        }
    }
}

/// `placeOrder` data payload.
#[derive(Clone, Debug, Deserialize)]
pub struct SmartApiPlaceOrderData {
    /// Broker-side order id.
    #[serde(rename = "orderid")]
    pub order_id: String,
}

/// Order status read.
#[derive(Clone, Debug, Deserialize)]
pub struct SmartApiOrderStatus {
    /// Broker-side order id.
    #[serde(default, rename = "orderid")]
    pub order_id: String,
    /// Status string. SmartAPI documents `complete`, `rejected`,
    /// `cancelled`, `open`, `pending`, `trigger pending`,
    /// `partially filled`. We classify these in the lib's
    /// `smartapi_status_to_fsm` helper.
    #[serde(default)]
    pub status: String,
    /// Filled quantity.
    #[serde(default, rename = "filledshares")]
    pub filled_shares: u64,
    /// Volume-weighted average fill price (rupees, decimal).
    #[serde(default, rename = "averageprice")]
    pub avg_price: f64,
}

/// Map SmartAPI errorcode strings to `BrokerError` variants. Codes
/// surfaced here come from the SmartAPI Errors page; not every code is
/// documented, so the default falls back to `Rejected`.
fn classify_smartapi_error(code: &str, msg: &str) -> BrokerError {
    let combined = format!("{code} {msg}");
    match code {
        // AB1010 — User not authenticated.
        // AB1004 — Invalid token.
        // AB2000 / AB2001 — Auth-related
        c if c.starts_with("AB10") => BrokerError::Auth(combined),
        // AB1007 / AB1008 — rate-limit or transient
        "AB1007" | "AB1008" | "AB1011" => BrokerError::Transient(combined),
        // AB9000 onward — internal SmartAPI errors → Transient
        c if c.starts_with("AB9") => BrokerError::Transient(combined),
        _ => BrokerError::Rejected(combined),
    }
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
        assert!(SmartApiCredentials::new("", "j", "c").validate().is_err());
        assert!(SmartApiCredentials::new("k", "", "c").validate().is_err());
        assert!(SmartApiCredentials::new("k", "j", "").validate().is_err());
        assert!(SmartApiCredentials::new("k", "j", "c").validate().is_ok());
    }

    #[test]
    fn classify_smartapi_error_routes_codes() {
        assert!(matches!(
            classify_smartapi_error("AB1010", "auth"),
            BrokerError::Auth(_)
        ));
        assert!(matches!(
            classify_smartapi_error("AB1007", "rate"),
            BrokerError::Transient(_)
        ));
        assert!(matches!(
            classify_smartapi_error("AB9001", "internal"),
            BrokerError::Transient(_)
        ));
        assert!(matches!(
            classify_smartapi_error("AB2042", "validation"),
            BrokerError::Rejected(_)
        ));
    }

    #[test]
    fn map_status_classifies_correctly() {
        assert!(matches!(map_status(401, "x"), BrokerError::Auth(_)));
        assert!(matches!(map_status(503, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(429, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(400, "x"), BrokerError::Rejected(_)));
    }

    #[test]
    fn smartapi_envelope_decodes_success() {
        let env: SmartApiEnvelope<SmartApiPlaceOrderData> = serde_json::from_str(
            r#"{"status":true,"message":"SUCCESS","errorcode":"","data":{"orderid":"ID-1"}}"#,
        )
        .unwrap();
        assert_eq!(env.into_data().unwrap().order_id, "ID-1");
    }

    #[test]
    fn smartapi_envelope_decodes_failure() {
        let env: SmartApiEnvelope<SmartApiPlaceOrderData> = serde_json::from_str(
            r#"{"status":false,"message":"Invalid token","errorcode":"AB1010","data":null}"#,
        )
        .unwrap();
        match env.into_data().unwrap_err() {
            BrokerError::Auth(m) => assert!(m.contains("AB1010")),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
