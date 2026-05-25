//! Async HTTP client wrapping `reqwest::Client` for Zerodha Kite Connect
//! v3 (<https://kite.trade/docs/connect/v3/>).
//!
//! ### Hot_Path discipline
//!
//! * Uses **only** the async `reqwest::Client`. `reqwest::blocking::*` is
//!   forbidden by the workspace CI gate (R30.7).
//! * Times out every request at the configured budget so a hung broker
//!   does not stall the Execution_Engine.
//! * Maps every response into a stable [`BrokerError`] variant; never
//!   panics on broker-supplied input.
//!
//! ### Auth
//!
//! Kite Connect uses an `api_key` plus a daily `access_token` minted via
//! the OAuth-style login flow. The Hot_Path receives both at startup
//! through `hedge-config`; this client treats the pair as opaque
//! credentials and applies them on every request via the canonical
//! `Authorization: token <api_key>:<access_token>` header.
//!
//! ### Production protocol gaps
//!
//! The order-placement endpoint surface is well-documented and is
//! implemented here. The Kite WebSocket binary tick protocol used for
//! market data is not part of task 17.1 (that path lives in
//! `hedge-market-data`). Where Kite exposes vendor-specific behaviours
//! we have insufficient public docs for (e.g. the post-trade
//! confirmation push), we mark the integration with
//! `// TODO: production protocol — replace with vendor-specific binary parser`.

use std::time::Duration;

use hedge_broker_api::BrokerError;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;

/// Production base URL for Kite Connect v3.
pub const KITE_API_BASE: &str = "https://api.kite.trade";

/// Default request timeout (5 s — well under R28.5 broker submit budget
/// after retries).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Static credentials passed at construction. Surface mirrors the
/// `BrokerConfig` shape; production callers populate it from
/// `hedge-config`.
#[derive(Clone, Debug)]
pub struct KiteCredentials {
    /// API key issued by Kite (free-form string).
    pub api_key: String,
    /// Daily access token minted via the login flow.
    pub access_token: String,
}

impl KiteCredentials {
    /// Construct from owned strings.
    pub fn new(api_key: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            access_token: access_token.into(),
        }
    }

    /// Returns `Ok(())` when both fields are non-empty. Used by the
    /// adapter's `ready()` to fail closed (R7.5).
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.api_key.is_empty() {
            return Err("zerodha api_key is empty");
        }
        if self.access_token.is_empty() {
            return Err("zerodha access_token is empty");
        }
        Ok(())
    }

    /// Build the `Authorization` header value Kite expects.
    pub fn auth_header(&self) -> String {
        format!("token {}:{}", self.api_key, self.access_token)
    }
}

/// Thin async REST client.
pub struct KiteClient {
    inner: Client,
    base_url: String,
    creds: KiteCredentials,
}

impl KiteClient {
    /// Construct with the given credentials and a default reqwest client
    /// (5-second per-request timeout, native rustls).
    pub fn new(creds: KiteCredentials) -> Result<Self, BrokerError> {
        Self::with_base(creds, KITE_API_BASE)
    }

    /// Construct with a custom base URL (used by tests against mock
    /// servers and by replay environments).
    pub fn with_base(creds: KiteCredentials, base_url: impl Into<String>) -> Result<Self, BrokerError> {
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

    /// Borrow the credentials (used by `ready()` validation).
    pub fn credentials(&self) -> &KiteCredentials {
        &self.creds
    }

    /// Borrow the configured base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Build a default header map with the `Authorization` and
    /// `Content-Type: application/x-www-form-urlencoded` headers Kite
    /// requires.
    fn default_headers(&self) -> Result<HeaderMap, BrokerError> {
        let mut h = HeaderMap::new();
        h.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&self.creds.auth_header())
                .map_err(|e| BrokerError::Auth(format!("invalid auth header: {e}")))?,
        );
        h.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        Ok(h)
    }

    /// `POST /orders/{variety}` — place a new order. Returns the
    /// `order_id` Kite assigned.
    ///
    /// `form` is a value that serialises to the form-urlencoded body.
    pub async fn place_order<F: serde::Serialize + ?Sized>(
        &self,
        variety: &str,
        form: &F,
    ) -> Result<String, BrokerError> {
        let url = format!("{}/orders/{}", self.base_url, variety);
        let resp = self
            .inner
            .post(&url)
            .headers(self.default_headers()?)
            .form(form)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let body = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &body));
        }
        // Kite returns:  { "status": "success", "data": { "order_id": "..." } }
        let parsed: KiteEnvelope<KitePlaceOrderResponse> = serde_json::from_str(&body)
            .map_err(|e| BrokerError::Internal(format!("decode place_order: {e} body={body}")))?;
        parsed.data_ok().map(|d| d.order_id)
    }

    /// `PUT /orders/{variety}/{order_id}` — modify a working order.
    pub async fn modify_order<F: serde::Serialize + ?Sized>(
        &self,
        variety: &str,
        order_id: &str,
        form: &F,
    ) -> Result<(), BrokerError> {
        let url = format!("{}/orders/{}/{}", self.base_url, variety, order_id);
        let resp = self
            .inner
            .put(&url)
            .headers(self.default_headers()?)
            .form(form)
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

    /// `DELETE /orders/{variety}/{order_id}` — cancel a working order.
    pub async fn cancel_order(&self, variety: &str, order_id: &str) -> Result<(), BrokerError> {
        let url = format!("{}/orders/{}/{}", self.base_url, variety, order_id);
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

    /// `GET /orders/{order_id}` — read order history; the most recent
    /// entry is the current state.
    pub async fn order_status(&self, order_id: &str) -> Result<KiteOrderHistory, BrokerError> {
        let url = format!("{}/orders/{}", self.base_url, order_id);
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
        let parsed: KiteEnvelope<Vec<KiteOrderHistory>> = serde_json::from_str(&body)
            .map_err(|e| BrokerError::Internal(format!("decode order_status: {e} body={body}")))?;
        let entries = parsed.data_ok()?;
        entries
            .into_iter()
            .last()
            .ok_or_else(|| BrokerError::UnknownOrderId(order_id.to_owned()))
    }

    /// Best-effort liveness probe used by [`ready()`]. Calls
    /// `GET /user/profile`. A successful response indicates the
    /// credentials are accepted; 401/403 is mapped to `Auth`; any
    /// transport error is mapped to `Network`.
    pub async fn ping_user_profile(&self) -> Result<(), BrokerError> {
        let url = format!("{}/user/profile", self.base_url);
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

/// Generic Kite envelope: `{ "status": "...", "data": ..., "message": "..." }`.
#[derive(Clone, Debug, Deserialize)]
pub struct KiteEnvelope<T> {
    /// `"success"` for OK.
    pub status: String,
    /// Optional message; populated on errors.
    #[serde(default)]
    pub message: Option<String>,
    /// Response payload.
    #[serde(default = "Option::default")]
    pub data: Option<T>,
}

impl<T> KiteEnvelope<T> {
    /// Take `data` if `status == "success"`; otherwise map to `Rejected`.
    pub fn data_ok(self) -> Result<T, BrokerError> {
        if self.status == "success" {
            self.data.ok_or_else(|| {
                BrokerError::Internal("kite envelope missing data on success".into())
            })
        } else {
            let msg = self.message.unwrap_or_else(|| self.status.clone());
            Err(BrokerError::Rejected(msg))
        }
    }
}

/// Place-order response payload.
#[derive(Clone, Debug, Deserialize)]
pub struct KitePlaceOrderResponse {
    /// Broker-side order id assigned by Kite.
    pub order_id: String,
}

/// Order history entry. Kite returns a list of these; the last one is
/// the current state. We surface only the fields the adapter needs.
///
/// **TODO: production protocol** — Kite documents at least a dozen
/// optional fields (e.g. `tag`, `instrument_token`, `placed_by`); the
/// canonical list lives at <https://kite.trade/docs/connect/v3/orders/>.
/// `#[serde(default)]` and `#[serde(deny_unknown_fields)] = false`
/// (the default) means new fields are silently ignored, which is the
/// right default for a forward-compatible REST client.
#[derive(Clone, Debug, Deserialize)]
pub struct KiteOrderHistory {
    /// Broker-side order id.
    pub order_id: String,
    /// One of `OPEN`, `COMPLETE`, `CANCELLED`, `REJECTED`, `TRIGGER PENDING`,
    /// `OPEN_PENDING`, `MODIFY_PENDING`, `CANCEL_PENDING`. We classify
    /// these into the FSM in the lib's `kite_status_to_fsm` helper.
    pub status: String,
    /// Total filled quantity to date.
    #[serde(default)]
    pub filled_quantity: u64,
    /// Volume-weighted average fill price (rupees, decimal). `0.0` before
    /// any fill.
    #[serde(default)]
    pub average_price: f64,
}

/// Map a `reqwest::Error` to a [`BrokerError`] variant. Network-level
/// errors are retryable; HTTP errors that surface here have already
/// produced a status (handled in [`map_status`]).
fn map_network_err(e: reqwest::Error) -> BrokerError {
    if e.is_timeout() {
        BrokerError::Transient(format!("timeout: {e}"))
    } else if e.is_connect() {
        BrokerError::Network(format!("connect: {e}"))
    } else {
        BrokerError::Network(e.to_string())
    }
}

/// Map an HTTP status (with body) to a [`BrokerError`].
fn map_status(status: u16, body: &str) -> BrokerError {
    let truncated_body = truncate(body, 512);
    match status {
        401 | 403 => BrokerError::Auth(format!("{status}: {truncated_body}")),
        429 => BrokerError::Transient(format!("rate limited: {truncated_body}")),
        500..=599 => BrokerError::Transient(format!("{status}: {truncated_body}")),
        400 | 404 | 422 => BrokerError::Rejected(format!("{status}: {truncated_body}")),
        _ => BrokerError::Http {
            status,
            body: truncated_body.into_owned(),
        },
    }
}

/// Truncate a string to at most `max_chars` characters, appending `…`
/// when truncated. Operates on `char` boundaries to stay UTF-8 safe.
fn truncate(s: &str, max_chars: usize) -> std::borrow::Cow<'_, str> {
    if s.chars().count() <= max_chars {
        std::borrow::Cow::Borrowed(s)
    } else {
        let mut out: String = s.chars().take(max_chars).collect();
        out.push('…');
        std::borrow::Cow::Owned(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_validate_rejects_empty_fields() {
        assert!(KiteCredentials::new("", "tok").validate().is_err());
        assert!(KiteCredentials::new("key", "").validate().is_err());
        assert!(KiteCredentials::new("key", "tok").validate().is_ok());
    }

    #[test]
    fn auth_header_format_matches_kite_spec() {
        let creds = KiteCredentials::new("apikey", "acctok");
        assert_eq!(creds.auth_header(), "token apikey:acctok");
    }

    #[test]
    fn map_status_classifies_http_codes() {
        assert!(matches!(map_status(401, "x"), BrokerError::Auth(_)));
        assert!(matches!(map_status(403, "x"), BrokerError::Auth(_)));
        assert!(matches!(map_status(429, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(503, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(400, "x"), BrokerError::Rejected(_)));
        assert!(matches!(map_status(404, "x"), BrokerError::Rejected(_)));
        assert!(matches!(map_status(418, "x"), BrokerError::Http { .. }));
    }

    #[test]
    fn truncate_keeps_under_limit_intact() {
        let s = "hello";
        assert_eq!(truncate(s, 10).as_ref(), "hello");
    }

    #[test]
    fn truncate_clips_long_strings() {
        let s = "a".repeat(20);
        let out = truncate(&s, 5);
        assert_eq!(out.as_ref().chars().count(), 6); // 5 + ellipsis
        assert!(out.ends_with('…'));
    }

    #[test]
    fn kite_envelope_data_ok_on_success() {
        let env: KiteEnvelope<KitePlaceOrderResponse> = serde_json::from_str(
            r#"{"status":"success","data":{"order_id":"abc-1"}}"#,
        )
        .unwrap();
        assert_eq!(env.data_ok().unwrap().order_id, "abc-1");
    }

    #[test]
    fn kite_envelope_data_ok_on_failure_returns_rejected() {
        let env: KiteEnvelope<KitePlaceOrderResponse> = serde_json::from_str(
            r#"{"status":"error","message":"invalid lot"}"#,
        )
        .unwrap();
        match env.data_ok().unwrap_err() {
            BrokerError::Rejected(m) => assert!(m.contains("invalid lot")),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
