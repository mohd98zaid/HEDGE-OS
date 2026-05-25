//! Async REST client for Shoonya / Finvasia NorenAPI
//! (<https://api.shoonya.com/NorenWebApi.html>).
//!
//! ### Wire format
//!
//! NorenAPI is unusual: every endpoint receives a single
//! `application/x-www-form-urlencoded` body of the form
//! `jData=<json>&jKey=<session_token>`. The `jData` JSON is the
//! semantically-typed body; `jKey` is the session token returned by
//! the login flow.
//!
//! ### Auth
//!
//! Login uses a SHA-256 of the password + a SHA-256 of `(uid + vc + apiKey)`.
//! We surface a [`ShoonyaCredentials`] that carries the **already-derived**
//! session token (typical production flow) plus the user id; the
//! login/refresh flow itself runs out-of-process in a small helper that
//! re-authenticates daily and writes the session token to a file the
//! adapter loads at startup.
//!
//! **TODO: production protocol** — the login endpoint and binary
//! WebSocket tick protocol are out of scope for task 17.1.

use std::time::Duration;

use hedge_broker_api::BrokerError;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Production base URL.
pub const SHOONYA_API_BASE: &str = "https://api.shoonya.com/NorenWClientTP";

/// Default per-request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Static credentials.
#[derive(Clone, Debug)]
pub struct ShoonyaCredentials {
    /// User id (NorenAPI `uid`).
    pub user_id: String,
    /// Account id (often equal to `user_id`).
    pub account_id: String,
    /// Session token returned by the login flow (NorenAPI `susertoken`).
    pub session_token: String,
}

impl ShoonyaCredentials {
    /// Construct from owned strings.
    pub fn new(
        user_id: impl Into<String>,
        account_id: impl Into<String>,
        session_token: impl Into<String>,
    ) -> Self {
        Self {
            user_id: user_id.into(),
            account_id: account_id.into(),
            session_token: session_token.into(),
        }
    }

    /// Returns `Ok(())` when every field is non-empty.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.user_id.is_empty() {
            return Err("shoonya user_id is empty");
        }
        if self.account_id.is_empty() {
            return Err("shoonya account_id is empty");
        }
        if self.session_token.is_empty() {
            return Err("shoonya session_token is empty");
        }
        Ok(())
    }
}

/// Compute the canonical Shoonya app-key digest used in some flows:
/// `SHA256(uid + "|" + api_key)` hex-lowercase. Surfaced as a free
/// function so production callers can use it without pulling in
/// `sha2` themselves.
pub fn app_key_digest(uid: &str, api_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(uid.as_bytes());
    hasher.update(b"|");
    hasher.update(api_key.as_bytes());
    let digest = hasher.finalize();
    hex::encode(digest)
}

/// Async REST client.
pub struct ShoonyaClient {
    inner: Client,
    base_url: String,
    creds: ShoonyaCredentials,
}

impl ShoonyaClient {
    /// Construct against the production base URL.
    pub fn new(creds: ShoonyaCredentials) -> Result<Self, BrokerError> {
        Self::with_base(creds, SHOONYA_API_BASE)
    }

    /// Construct against a custom base URL.
    pub fn with_base(
        creds: ShoonyaCredentials,
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
    pub fn credentials(&self) -> &ShoonyaCredentials {
        &self.creds
    }

    /// Borrow the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn default_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        h
    }

    /// Build the form body for a NorenAPI POST: `jData=<json>&jKey=<token>`.
    /// We hand-build the form body because `reqwest::form()` would
    /// require a `&[(K, V)]` slice and Shoonya's quirk of nesting JSON
    /// under a single key makes a typed `Serialize` form awkward.
    fn body_for<J: serde::Serialize + ?Sized>(&self, j_data: &J) -> Result<String, BrokerError> {
        let json = serde_json::to_string(j_data)
            .map_err(|e| BrokerError::Internal(format!("encode jData: {e}")))?;
        Ok(url::form_urlencoded::Serializer::new(String::new())
            .append_pair("jData", &json)
            .append_pair("jKey", &self.creds.session_token)
            .finish())
    }

    /// `POST /PlaceOrder` — place a new order.
    pub async fn place_order<J: serde::Serialize + ?Sized>(
        &self,
        j_data: &J,
    ) -> Result<String, BrokerError> {
        let url = format!("{}/PlaceOrder", self.base_url);
        let body = self.body_for(j_data)?;
        let resp = self
            .inner
            .post(&url)
            .headers(Self::default_headers())
            .body(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: ShoonyaPlaceOrderResponse = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode shoonya place_order: {e} body={text}")))?;
        parsed.into_result()
    }

    /// `POST /ModifyOrder` — modify a working order.
    pub async fn modify_order<J: serde::Serialize + ?Sized>(
        &self,
        j_data: &J,
    ) -> Result<(), BrokerError> {
        let url = format!("{}/ModifyOrder", self.base_url);
        let body = self.body_for(j_data)?;
        let resp = self
            .inner
            .post(&url)
            .headers(Self::default_headers())
            .body(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: ShoonyaSimpleStatResponse = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode shoonya modify: {e} body={text}")))?;
        parsed.into_result()
    }

    /// `POST /CancelOrder` — cancel a working order.
    pub async fn cancel_order(&self, norenordno: &str) -> Result<(), BrokerError> {
        let url = format!("{}/CancelOrder", self.base_url);
        let body = self.body_for(&serde_json::json!({
            "uid": self.creds.user_id,
            "norenordno": norenordno,
        }))?;
        let resp = self
            .inner
            .post(&url)
            .headers(Self::default_headers())
            .body(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: ShoonyaSimpleStatResponse = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode shoonya cancel: {e} body={text}")))?;
        parsed.into_result()
    }

    /// `POST /SingleOrdHist` — order history; the most recent entry is
    /// the current state.
    pub async fn order_status(
        &self,
        norenordno: &str,
    ) -> Result<ShoonyaOrderStatusResponse, BrokerError> {
        let url = format!("{}/SingleOrdHist", self.base_url);
        let body = self.body_for(&serde_json::json!({
            "uid": self.creds.user_id,
            "norenordno": norenordno,
        }))?;
        let resp = self
            .inner
            .post(&url)
            .headers(Self::default_headers())
            .body(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.map_err(map_network_err)?;
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        // Shoonya returns either a single object for an error or an
        // array of order-state entries. Try array first.
        if let Ok(arr) = serde_json::from_str::<Vec<ShoonyaOrderStatusResponse>>(&text) {
            return arr
                .into_iter()
                .last()
                .ok_or_else(|| BrokerError::UnknownOrderId(norenordno.to_owned()));
        }
        // Fall back to the error object shape.
        let err: ShoonyaSimpleStatResponse = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode shoonya status: {e} body={text}")))?;
        match err.into_result() {
            Ok(()) => Err(BrokerError::Internal(format!(
                "shoonya order status returned ok with no entries: {text}"
            ))),
            Err(e) => Err(e),
        }
    }

    /// Liveness probe: `POST /UserDetails`. Cheap and confirms the
    /// session token is accepted.
    pub async fn ping_user_details(&self) -> Result<(), BrokerError> {
        let url = format!("{}/UserDetails", self.base_url);
        let body = self.body_for(&serde_json::json!({
            "uid": self.creds.user_id,
        }))?;
        let resp = self
            .inner
            .post(&url)
            .headers(Self::default_headers())
            .body(body)
            .send()
            .await
            .map_err(map_network_err)?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(map_status(status.as_u16(), &text));
        }
        let parsed: ShoonyaSimpleStatResponse = serde_json::from_str(&text)
            .map_err(|e| BrokerError::Internal(format!("decode shoonya user details: {e} body={text}")))?;
        parsed.into_result()
    }
}

/// `PlaceOrder` response shape.
#[derive(Clone, Debug, Deserialize)]
pub struct ShoonyaPlaceOrderResponse {
    /// `"Ok"` on success, `"Not_Ok"` on failure.
    pub stat: String,
    /// Broker order id (only present on success).
    #[serde(default)]
    pub norenordno: Option<String>,
    /// Error message on failure.
    #[serde(default)]
    pub emsg: Option<String>,
}

impl ShoonyaPlaceOrderResponse {
    fn into_result(self) -> Result<String, BrokerError> {
        if self.stat == "Ok" {
            self.norenordno
                .ok_or_else(|| BrokerError::Internal("shoonya success without norenordno".into()))
        } else {
            Err(BrokerError::Rejected(
                self.emsg.unwrap_or_else(|| self.stat.clone()),
            ))
        }
    }
}

/// Generic `{stat, emsg}` envelope used by modify/cancel.
#[derive(Clone, Debug, Deserialize)]
pub struct ShoonyaSimpleStatResponse {
    /// `"Ok"` on success.
    pub stat: String,
    /// Optional error message.
    #[serde(default)]
    pub emsg: Option<String>,
}

impl ShoonyaSimpleStatResponse {
    fn into_result(self) -> Result<(), BrokerError> {
        if self.stat == "Ok" {
            Ok(())
        } else {
            Err(BrokerError::Rejected(
                self.emsg.unwrap_or_else(|| self.stat.clone()),
            ))
        }
    }
}

/// `SingleOrdHist` response entry. The status is one of `OPEN`,
/// `COMPLETE`, `CANCELED` (single-l), `REJECTED`, `TRIGGER_PENDING`.
#[derive(Clone, Debug, Deserialize)]
pub struct ShoonyaOrderStatusResponse {
    /// `"Ok"` / `"Not_Ok"`.
    #[serde(default)]
    pub stat: Option<String>,
    /// Order number.
    #[serde(default)]
    pub norenordno: Option<String>,
    /// Order status string.
    #[serde(default)]
    pub status: Option<String>,
    /// Filled quantity (string in the wire payload).
    #[serde(default)]
    pub fillshares: Option<String>,
    /// Volume-weighted average fill price (string).
    #[serde(default)]
    pub avgprc: Option<String>,
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
        assert!(ShoonyaCredentials::new("", "", "").validate().is_err());
        assert!(ShoonyaCredentials::new("u", "", "tok").validate().is_err());
        assert!(ShoonyaCredentials::new("u", "a", "").validate().is_err());
        assert!(ShoonyaCredentials::new("u", "a", "tok").validate().is_ok());
    }

    #[test]
    fn app_key_digest_is_deterministic() {
        let a = app_key_digest("USER1", "apikey");
        let b = app_key_digest("USER1", "apikey");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64); // SHA-256 hex
    }

    #[test]
    fn map_status_classifies_correctly() {
        assert!(matches!(map_status(401, "x"), BrokerError::Auth(_)));
        assert!(matches!(map_status(429, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(503, "x"), BrokerError::Transient(_)));
        assert!(matches!(map_status(400, "x"), BrokerError::Rejected(_)));
        assert!(matches!(map_status(418, "x"), BrokerError::Http { .. }));
    }

    #[test]
    fn place_order_response_success_decodes() {
        let r: ShoonyaPlaceOrderResponse =
            serde_json::from_str(r#"{"stat":"Ok","norenordno":"ABC-1"}"#).unwrap();
        assert_eq!(r.into_result().unwrap(), "ABC-1");
    }

    #[test]
    fn place_order_response_failure_decodes() {
        let r: ShoonyaPlaceOrderResponse =
            serde_json::from_str(r#"{"stat":"Not_Ok","emsg":"Insufficient margin"}"#).unwrap();
        match r.into_result().unwrap_err() {
            BrokerError::Rejected(m) => assert!(m.contains("margin")),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn simple_stat_response_handles_both_cases() {
        let ok: ShoonyaSimpleStatResponse = serde_json::from_str(r#"{"stat":"Ok"}"#).unwrap();
        ok.into_result().unwrap();
        let not_ok: ShoonyaSimpleStatResponse =
            serde_json::from_str(r#"{"stat":"Not_Ok","emsg":"order not found"}"#).unwrap();
        match not_ok.into_result().unwrap_err() {
            BrokerError::Rejected(m) => assert!(m.contains("not found")),
            _ => panic!("wrong variant"),
        }
    }
}
