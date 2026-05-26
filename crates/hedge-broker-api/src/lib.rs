//! `hedge-broker-api` — the [`BrokerAdapter`] trait and the small set of
//! shared types every concrete adapter implements against (R7.1–R7.5,
//! R22.4; design § Components § Broker_Adapter Abstraction).
//!
//! ### Why a separate crate
//!
//! The `BrokerAdapter` trait is the seam between the Execution_Engine
//! (`hedge-exec`) and the per-broker crates (`hedge-broker-zerodha`,
//! `hedge-broker-dhan`, `hedge-broker-shoonya`, `hedge-broker-angelone`,
//! `hedge-broker-simulated`). Both sides depend on this crate; neither
//! the engine nor any broker depends on a sibling broker. That keeps the
//! per-broker crates fully replaceable and lets the workspace add a new
//! broker without touching `hedge-exec` (R7.1).
//!
//! ### Hot_Path discipline
//!
//! This crate has **no** transitive dependency on `pyo3`, `numpy`,
//! `pandas`, `reqwest::blocking`, or any cloud LLM SDK (R30.6, R30.7,
//! R30.8). The `forbid_modules` CI gate (task 8.1) enforces the closure
//! check; the dependency list in `Cargo.toml` is intentionally minimal.
//!
//! ### Public surface
//!
//! * [`BrokerAdapter`] — `async_trait` defining `submit`, `modify`,
//!   `cancel`, `status`, `metrics`, `ready` and `broker_id`.
//! * [`BrokerError`] — error taxonomy mapped from broker-specific HTTP
//!   responses to a stable variant set the Execution_Engine can route on
//!   (retryable vs fatal vs config).
//! * [`BrokerMetric`] — the `broker.metric.<broker>` payload published
//!   on every broker request (latency in nanoseconds, error flag,
//!   broker id, correlation id) (R7.4).
//! * [`ReadyState`] — `Ready`, `ConfigError(reason)`, `Disconnected`. On
//!   startup an adapter with missing or invalid credentials must return
//!   [`ReadyState::ConfigError`] and [`BrokerAdapter::submit`] must fail
//!   closed with [`BrokerError::NotReady`] (R7.5).
//! * [`OrderModification`] — typed input to `modify()`.
//! * [`OrderStatus`] — typed read-side projection returned by `status()`.
//!
//! Each concrete adapter owns its own `OrderIntent` translation
//! (FlatBuffers `OrderIntent_v1` → broker-specific REST/WebSocket
//! payload) so this crate stays small and trait-only.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt;

use async_trait::async_trait;
use hedge_core::{BrokerId, CorrelationId, Qty};
use hedge_schemas::order_state::OrderLifecycleState;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// Re-export `Side` so adapters that import from this crate get a single
// import surface; the wire payload still comes from `hedge_core`.
pub use hedge_core::Side;

// ---------------------------------------------------------------------------
// Order intent — broker-agnostic projection of OrderIntent_v1 ---------------
// ---------------------------------------------------------------------------

/// Order type, mirroring `OrderIntent_v1.order_type` (`0 = Market`,
/// `1 = Limit`).
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    /// Market order — broker fills at the prevailing price.
    Market = 0,
    /// Limit order — broker holds at `limit_paise` until matched or cancelled.
    Limit = 1,
}

impl OrderType {
    /// Reconstruct from the wire byte. Returns `None` for unknown values.
    #[inline]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Market),
            1 => Some(Self::Limit),
            _ => None,
        }
    }

    /// Wire byte.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Exchange selector; mirrors `OrderIntent_v1.exchange` (`0 = NSE`,
/// `1 = BSE`).
#[repr(i8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    /// National Stock Exchange of India.
    Nse = 0,
    /// Bombay Stock Exchange.
    Bse = 1,
}

impl Exchange {
    /// Reconstruct from the wire byte.
    #[inline]
    pub const fn from_i8(byte: i8) -> Option<Self> {
        match byte {
            0 => Some(Self::Nse),
            1 => Some(Self::Bse),
            _ => None,
        }
    }

    /// Wire byte.
    #[inline]
    pub const fn as_i8(self) -> i8 {
        self as i8
    }

    /// Canonical short tag used in REST requests (`"NSE"` / `"BSE"`).
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nse => "NSE",
            Self::Bse => "BSE",
        }
    }
}

/// Broker-agnostic order intent.
///
/// This is the value the Execution_Engine constructs from a verified
/// `OrderIntent_v1` (after `ApprovalToken` HMAC verification) and hands
/// to the adapter via [`BrokerAdapter::submit`]. Each adapter then
/// projects it to its broker-specific REST payload through a
/// `translator` module (see e.g. `hedge_broker_zerodha::translator`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    /// End-to-end correlation id (R27.4).
    pub correlation_id: CorrelationId,
    /// Interned symbol id; the adapter's symbol resolver maps this to the
    /// broker's tradingsymbol on the wire.
    pub symbol_raw: u32,
    /// Buy / Sell.
    pub side: Side,
    /// Quantity (units / contracts).
    pub quantity: Qty,
    /// Market vs Limit.
    pub order_type: OrderType,
    /// Limit price in paise. Ignored for market orders.
    pub limit_paise: i64,
    /// NSE / BSE.
    pub exchange: Exchange,
}

// ---------------------------------------------------------------------------
// Order modification --------------------------------------------------------
// ---------------------------------------------------------------------------

/// Subset of an existing working order that the Execution_Engine may
/// adjust without cancelling and re-submitting.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderModification {
    /// Broker-side order id (returned by [`BrokerAdapter::submit`] on
    /// the [`SubmitAck`]).
    pub broker_order_id: String,
    /// New quantity, if changing.
    pub new_quantity: Option<Qty>,
    /// New limit price in paise, if changing.
    pub new_limit_paise: Option<i64>,
}

// ---------------------------------------------------------------------------
// Submit acknowledgement and status read -----------------------------------
// ---------------------------------------------------------------------------

/// Response from a successful [`BrokerAdapter::submit`]. Equivalent to a
/// `Submitted` transition on the FSM.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitAck {
    /// Broker-assigned order id; opaque string, used as the key for
    /// later modify / cancel / status calls.
    pub broker_order_id: String,
    /// Wall-clock timestamp the broker reports as the submit time, if
    /// any. `None` when the broker does not surface this.
    pub broker_ts_ns: Option<u64>,
}

/// Read-side projection of an order's broker-side state. Returned by
/// [`BrokerAdapter::status`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderStatus {
    /// Broker order id this status refers to.
    pub broker_order_id: String,
    /// Lifecycle state classified into the FSM (R6.3).
    pub state: OrderLifecycleState,
    /// Filled quantity to date.
    pub filled_qty: Qty,
    /// Volume-weighted average fill price in paise. Zero before any fill.
    pub avg_fill_paise: i64,
    /// Optional broker-side timestamp (ns).
    pub broker_ts_ns: Option<u64>,
}

// ---------------------------------------------------------------------------
// Errors --------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Adapter error taxonomy. Variants are deliberately stable so the
/// Execution_Engine's retry-vs-failover policy can route on the variant
/// without parsing strings.
#[derive(Clone, Debug, Error, Serialize, Deserialize)]
pub enum BrokerError {
    /// Adapter is in [`ReadyState::ConfigError`] — missing or invalid
    /// credentials. **Fail-closed.** R7.5.
    #[error("broker not ready: {0}")]
    NotReady(String),

    /// Broker rejected the order outright (validation, lot-size, margin,
    /// etc.). Not retryable. The string is the broker-provided reason.
    #[error("broker rejected: {0}")]
    Rejected(String),

    /// Broker accepted the request but the response indicated transient
    /// failure (rate limit, timeout, 5xx). Retryable per R6.4.
    #[error("broker transient error: {0}")]
    Transient(String),

    /// Network-level failure (TCP reset, DNS, TLS handshake, etc.).
    /// Retryable.
    #[error("network error: {0}")]
    Network(String),

    /// HTTP error not falling into the retryable buckets above.
    #[error("http {status}: {body}")]
    Http {
        /// HTTP status code reported by the broker.
        status: u16,
        /// Best-effort body text; truncated by the adapter for logs.
        body: String,
    },

    /// Authentication / authorization failure (401, 403, expired token).
    /// **Not** retried: the bus is alerted via `obs.error.broker.<id>`
    /// and the active broker is marked unhealthy.
    #[error("auth failure: {0}")]
    Auth(String),

    /// `ApprovalToken` HMAC could not be verified at the adapter
    /// boundary. The Execution_Engine verifies first; this variant
    /// exists for defence in depth so an adapter that is wired up
    /// outside the engine still fails closed.
    #[error("invalid approval token")]
    InvalidApprovalToken,

    /// Order modification or cancellation referenced a `broker_order_id`
    /// the adapter has no record of.
    #[error("unknown order id: {0}")]
    UnknownOrderId(String),

    /// Internal adapter invariant violated. Indicates a bug in the
    /// adapter; the engine treats this as fatal for the affected
    /// correlation id.
    #[error("internal adapter error: {0}")]
    Internal(String),
}

impl BrokerError {
    /// Whether the Execution_Engine should retry the same request after
    /// backoff. Mirrors the policy in R6.4.
    #[inline]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient(_) | Self::Network(_))
    }

    /// Whether this error should count toward the broker-failover
    /// sliding-window error rate (R6.5). Auth and config errors are
    /// excluded — they indicate the broker is mis-configured rather
    /// than degraded, and failover is not the right response.
    #[inline]
    pub const fn counts_toward_failover(&self) -> bool {
        matches!(
            self,
            Self::Transient(_) | Self::Network(_) | Self::Http { .. } | Self::Rejected(_)
        )
    }
}

// ---------------------------------------------------------------------------
// Ready state ---------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Adapter readiness as reported by [`BrokerAdapter::ready`].
///
/// On startup, if credentials are missing or invalid, an adapter MUST
/// return [`ReadyState::ConfigError`] and [`BrokerAdapter::submit`] MUST
/// return [`BrokerError::NotReady`] (R7.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadyState {
    /// Credentials present, network reachable, ready to accept orders.
    Ready,
    /// Credentials missing, malformed, or rejected by the broker. The
    /// inner string is a human-readable reason surfaced through
    /// `obs.error.broker.<id>`. Adapters in this state fail closed.
    ConfigError(String),
    /// Credentials are valid but the network connection has dropped.
    /// The Execution_Engine treats this like
    /// [`BrokerError::Network`] for routing purposes.
    Disconnected(String),
}

impl ReadyState {
    /// Convenience: `true` only for [`ReadyState::Ready`].
    #[inline]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Convenience: `true` for any non-[`ReadyState::Ready`] state.
    #[inline]
    pub const fn is_unready(&self) -> bool {
        !self.is_ready()
    }
}

impl fmt::Display for ReadyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => f.write_str("Ready"),
            Self::ConfigError(r) => write!(f, "ConfigError({})", r),
            Self::Disconnected(r) => write!(f, "Disconnected({})", r),
        }
    }
}

// ---------------------------------------------------------------------------
// BrokerMetric --------------------------------------------------------------
// ---------------------------------------------------------------------------

/// One metric record published on `broker.metric.<broker>` after every
/// adapter request (R7.4).
///
/// The Execution_Engine consumes this stream to drive the failover
/// policy (R6.5) and the Risk_Engine consumes it for the broker-latency
/// block gate (R5.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerMetric {
    /// Which broker the metric refers to.
    pub broker_id: BrokerId,
    /// Correlation id this request was made under, when one applies.
    /// `CorrelationId::NIL` for periodic ready-checks that have no
    /// originating order intent.
    pub correlation_id: CorrelationId,
    /// Operation that produced the metric (`submit` / `modify` / `cancel`
    /// / `status` / `ready`).
    pub op: BrokerOp,
    /// Wall-clock latency of the request in nanoseconds.
    pub latency_ns: u64,
    /// `true` iff the request returned [`Result::Err`].
    pub error: bool,
    /// HTTP status when the underlying transport reported one. `None`
    /// for transport-level failures (TCP reset, DNS, etc.) and for
    /// in-process simulated paths.
    pub http_status: Option<u16>,
    /// Monotonic timestamp at metric emission (`hedge_core::now_ns()`).
    pub ts_ns: u64,
}

/// Adapter operation discriminant carried by [`BrokerMetric::op`].
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BrokerOp {
    /// `submit()` — place a new order.
    Submit = 0,
    /// `modify()` — adjust a working order.
    Modify = 1,
    /// `cancel()` — cancel a working order.
    Cancel = 2,
    /// `status()` — read the current state of an order.
    Status = 3,
    /// `ready()` — readiness check.
    Ready = 4,
}

impl BrokerOp {
    /// Stable canonical string used as a metric label.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submit => "submit",
            Self::Modify => "modify",
            Self::Cancel => "cancel",
            Self::Status => "status",
            Self::Ready => "ready",
        }
    }
}

impl fmt::Display for BrokerOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Build a NATS-style `broker.metric.<broker>` subject string from a
/// [`BrokerId`]. Adapters call this once at construction and cache the
/// resulting `String`.
#[inline]
pub fn broker_metric_subject(broker: BrokerId) -> String {
    format!("broker.metric.{}", broker_id_token(broker))
}

/// Canonical short token for a broker, matching `BrokerConfig`'s YAML
/// names so the same string is used across config, NATS subjects, and
/// metric labels.
#[inline]
pub const fn broker_id_token(broker: BrokerId) -> &'static str {
    match broker {
        BrokerId::Zerodha => "zerodha",
        BrokerId::Dhan => "dhan",
        BrokerId::Shoonya => "shoonya",
        BrokerId::AngelOne => "angel_one",
        BrokerId::Upstox => "upstox",
        BrokerId::Simulated => "simulated",
    }
}

// ---------------------------------------------------------------------------
// Metric publisher trait — pluggable transport ------------------------------
// ---------------------------------------------------------------------------

/// Pluggable metric publisher. Adapters take `&dyn MetricPublisher` so
/// tests can pass an in-memory recorder while production wires
/// [`hedge_bus::NatsPublisher`] (or equivalent).
///
/// Implementations must be cheap to call and **must not block**; the
/// canonical NATS implementation does an async-fire-and-forget publish
/// from a small buffer.
#[async_trait]
pub trait MetricPublisher: Send + Sync {
    /// Publish a single broker metric. Errors are logged by the
    /// implementation; this method intentionally returns `()` so a
    /// failing telemetry path never blocks order flow.
    async fn publish(&self, metric: BrokerMetric);
}

/// In-memory recorder used by tests (and as the default no-op when no
/// transport is wired up).
#[derive(Default)]
pub struct VecMetricRecorder {
    /// Stored metrics, in publish order.
    pub records: tokio::sync::Mutex<Vec<BrokerMetric>>,
}

impl VecMetricRecorder {
    /// Construct an empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current records.
    pub async fn snapshot(&self) -> Vec<BrokerMetric> {
        self.records.lock().await.clone()
    }
}

#[async_trait]
impl MetricPublisher for VecMetricRecorder {
    async fn publish(&self, metric: BrokerMetric) {
        self.records.lock().await.push(metric);
    }
}

// ---------------------------------------------------------------------------
// BrokerAdapter trait -------------------------------------------------------
// ---------------------------------------------------------------------------

/// The single seam between [`hedge_exec::BrokerRouter`] and a concrete
/// broker integration.
///
/// Every method is async because every operation crosses an external
/// boundary (HTTPS for live brokers; an in-process sleep for the
/// simulated adapter). Implementations must:
///
/// * Verify any caller-side approval before acting (the Execution_Engine
///   verifies the `ApprovalToken` HMAC; defensively, adapters that are
///   wired up out-of-band should still fail closed if a token is
///   missing).
/// * Emit a [`BrokerMetric`] on `broker.metric.<broker>` for **every**
///   request, success or failure (R7.4).
/// * Return [`BrokerError::NotReady`] if [`Self::ready`] would not
///   return [`ReadyState::Ready`] at the moment of submit (R7.5).
/// * Never panic on malformed or hostile broker responses; map them to
///   [`BrokerError`] variants instead.
#[async_trait]
pub trait BrokerAdapter: Send + Sync {
    /// Which broker this adapter speaks to.
    fn broker_id(&self) -> BrokerId;

    /// Submit a new order. On success the adapter has confirmed the
    /// broker accepted the order and assigned it a `broker_order_id`;
    /// the FSM transitions to `Submitted`.
    async fn submit(&self, intent: &OrderIntent) -> Result<SubmitAck, BrokerError>;

    /// Modify a working order. `new_quantity` and `new_limit_paise` are
    /// independently optional; passing both `None` is a no-op (the
    /// adapter SHOULD return `Ok(())` without making a network call,
    /// but is permitted to round-trip the broker for verification).
    async fn modify(&self, modification: &OrderModification) -> Result<(), BrokerError>;

    /// Cancel a working order by its broker-side id.
    async fn cancel(&self, broker_order_id: &str) -> Result<(), BrokerError>;

    /// Read the current state of an order.
    async fn status(&self, broker_order_id: &str) -> Result<OrderStatus, BrokerError>;

    /// Latest published metrics in adapter-local order. Optional —
    /// returns an empty slice when the adapter does not retain
    /// in-memory state.
    async fn metrics(&self) -> Vec<BrokerMetric>;

    /// Snapshot of adapter readiness. Cheap to call; adapters cache the
    /// result and refresh asynchronously.
    async fn ready(&self) -> ReadyState;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_type_round_trips() {
        for (raw, ty) in [(0u8, OrderType::Market), (1u8, OrderType::Limit)] {
            assert_eq!(OrderType::from_u8(raw), Some(ty));
            assert_eq!(ty.as_u8(), raw);
        }
        assert_eq!(OrderType::from_u8(2), None);
    }

    #[test]
    fn exchange_round_trips() {
        for (raw, ex) in [(0i8, Exchange::Nse), (1i8, Exchange::Bse)] {
            assert_eq!(Exchange::from_i8(raw), Some(ex));
            assert_eq!(ex.as_i8(), raw);
        }
        assert_eq!(Exchange::from_i8(2), None);
        assert_eq!(Exchange::Nse.as_str(), "NSE");
        assert_eq!(Exchange::Bse.as_str(), "BSE");
    }

    #[test]
    fn broker_id_token_is_stable() {
        assert_eq!(broker_id_token(BrokerId::Zerodha), "zerodha");
        assert_eq!(broker_id_token(BrokerId::Dhan), "dhan");
        assert_eq!(broker_id_token(BrokerId::Shoonya), "shoonya");
        assert_eq!(broker_id_token(BrokerId::AngelOne), "angel_one");
        assert_eq!(broker_id_token(BrokerId::Upstox), "upstox");
        assert_eq!(broker_id_token(BrokerId::Simulated), "simulated");
    }

    #[test]
    fn broker_metric_subject_format() {
        assert_eq!(
            broker_metric_subject(BrokerId::Zerodha),
            "broker.metric.zerodha"
        );
        assert_eq!(
            broker_metric_subject(BrokerId::Simulated),
            "broker.metric.simulated"
        );
    }

    #[test]
    fn ready_state_is_ready_helpers() {
        assert!(ReadyState::Ready.is_ready());
        assert!(!ReadyState::Ready.is_unready());
        let cfg = ReadyState::ConfigError("missing api key".into());
        assert!(!cfg.is_ready());
        assert!(cfg.is_unready());
        let disc = ReadyState::Disconnected("ws closed".into());
        assert!(!disc.is_ready());
        assert!(disc.is_unready());
    }

    #[test]
    fn broker_error_classification_matches_design() {
        // R6.4: Transient + Network are retryable. Auth + Rejected are NOT.
        assert!(BrokerError::Transient("503".into()).is_retryable());
        assert!(BrokerError::Network("dns".into()).is_retryable());
        assert!(!BrokerError::Auth("401".into()).is_retryable());
        assert!(!BrokerError::Rejected("invalid lot".into()).is_retryable());
        assert!(!BrokerError::NotReady("missing creds".into()).is_retryable());
        assert!(!BrokerError::InvalidApprovalToken.is_retryable());

        // R6.5: failover counts only network/transient/http/rejected.
        assert!(BrokerError::Transient("x".into()).counts_toward_failover());
        assert!(BrokerError::Network("x".into()).counts_toward_failover());
        assert!(BrokerError::Http { status: 500, body: "".into() }.counts_toward_failover());
        assert!(BrokerError::Rejected("x".into()).counts_toward_failover());
        assert!(!BrokerError::Auth("x".into()).counts_toward_failover());
        assert!(!BrokerError::NotReady("x".into()).counts_toward_failover());
    }

    #[tokio::test]
    async fn vec_recorder_collects_in_order() {
        let r = VecMetricRecorder::new();
        for i in 0..3u64 {
            r.publish(BrokerMetric {
                broker_id: BrokerId::Simulated,
                correlation_id: CorrelationId::NIL,
                op: BrokerOp::Submit,
                latency_ns: i,
                error: false,
                http_status: None,
                ts_ns: i * 10,
            })
            .await;
        }
        let snap = r.snapshot().await;
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].latency_ns, 0);
        assert_eq!(snap[2].latency_ns, 2);
    }
}
