//! `hedge-broker-zerodha` — [`BrokerAdapter`] implementation against
//! Zerodha Kite Connect v3 (<https://kite.trade/docs/connect/v3/>).
//!
//! ### Layout
//!
//! * [`client`] — async REST wrapper around `reqwest::Client`. Implements
//!   `place_order`, `modify_order`, `cancel_order`, `order_status`, and a
//!   `ping_user_profile` liveness probe.
//! * [`translator`] — `OrderIntent` → Kite form translator.
//! * This module — composes the two into a [`ZerodhaBroker`] that satisfies
//!   the [`BrokerAdapter`] trait.
//!
//! ### Hot_Path discipline
//!
//! * **Async only.** No `reqwest::blocking`. The `forbid_modules` CI gate
//!   (task 8.1) verifies this in the transitive dep closure.
//! * Every `submit / modify / cancel / status / ready` request emits a
//!   [`BrokerMetric`] on `broker.metric.zerodha` (R7.4).
//! * `ready()` returns [`ReadyState::ConfigError`] when credentials are
//!   missing or invalid; `submit()` then fails closed with
//!   [`BrokerError::NotReady`] (R7.5).
//!
//! ### Production gaps
//!
//! The Kite **WebSocket binary tick protocol** used for real-time market
//! data lives in `hedge-market-data`, not here. The post-trade
//! confirmation push (binary frame on the same WebSocket) is also
//! market-data territory. Where Kite's REST surface has insufficient
//! public documentation (some advanced order varieties, post-trade
//! settlement messages), we leave a `// TODO: production protocol`
//! marker.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod translator;

use std::sync::Arc;

use async_trait::async_trait;
use hedge_broker_api::{
    broker_id_token, BrokerAdapter, BrokerError, BrokerMetric, BrokerOp, MetricPublisher,
    OrderIntent, OrderModification, OrderStatus, ReadyState, SubmitAck, VecMetricRecorder,
};
use hedge_core::{now_ns, BrokerId, CorrelationId, Qty};
use hedge_schemas::order_state::OrderLifecycleState;
use parking_lot::RwLock;

pub use crate::client::{KiteClient, KiteCredentials, KiteOrderHistory, KITE_API_BASE};
pub use crate::translator::{
    default_symbol_resolver, intent_to_kite_form, modification_to_kite_form,
    paise_to_rupee_string, KiteModifyOrderForm, KitePlaceOrderForm,
};

/// Default order variety used by the Hot_Path (`regular`).
pub const DEFAULT_VARIETY: &str = "regular";

/// Zerodha Kite Connect adapter.
pub struct ZerodhaBroker {
    client: KiteClient,
    publisher: Arc<dyn MetricPublisher>,
    /// Lazily-cached `ready` snapshot. Refreshed on `ready()` calls and
    /// downgraded to `Disconnected` on transient transport failures so
    /// the BrokerRouter can short-circuit subsequent `submit` calls
    /// without retrying.
    ready_state: RwLock<ReadyState>,
    variety: String,
}

impl ZerodhaBroker {
    /// Construct from explicit credentials and a metric publisher.
    pub fn new(
        creds: KiteCredentials,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        Self::with_base_url(creds, KITE_API_BASE, publisher)
    }

    /// Construct against a custom base URL (used by tests).
    pub fn with_base_url(
        creds: KiteCredentials,
        base_url: impl Into<String>,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        // Pre-validate credentials so missing values fail closed at
        // construction (R7.5). The adapter is constructable with empty
        // creds for tests that exercise `ready() == ConfigError`, but
        // by default we surface the configuration problem immediately
        // through the cached `ready_state`.
        let initial = match creds.validate() {
            Ok(()) => ReadyState::Disconnected("not yet probed".into()),
            Err(reason) => ReadyState::ConfigError(reason.into()),
        };
        let client = KiteClient::with_base(creds, base_url)?;
        Ok(Self {
            client,
            publisher,
            ready_state: RwLock::new(initial),
            variety: DEFAULT_VARIETY.into(),
        })
    }

    /// Convenience constructor: install an in-memory [`VecMetricRecorder`]
    /// so tests can assert on emitted metrics without setting up NATS.
    pub fn with_recorder(
        creds: KiteCredentials,
    ) -> Result<(Self, Arc<VecMetricRecorder>), BrokerError> {
        let recorder = Arc::new(VecMetricRecorder::new());
        let pub_dyn: Arc<dyn MetricPublisher> = recorder.clone();
        let b = Self::new(creds, pub_dyn)?;
        Ok((b, recorder))
    }

    /// Override the order variety (default `"regular"`). Used by callers
    /// that want to place CO/AMO orders.
    pub fn set_variety(&mut self, variety: impl Into<String>) {
        self.variety = variety.into();
    }

    /// Snapshot the current cached ready state.
    pub fn ready_snapshot(&self) -> ReadyState {
        self.ready_state.read().clone()
    }

    /// Internal: stamp a [`BrokerMetric`] and publish it.
    async fn record_metric(
        &self,
        op: BrokerOp,
        cid: CorrelationId,
        start_ns: u64,
        result: &Result<(), BrokerError>,
    ) {
        let latency_ns = now_ns().saturating_sub(start_ns);
        let (error, http_status) = match result {
            Ok(()) => (false, None),
            Err(BrokerError::Http { status, .. }) => (true, Some(*status)),
            Err(_) => (true, None),
        };
        let metric = BrokerMetric {
            broker_id: BrokerId::Zerodha,
            correlation_id: cid,
            op,
            latency_ns,
            error,
            http_status,
            ts_ns: now_ns(),
        };
        self.publisher.publish(metric).await;
    }

    /// Update the cached ready state in response to an operation result.
    /// Auth or NotReady errors lock the state into `ConfigError`;
    /// network/transient errors transition to `Disconnected`; any
    /// successful network round-trip restores `Ready`.
    fn update_ready_from_result<T>(&self, result: &Result<T, BrokerError>) {
        let mut g = self.ready_state.write();
        match result {
            Ok(_) => *g = ReadyState::Ready,
            Err(BrokerError::Auth(s)) => *g = ReadyState::ConfigError(s.clone()),
            Err(BrokerError::NotReady(s)) => *g = ReadyState::ConfigError(s.clone()),
            Err(BrokerError::Network(s)) | Err(BrokerError::Transient(s)) => {
                // Only override Ready/Disconnected. Don't downgrade a
                // ConfigError caused by missing creds because the
                // network failure is a downstream symptom.
                if !matches!(*g, ReadyState::ConfigError(_)) {
                    *g = ReadyState::Disconnected(s.clone());
                }
            }
            Err(_) => { /* leave state unchanged; broker rejected request */ }
        }
    }

    /// Quick guard called at the top of `submit`: if creds are clearly
    /// invalid, fail closed without making the network call.
    fn fail_closed_if_misconfigured(&self) -> Result<(), BrokerError> {
        let snap = self.ready_state.read().clone();
        if let ReadyState::ConfigError(reason) = snap {
            return Err(BrokerError::NotReady(reason));
        }
        Ok(())
    }
}

/// Map a Kite status string to the canonical FSM state.
///
/// Kite values seen in the wild: `OPEN`, `COMPLETE`, `CANCELLED`,
/// `REJECTED`, `TRIGGER PENDING`, `OPEN_PENDING`, `MODIFY_PENDING`,
/// `CANCEL_PENDING`. Anything not recognised is conservatively mapped
/// to `Submitted` so the engine continues to track the order.
pub fn kite_status_to_fsm(status: &str) -> OrderLifecycleState {
    match status {
        "COMPLETE" => OrderLifecycleState::Filled,
        "CANCELLED" => OrderLifecycleState::Cancelled,
        "REJECTED" => OrderLifecycleState::Rejected,
        // Pending / open variants — order is still working.
        "OPEN" | "TRIGGER PENDING" | "OPEN_PENDING" | "MODIFY_PENDING" | "CANCEL_PENDING" => {
            OrderLifecycleState::Submitted
        }
        _ => OrderLifecycleState::Submitted,
    }
}

/// Map a Kite `KiteOrderHistory` to an `OrderStatus`. The avg price
/// arrives in rupees (decimal); we round-trip through `i64` paise.
pub fn order_history_to_status(history: KiteOrderHistory) -> OrderStatus {
    let state = kite_status_to_fsm(&history.status);
    // Round to nearest paise to avoid drift from f64 representation.
    let avg_paise = (history.average_price * 100.0).round() as i64;
    OrderStatus {
        broker_order_id: history.order_id,
        state,
        filled_qty: Qty::new(history.filled_quantity),
        avg_fill_paise: avg_paise,
        broker_ts_ns: None,
    }
}

#[async_trait]
impl BrokerAdapter for ZerodhaBroker {
    fn broker_id(&self) -> BrokerId {
        BrokerId::Zerodha
    }

    async fn submit(&self, intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
        let start = now_ns();
        let cid = intent.correlation_id;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Submit, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let form = intent_to_kite_form(intent, default_symbol_resolver);
        let result = self.client.place_order(&self.variety, &form).await;
        self.update_ready_from_result(&result);

        let metric_view: Result<(), BrokerError> = match &result {
            Ok(_) => Ok(()),
            Err(e) => Err(e.clone()),
        };
        self.record_metric(BrokerOp::Submit, cid, start, &metric_view).await;

        result.map(|order_id| SubmitAck {
            broker_order_id: order_id,
            broker_ts_ns: None,
        })
    }

    async fn modify(&self, modification: &OrderModification) -> Result<(), BrokerError> {
        let start = now_ns();
        let cid = CorrelationId::NIL; // Modify path has no upstream cid.

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Modify, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let form = modification_to_kite_form(modification);
        let result = self
            .client
            .modify_order(&self.variety, &modification.broker_order_id, &form)
            .await;
        self.update_ready_from_result(&result);
        self.record_metric(BrokerOp::Modify, cid, start, &result).await;
        result
    }

    async fn cancel(&self, broker_order_id: &str) -> Result<(), BrokerError> {
        let start = now_ns();
        let cid = CorrelationId::NIL;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Cancel, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let result = self.client.cancel_order(&self.variety, broker_order_id).await;
        self.update_ready_from_result(&result);
        self.record_metric(BrokerOp::Cancel, cid, start, &result).await;
        result
    }

    async fn status(&self, broker_order_id: &str) -> Result<OrderStatus, BrokerError> {
        let start = now_ns();
        let cid = CorrelationId::NIL;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Status, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let result = self.client.order_status(broker_order_id).await;
        self.update_ready_from_result(&result);
        let metric_view: Result<(), BrokerError> = match &result {
            Ok(_) => Ok(()),
            Err(e) => Err(e.clone()),
        };
        self.record_metric(BrokerOp::Status, cid, start, &metric_view).await;
        result.map(order_history_to_status)
    }

    async fn metrics(&self) -> Vec<BrokerMetric> {
        Vec::new()
    }

    async fn ready(&self) -> ReadyState {
        let start = now_ns();
        // If creds are missing, short-circuit without a network call.
        if let Err(reason) = self.client.credentials().validate() {
            let snap = ReadyState::ConfigError(reason.into());
            *self.ready_state.write() = snap.clone();
            self.record_metric(BrokerOp::Ready, CorrelationId::NIL, start, &Err(BrokerError::NotReady(reason.into()))).await;
            return snap;
        }
        // Otherwise probe `/user/profile` to confirm the access token.
        let probe = self.client.ping_user_profile().await;
        self.update_ready_from_result(&probe);
        let snap = self.ready_state.read().clone();
        let metric_view: Result<(), BrokerError> = match &probe {
            Ok(()) => Ok(()),
            Err(e) => Err(e.clone()),
        };
        self.record_metric(BrokerOp::Ready, CorrelationId::NIL, start, &metric_view).await;
        snap
    }
}

/// Subject string the Zerodha adapter publishes metrics on.
#[inline]
pub fn metric_subject() -> String {
    format!("broker.metric.{}", broker_id_token(BrokerId::Zerodha))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ready_with_empty_creds_is_config_error_and_submit_fails_closed() {
        let creds = KiteCredentials::new("", "");
        let (b, _) = ZerodhaBroker::with_recorder(creds).unwrap();
        let r = b.ready().await;
        assert!(matches!(r, ReadyState::ConfigError(_)));

        // Submit must now fail with NotReady — the fail-closed contract
        // (R7.5).
        let intent = OrderIntent {
            correlation_id: CorrelationId::NIL,
            symbol_raw: 1,
            side: hedge_core::Side::Buy,
            quantity: Qty::new(1),
            order_type: hedge_broker_api::OrderType::Market,
            limit_paise: 0,
            exchange: hedge_broker_api::Exchange::Nse,
        };
        let err = b.submit(&intent).await.unwrap_err();
        assert!(matches!(err, BrokerError::NotReady(_)));
    }

    #[tokio::test]
    async fn ready_with_empty_creds_emits_metric() {
        let creds = KiteCredentials::new("", "tok");
        let (b, recorder) = ZerodhaBroker::with_recorder(creds).unwrap();
        let _ = b.ready().await;
        let snap = recorder.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].op, BrokerOp::Ready);
        assert!(snap[0].error);
        assert_eq!(snap[0].broker_id, BrokerId::Zerodha);
    }

    #[test]
    fn kite_status_to_fsm_classifies_known_values() {
        assert_eq!(
            kite_status_to_fsm("COMPLETE"),
            OrderLifecycleState::Filled
        );
        assert_eq!(
            kite_status_to_fsm("CANCELLED"),
            OrderLifecycleState::Cancelled
        );
        assert_eq!(
            kite_status_to_fsm("REJECTED"),
            OrderLifecycleState::Rejected
        );
        assert_eq!(
            kite_status_to_fsm("OPEN"),
            OrderLifecycleState::Submitted
        );
        assert_eq!(
            kite_status_to_fsm("UNKNOWN"),
            OrderLifecycleState::Submitted
        );
    }

    #[test]
    fn order_history_to_status_round_trips_avg_price() {
        let h = KiteOrderHistory {
            order_id: "abc".into(),
            status: "COMPLETE".into(),
            filled_quantity: 7,
            average_price: 123.45,
        };
        let s = order_history_to_status(h);
        assert_eq!(s.broker_order_id, "abc");
        assert_eq!(s.state, OrderLifecycleState::Filled);
        assert_eq!(s.filled_qty.raw(), 7);
        assert_eq!(s.avg_fill_paise, 12345);
    }

    #[test]
    fn metric_subject_string_is_stable() {
        assert_eq!(metric_subject(), "broker.metric.zerodha");
    }
}
