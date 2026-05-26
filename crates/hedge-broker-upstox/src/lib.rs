//! `hedge-broker-upstox` — [`BrokerAdapter`] implementation against
//! Upstox API v2 (<https://upstox.com/developer/api-documentation/>).
//!
//! ### Layout
//!
//! * [`client`] — async REST wrapper around `reqwest::Client`.
//! * [`translator`] — `OrderIntent` → Upstox JSON body translator.
//! * This module — composes the two into an [`UpstoxBroker`] satisfying
//!   the [`BrokerAdapter`] trait.
//!
//! ### Auth
//!
//! Upstox uses an `Authorization: Bearer <access_token>` header where
//! `access_token` is minted daily via the OAuth login redirect flow.
//! Missing or empty credentials cause `ready()` to return
//! [`ReadyState::ConfigError`] and `submit()` to fail closed (R7.5).
//!
//! ### Production protocol gaps
//!
//! Upstox's **WebSocket binary tick protocol** for real-time market
//! data lives in `hedge-market-data`, not here. The instrument-token
//! resolver in [`translator::default_instrument_token_resolver`] is a
//! stub that renders `<EXCHANGE>_EQ|<symbol_raw>` — production wiring
//! must replace it with a real lookup against the Upstox instruments
//! dump.

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

pub use crate::client::{
    UpstoxClient, UpstoxCredentials, UpstoxOrderDetail, UpstoxOrderStatusResponse,
    UPSTOX_API_BASE,
};
pub use crate::translator::{
    default_instrument_token_resolver, intent_to_upstox_body, modification_to_upstox_body,
    paise_to_rupee_f64, UpstoxModifyOrderBody, UpstoxPlaceOrderBody,
};

/// Upstox adapter.
pub struct UpstoxBroker {
    client: UpstoxClient,
    publisher: Arc<dyn MetricPublisher>,
    ready_state: RwLock<ReadyState>,
}

impl UpstoxBroker {
    /// Construct from explicit credentials and a metric publisher.
    pub fn new(
        creds: UpstoxCredentials,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        Self::with_base_url(creds, UPSTOX_API_BASE, publisher)
    }

    /// Construct against a custom base URL (used by tests).
    pub fn with_base_url(
        creds: UpstoxCredentials,
        base_url: impl Into<String>,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        let initial = match creds.validate() {
            Ok(()) => ReadyState::Disconnected("not yet probed".into()),
            Err(reason) => ReadyState::ConfigError(reason.into()),
        };
        let client = UpstoxClient::with_base(creds, base_url)?;
        Ok(Self {
            client,
            publisher,
            ready_state: RwLock::new(initial),
        })
    }

    /// Convenience: install an in-memory metric recorder so tests can
    /// assert on emitted metrics without setting up NATS.
    pub fn with_recorder(
        creds: UpstoxCredentials,
    ) -> Result<(Self, Arc<VecMetricRecorder>), BrokerError> {
        let recorder = Arc::new(VecMetricRecorder::new());
        let pub_dyn: Arc<dyn MetricPublisher> = recorder.clone();
        let b = Self::new(creds, pub_dyn)?;
        Ok((b, recorder))
    }

    /// Snapshot the cached ready state.
    pub fn ready_snapshot(&self) -> ReadyState {
        self.ready_state.read().clone()
    }

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
            broker_id: BrokerId::Upstox,
            correlation_id: cid,
            op,
            latency_ns,
            error,
            http_status,
            ts_ns: now_ns(),
        };
        self.publisher.publish(metric).await;
    }

    fn update_ready_from_result<T>(&self, result: &Result<T, BrokerError>) {
        let mut g = self.ready_state.write();
        match result {
            Ok(_) => *g = ReadyState::Ready,
            Err(BrokerError::Auth(s)) | Err(BrokerError::NotReady(s)) => {
                *g = ReadyState::ConfigError(s.clone())
            }
            Err(BrokerError::Network(s)) | Err(BrokerError::Transient(s)) => {
                if !matches!(*g, ReadyState::ConfigError(_)) {
                    *g = ReadyState::Disconnected(s.clone());
                }
            }
            Err(_) => {}
        }
    }

    fn fail_closed_if_misconfigured(&self) -> Result<(), BrokerError> {
        let snap = self.ready_state.read().clone();
        if let ReadyState::ConfigError(reason) = snap {
            return Err(BrokerError::NotReady(reason));
        }
        Ok(())
    }
}

/// Map an Upstox order status string to the canonical FSM state.
///
/// Upstox uses lowercase / mixed-case status strings; we normalise via
/// `to_ascii_lowercase` to keep the match table small.
pub fn upstox_status_to_fsm(status: &str) -> OrderLifecycleState {
    match status.to_ascii_lowercase().trim() {
        "complete" | "filled" => OrderLifecycleState::Filled,
        "cancelled" | "expired" => OrderLifecycleState::Cancelled,
        "rejected" => OrderLifecycleState::Rejected,
        "partial filled" | "partially filled" | "partial" => {
            OrderLifecycleState::PartiallyFilled
        }
        // Anything that means "the order is at the broker but not done"
        // collapses to `Submitted`.
        "open"
        | "validation pending"
        | "put order req received"
        | "modify pending"
        | "cancel pending"
        | "trigger pending"
        | "after market order req received" => OrderLifecycleState::Submitted,
        // Conservative fallback so an unknown status keeps the FSM
        // tracking the order rather than dropping it.
        _ => OrderLifecycleState::Submitted,
    }
}

/// Project an [`UpstoxOrderStatusResponse`] to the canonical
/// [`OrderStatus`].
pub fn order_status_response_to_status(r: UpstoxOrderStatusResponse) -> OrderStatus {
    let detail = r.data;
    let state = upstox_status_to_fsm(&detail.status);
    let avg_paise = (detail.average_price * 100.0).round() as i64;
    OrderStatus {
        broker_order_id: detail.order_id,
        state,
        filled_qty: Qty::new(detail.filled_quantity),
        avg_fill_paise: avg_paise,
        broker_ts_ns: None,
    }
}

#[async_trait]
impl BrokerAdapter for UpstoxBroker {
    fn broker_id(&self) -> BrokerId {
        BrokerId::Upstox
    }

    async fn submit(&self, intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
        let start = now_ns();
        let cid = intent.correlation_id;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Submit, cid, start, &Err(e.clone()))
                .await;
            return Err(e);
        }

        let body = intent_to_upstox_body(intent, default_instrument_token_resolver);
        let result = self.client.place_order(&body).await;
        self.update_ready_from_result(&result);

        let metric_view: Result<(), BrokerError> = match &result {
            Ok(_) => Ok(()),
            Err(e) => Err(e.clone()),
        };
        self.record_metric(BrokerOp::Submit, cid, start, &metric_view)
            .await;

        result.map(|order_id| SubmitAck {
            broker_order_id: order_id,
            broker_ts_ns: None,
        })
    }

    async fn modify(&self, modification: &OrderModification) -> Result<(), BrokerError> {
        let start = now_ns();
        let cid = CorrelationId::NIL;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Modify, cid, start, &Err(e.clone()))
                .await;
            return Err(e);
        }

        let body = modification_to_upstox_body(modification);
        let result = self.client.modify_order(&body).await;
        self.update_ready_from_result(&result);
        self.record_metric(BrokerOp::Modify, cid, start, &result)
            .await;
        result
    }

    async fn cancel(&self, broker_order_id: &str) -> Result<(), BrokerError> {
        let start = now_ns();
        let cid = CorrelationId::NIL;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Cancel, cid, start, &Err(e.clone()))
                .await;
            return Err(e);
        }

        let result = self.client.cancel_order(broker_order_id).await;
        self.update_ready_from_result(&result);
        self.record_metric(BrokerOp::Cancel, cid, start, &result)
            .await;
        result
    }

    async fn status(&self, broker_order_id: &str) -> Result<OrderStatus, BrokerError> {
        let start = now_ns();
        let cid = CorrelationId::NIL;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Status, cid, start, &Err(e.clone()))
                .await;
            return Err(e);
        }

        let result = self.client.order_status(broker_order_id).await;
        self.update_ready_from_result(&result);
        let metric_view: Result<(), BrokerError> = match &result {
            Ok(_) => Ok(()),
            Err(e) => Err(e.clone()),
        };
        self.record_metric(BrokerOp::Status, cid, start, &metric_view)
            .await;
        result.map(order_status_response_to_status)
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
            self.record_metric(
                BrokerOp::Ready,
                CorrelationId::NIL,
                start,
                &Err(BrokerError::NotReady(reason.into())),
            )
            .await;
            return snap;
        }
        // Otherwise probe `/v2/user/profile` to confirm the access token.
        let probe = self.client.ping_user_profile().await;
        self.update_ready_from_result(&probe);
        let snap = self.ready_state.read().clone();
        let metric_view: Result<(), BrokerError> = match &probe {
            Ok(()) => Ok(()),
            Err(e) => Err(e.clone()),
        };
        self.record_metric(BrokerOp::Ready, CorrelationId::NIL, start, &metric_view)
            .await;
        snap
    }
}

/// Subject string the Upstox adapter publishes metrics on.
#[inline]
pub fn metric_subject() -> String {
    format!("broker.metric.{}", broker_id_token(BrokerId::Upstox))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ready_with_empty_creds_is_config_error_and_submit_fails_closed() {
        let creds = UpstoxCredentials::new("", "", "");
        let (b, _) = UpstoxBroker::with_recorder(creds).unwrap();
        let r = b.ready().await;
        assert!(matches!(r, ReadyState::ConfigError(_)));

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
        let creds = UpstoxCredentials::new("", "", "tok");
        let (b, recorder) = UpstoxBroker::with_recorder(creds).unwrap();
        let _ = b.ready().await;
        let snap = recorder.snapshot().await;
        assert!(!snap.is_empty(), "expected at least one metric");
        assert!(snap.iter().any(|m| m.broker_id == BrokerId::Upstox));
        assert!(snap.iter().any(|m| matches!(m.op, BrokerOp::Ready)));
        assert!(snap.iter().all(|m| m.error));
    }

    #[test]
    fn upstox_status_to_fsm_classifies_known_values() {
        assert_eq!(
            upstox_status_to_fsm("complete"),
            OrderLifecycleState::Filled
        );
        assert_eq!(
            upstox_status_to_fsm("Complete"),
            OrderLifecycleState::Filled
        );
        assert_eq!(
            upstox_status_to_fsm("cancelled"),
            OrderLifecycleState::Cancelled
        );
        assert_eq!(
            upstox_status_to_fsm("rejected"),
            OrderLifecycleState::Rejected
        );
        assert_eq!(
            upstox_status_to_fsm("partially filled"),
            OrderLifecycleState::PartiallyFilled
        );
        assert_eq!(
            upstox_status_to_fsm("partial"),
            OrderLifecycleState::PartiallyFilled
        );
        assert_eq!(
            upstox_status_to_fsm("open"),
            OrderLifecycleState::Submitted
        );
        assert_eq!(
            upstox_status_to_fsm("validation pending"),
            OrderLifecycleState::Submitted
        );
        // Unknown statuses fall back to Submitted (conservative).
        assert_eq!(
            upstox_status_to_fsm("UNKNOWN_STATE"),
            OrderLifecycleState::Submitted
        );
    }

    #[test]
    fn order_status_response_projects_to_canonical() {
        let r = UpstoxOrderStatusResponse {
            status: Some("success".into()),
            data: UpstoxOrderDetail {
                order_id: "ord-1".into(),
                status: "complete".into(),
                filled_quantity: 7,
                average_price: 123.45,
            },
        };
        let s = order_status_response_to_status(r);
        assert_eq!(s.broker_order_id, "ord-1");
        assert_eq!(s.state, OrderLifecycleState::Filled);
        assert_eq!(s.filled_qty.raw(), 7);
        assert_eq!(s.avg_fill_paise, 12345);
    }

    #[test]
    fn metric_subject_string_is_stable() {
        assert_eq!(metric_subject(), "broker.metric.upstox");
    }
}
