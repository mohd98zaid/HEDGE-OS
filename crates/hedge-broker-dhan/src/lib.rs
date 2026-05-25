//! `hedge-broker-dhan` — [`BrokerAdapter`] implementation against
//! Dhan API v2 (<https://dhanhq.co/docs/v2/>).
//!
//! ### Layout
//!
//! * [`client`] — async REST wrapper around `reqwest::Client`.
//! * [`translator`] — `OrderIntent` → Dhan JSON body translator.
//! * This module — composes the two into a [`DhanBroker`] satisfying
//!   the [`BrokerAdapter`] trait.
//!
//! ### Auth
//!
//! Dhan uses a `client_id` plus an `access-token` HTTP header. Missing
//! or empty credentials cause `ready()` to return
//! [`ReadyState::ConfigError`] and `submit()` to fail closed (R7.5).
//!
//! ### Production protocol gaps
//!
//! Dhan's **WebSocket binary tick protocol** for real-time market data
//! lives in `hedge-market-data`, not here. Where Dhan's REST surface
//! has insufficient public documentation we mark
//! `// TODO: production protocol — replace with vendor-specific binary parser`.

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

pub use crate::client::{DhanClient, DhanCredentials, DhanOrderStatusResponse, DHAN_API_BASE};
pub use crate::translator::{
    default_security_id_resolver, intent_to_dhan_body, modification_to_dhan_body,
    paise_to_rupee_f64, DhanModifyOrderBody, DhanPlaceOrderBody,
};

/// Dhan adapter.
pub struct DhanBroker {
    client: DhanClient,
    publisher: Arc<dyn MetricPublisher>,
    ready_state: RwLock<ReadyState>,
}

impl DhanBroker {
    /// Construct from credentials and a metric publisher.
    pub fn new(
        creds: DhanCredentials,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        Self::with_base_url(creds, DHAN_API_BASE, publisher)
    }

    /// Construct against a custom base URL (used by tests).
    pub fn with_base_url(
        creds: DhanCredentials,
        base_url: impl Into<String>,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        let initial = match creds.validate() {
            Ok(()) => ReadyState::Disconnected("not yet probed".into()),
            Err(reason) => ReadyState::ConfigError(reason.into()),
        };
        let client = DhanClient::with_base(creds, base_url)?;
        Ok(Self {
            client,
            publisher,
            ready_state: RwLock::new(initial),
        })
    }

    /// Convenience: install an in-memory recorder.
    pub fn with_recorder(
        creds: DhanCredentials,
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
            broker_id: BrokerId::Dhan,
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

/// Map a Dhan order status string to the canonical FSM state.
pub fn dhan_status_to_fsm(status: &str) -> OrderLifecycleState {
    match status {
        "TRADED" | "FILLED" => OrderLifecycleState::Filled,
        "CANCELLED" | "EXPIRED" => OrderLifecycleState::Cancelled,
        "REJECTED" => OrderLifecycleState::Rejected,
        "PARTIAL" | "PARTIALLY_TRADED" => OrderLifecycleState::PartiallyFilled,
        "PENDING" | "TRANSIT" | "OPEN" | "MODIFIED" => OrderLifecycleState::Submitted,
        _ => OrderLifecycleState::Submitted,
    }
}

/// Project a [`DhanOrderStatusResponse`] to the canonical [`OrderStatus`].
pub fn order_status_response_to_status(r: DhanOrderStatusResponse) -> OrderStatus {
    let state = dhan_status_to_fsm(&r.order_status);
    let avg_paise = (r.avg_price * 100.0).round() as i64;
    OrderStatus {
        broker_order_id: r.order_id,
        state,
        filled_qty: Qty::new(r.filled_qty),
        avg_fill_paise: avg_paise,
        broker_ts_ns: None,
    }
}

#[async_trait]
impl BrokerAdapter for DhanBroker {
    fn broker_id(&self) -> BrokerId {
        BrokerId::Dhan
    }

    async fn submit(&self, intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
        let start = now_ns();
        let cid = intent.correlation_id;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Submit, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let body = intent_to_dhan_body(
            intent,
            &self.client.credentials().client_id,
            default_security_id_resolver,
        );
        let result = self.client.place_order(&body).await;
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
        let cid = CorrelationId::NIL;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Modify, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let body = modification_to_dhan_body(modification);
        let result = self
            .client
            .modify_order(&modification.broker_order_id, &body)
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

        let result = self.client.cancel_order(broker_order_id).await;
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
        result.map(order_status_response_to_status)
    }

    async fn metrics(&self) -> Vec<BrokerMetric> {
        Vec::new()
    }

    async fn ready(&self) -> ReadyState {
        let start = now_ns();
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
        let probe = self.client.ping_orders().await;
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

/// Subject string the Dhan adapter publishes metrics on.
#[inline]
pub fn metric_subject() -> String {
    format!("broker.metric.{}", broker_id_token(BrokerId::Dhan))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ready_with_empty_creds_is_config_error_and_submit_fails_closed() {
        let creds = DhanCredentials::new("", "");
        let (b, _) = DhanBroker::with_recorder(creds).unwrap();
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

    #[test]
    fn dhan_status_to_fsm_classifies_known_values() {
        assert_eq!(dhan_status_to_fsm("TRADED"), OrderLifecycleState::Filled);
        assert_eq!(dhan_status_to_fsm("CANCELLED"), OrderLifecycleState::Cancelled);
        assert_eq!(dhan_status_to_fsm("REJECTED"), OrderLifecycleState::Rejected);
        assert_eq!(dhan_status_to_fsm("PARTIAL"), OrderLifecycleState::PartiallyFilled);
        assert_eq!(dhan_status_to_fsm("PENDING"), OrderLifecycleState::Submitted);
    }

    #[test]
    fn metric_subject_string_is_stable() {
        assert_eq!(metric_subject(), "broker.metric.dhan");
    }
}
