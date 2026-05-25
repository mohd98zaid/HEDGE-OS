//! `hedge-broker-angelone` — [`BrokerAdapter`] implementation against
//! Angel One **SmartAPI** (<https://smartapi.angelbroking.com/docs>).
//!
//! ### Layout
//!
//! * [`client`] — async REST wrapper around `reqwest::Client` with
//!   SmartAPI's specific header set (`Authorization: Bearer <jwt>`,
//!   `X-PrivateKey`, `X-UserType`, `X-SourceID`, `X-Client*IP`,
//!   `X-MACAddress`).
//! * [`translator`] — `OrderIntent` → SmartAPI JSON body translator.
//! * This module — composes the two into an [`AngelOneBroker`]
//!   satisfying the [`BrokerAdapter`] trait.
//!
//! ### Auth
//!
//! SmartAPI uses an `api_key` plus a `jwtToken` (minted by the login
//! flow) plus the `client_code`. Missing or empty credentials cause
//! `ready()` to return [`ReadyState::ConfigError`] and `submit()` to
//! fail closed (R7.5).
//!
//! ### Production protocol gaps
//!
//! The SmartAPI **WebSocket binary tick protocol** lives in
//! `hedge-market-data`, not here. The order-details GET endpoint has
//! sparse public docs; production callers should consider switching to
//! `getOrderBook` + local filtering — see the
//! `// TODO: production protocol` markers in `client.rs`.

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
    SmartApiClient, SmartApiCredentials, SmartApiEnvelope, SmartApiOrderStatus,
    SmartApiPlaceOrderData, SMARTAPI_BASE,
};
pub use crate::translator::{
    default_symbol_token_resolver, default_tradingsymbol_resolver, intent_to_smartapi_body,
    modification_to_smartapi_body, paise_to_rupee_string, SmartApiCancelOrderBody,
    SmartApiModifyOrderBody, SmartApiPlaceOrderBody,
};

/// AngelOne SmartAPI adapter.
pub struct AngelOneBroker {
    client: SmartApiClient,
    publisher: Arc<dyn MetricPublisher>,
    ready_state: RwLock<ReadyState>,
}

impl AngelOneBroker {
    /// Construct from credentials and a metric publisher.
    pub fn new(
        creds: SmartApiCredentials,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        Self::with_base_url(creds, SMARTAPI_BASE, publisher)
    }

    /// Construct against a custom base URL.
    pub fn with_base_url(
        creds: SmartApiCredentials,
        base_url: impl Into<String>,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        let initial = match creds.validate() {
            Ok(()) => ReadyState::Disconnected("not yet probed".into()),
            Err(reason) => ReadyState::ConfigError(reason.into()),
        };
        let client = SmartApiClient::with_base(creds, base_url)?;
        Ok(Self {
            client,
            publisher,
            ready_state: RwLock::new(initial),
        })
    }

    /// Convenience: install an in-memory recorder.
    pub fn with_recorder(
        creds: SmartApiCredentials,
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
            broker_id: BrokerId::AngelOne,
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

/// Map a SmartAPI status string to the canonical FSM state.
pub fn smartapi_status_to_fsm(status: &str) -> OrderLifecycleState {
    let lc = status.to_ascii_lowercase();
    match lc.as_str() {
        "complete" => OrderLifecycleState::Filled,
        "cancelled" | "canceled" => OrderLifecycleState::Cancelled,
        "rejected" => OrderLifecycleState::Rejected,
        "partially filled" | "partial" => OrderLifecycleState::PartiallyFilled,
        "open" | "pending" | "trigger pending" => OrderLifecycleState::Submitted,
        _ => OrderLifecycleState::Submitted,
    }
}

/// Project a [`SmartApiOrderStatus`] to the canonical [`OrderStatus`].
pub fn order_status_response_to_status(r: SmartApiOrderStatus) -> OrderStatus {
    let state = smartapi_status_to_fsm(&r.status);
    let avg_paise = (r.avg_price * 100.0).round() as i64;
    OrderStatus {
        broker_order_id: r.order_id,
        state,
        filled_qty: Qty::new(r.filled_shares),
        avg_fill_paise: avg_paise,
        broker_ts_ns: None,
    }
}

#[async_trait]
impl BrokerAdapter for AngelOneBroker {
    fn broker_id(&self) -> BrokerId {
        BrokerId::AngelOne
    }

    async fn submit(&self, intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
        let start = now_ns();
        let cid = intent.correlation_id;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Submit, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let body = intent_to_smartapi_body(
            intent,
            default_symbol_token_resolver,
            default_tradingsymbol_resolver,
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

        let body = modification_to_smartapi_body(modification);
        let result = self.client.modify_order(&body).await;
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

        let result = self.client.cancel_order(broker_order_id, "NORMAL").await;
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
        let probe = self.client.ping_profile().await;
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

/// Subject string the AngelOne adapter publishes metrics on.
#[inline]
pub fn metric_subject() -> String {
    format!("broker.metric.{}", broker_id_token(BrokerId::AngelOne))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ready_with_empty_creds_is_config_error_and_submit_fails_closed() {
        let creds = SmartApiCredentials::new("", "", "");
        let (b, _) = AngelOneBroker::with_recorder(creds).unwrap();
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
    fn smartapi_status_to_fsm_classifies_known_values() {
        assert_eq!(smartapi_status_to_fsm("complete"), OrderLifecycleState::Filled);
        assert_eq!(
            smartapi_status_to_fsm("CANCELLED"),
            OrderLifecycleState::Cancelled
        );
        assert_eq!(
            smartapi_status_to_fsm("rejected"),
            OrderLifecycleState::Rejected
        );
        assert_eq!(
            smartapi_status_to_fsm("partially filled"),
            OrderLifecycleState::PartiallyFilled
        );
        assert_eq!(smartapi_status_to_fsm("open"), OrderLifecycleState::Submitted);
        assert_eq!(
            smartapi_status_to_fsm("trigger pending"),
            OrderLifecycleState::Submitted
        );
    }

    #[test]
    fn metric_subject_string_is_stable() {
        assert_eq!(metric_subject(), "broker.metric.angel_one");
    }
}
