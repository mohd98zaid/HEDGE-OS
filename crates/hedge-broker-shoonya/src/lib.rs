//! `hedge-broker-shoonya` — [`BrokerAdapter`] implementation against
//! the Shoonya / Finvasia NorenAPI
//! (<https://api.shoonya.com/NorenWebApi.html>).
//!
//! ### Layout
//!
//! * [`client`] — async REST wrapper around `reqwest::Client`. Handles
//!   the `jData=<json>&jKey=<session_token>` wire format that the
//!   NorenAPI uses on every endpoint.
//! * [`translator`] — `OrderIntent` → Shoonya `jData` JSON translator.
//! * This module — composes the two into a [`ShoonyaBroker`] satisfying
//!   the [`BrokerAdapter`] trait.
//!
//! ### Auth
//!
//! Shoonya uses a session token (NorenAPI `susertoken`) refreshed
//! daily by an out-of-process helper. Missing or empty credentials
//! cause `ready()` to return [`ReadyState::ConfigError`] and `submit()`
//! to fail closed (R7.5).
//!
//! ### Production protocol gaps
//!
//! The Shoonya **WebSocket binary tick protocol** lives in
//! `hedge-market-data`. Where Shoonya's REST surface has insufficient
//! public documentation we leave a `// TODO: production protocol`
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

pub use crate::client::{
    app_key_digest, ShoonyaClient, ShoonyaCredentials, ShoonyaOrderStatusResponse,
    SHOONYA_API_BASE,
};
pub use crate::translator::{
    default_tradingsymbol_resolver, intent_to_shoonya_jdata, modification_to_shoonya_jdata,
    paise_to_rupee_string, ShoonyaModifyOrderJData, ShoonyaPlaceOrderJData,
};

/// Shoonya / Finvasia adapter.
pub struct ShoonyaBroker {
    client: ShoonyaClient,
    publisher: Arc<dyn MetricPublisher>,
    ready_state: RwLock<ReadyState>,
}

impl ShoonyaBroker {
    /// Construct from credentials and a metric publisher.
    pub fn new(
        creds: ShoonyaCredentials,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        Self::with_base_url(creds, SHOONYA_API_BASE, publisher)
    }

    /// Construct against a custom base URL.
    pub fn with_base_url(
        creds: ShoonyaCredentials,
        base_url: impl Into<String>,
        publisher: Arc<dyn MetricPublisher>,
    ) -> Result<Self, BrokerError> {
        let initial = match creds.validate() {
            Ok(()) => ReadyState::Disconnected("not yet probed".into()),
            Err(reason) => ReadyState::ConfigError(reason.into()),
        };
        let client = ShoonyaClient::with_base(creds, base_url)?;
        Ok(Self {
            client,
            publisher,
            ready_state: RwLock::new(initial),
        })
    }

    /// Convenience: install an in-memory recorder.
    pub fn with_recorder(
        creds: ShoonyaCredentials,
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
            broker_id: BrokerId::Shoonya,
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

/// Map a Shoonya order status string to the canonical FSM state.
pub fn shoonya_status_to_fsm(status: &str) -> OrderLifecycleState {
    match status {
        "COMPLETE" => OrderLifecycleState::Filled,
        "CANCELED" | "CANCELLED" => OrderLifecycleState::Cancelled,
        "REJECTED" => OrderLifecycleState::Rejected,
        "PARTIAL" | "PARTIALLY_TRADED" => OrderLifecycleState::PartiallyFilled,
        "OPEN" | "TRIGGER_PENDING" | "PENDING" => OrderLifecycleState::Submitted,
        _ => OrderLifecycleState::Submitted,
    }
}

/// Project a [`ShoonyaOrderStatusResponse`] to the canonical
/// [`OrderStatus`].
pub fn order_status_response_to_status(
    r: ShoonyaOrderStatusResponse,
    fallback_id: &str,
) -> Result<OrderStatus, BrokerError> {
    let id = r.norenordno.unwrap_or_else(|| fallback_id.to_owned());
    let state = shoonya_status_to_fsm(r.status.as_deref().unwrap_or(""));
    let filled_qty = r
        .fillshares
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let avg_paise = r
        .avgprc
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|f| (f * 100.0).round() as i64)
        .unwrap_or(0);
    Ok(OrderStatus {
        broker_order_id: id,
        state,
        filled_qty: Qty::new(filled_qty),
        avg_fill_paise: avg_paise,
        broker_ts_ns: None,
    })
}

#[async_trait]
impl BrokerAdapter for ShoonyaBroker {
    fn broker_id(&self) -> BrokerId {
        BrokerId::Shoonya
    }

    async fn submit(&self, intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
        let start = now_ns();
        let cid = intent.correlation_id;

        if let Err(e) = self.fail_closed_if_misconfigured() {
            self.record_metric(BrokerOp::Submit, cid, start, &Err(e.clone())).await;
            return Err(e);
        }

        let creds = self.client.credentials();
        let j = intent_to_shoonya_jdata(
            intent,
            &creds.user_id,
            &creds.account_id,
            default_tradingsymbol_resolver,
        );
        let result = self.client.place_order(&j).await;
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

        // Shoonya modify requires the original tsym + exch which the
        // adapter does not retain in-process. Production callers route
        // modify through the lifecycle tracker that holds the original
        // intent. For now we use placeholders; the lifecycle wiring is
        // task 15.1.
        let creds = self.client.credentials();
        let j = modification_to_shoonya_jdata(
            modification,
            &creds.user_id,
            "UNKNOWN-EQ".into(),
            "NSE",
        );
        let result = self.client.modify_order(&j).await;
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
        result.and_then(|r| order_status_response_to_status(r, broker_order_id))
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
        let probe = self.client.ping_user_details().await;
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

/// Subject string the Shoonya adapter publishes metrics on.
#[inline]
pub fn metric_subject() -> String {
    format!("broker.metric.{}", broker_id_token(BrokerId::Shoonya))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ready_with_empty_creds_is_config_error_and_submit_fails_closed() {
        let creds = ShoonyaCredentials::new("", "", "");
        let (b, _) = ShoonyaBroker::with_recorder(creds).unwrap();
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
    fn shoonya_status_to_fsm_classifies_known_values() {
        assert_eq!(shoonya_status_to_fsm("COMPLETE"), OrderLifecycleState::Filled);
        assert_eq!(shoonya_status_to_fsm("CANCELED"), OrderLifecycleState::Cancelled);
        assert_eq!(shoonya_status_to_fsm("CANCELLED"), OrderLifecycleState::Cancelled);
        assert_eq!(shoonya_status_to_fsm("REJECTED"), OrderLifecycleState::Rejected);
        assert_eq!(shoonya_status_to_fsm("PARTIAL"), OrderLifecycleState::PartiallyFilled);
        assert_eq!(shoonya_status_to_fsm("OPEN"), OrderLifecycleState::Submitted);
    }

    #[test]
    fn metric_subject_string_is_stable() {
        assert_eq!(metric_subject(), "broker.metric.shoonya");
    }

    #[test]
    fn order_status_response_to_status_parses_strings() {
        let r = ShoonyaOrderStatusResponse {
            stat: Some("Ok".into()),
            norenordno: Some("NN-1".into()),
            status: Some("COMPLETE".into()),
            fillshares: Some("7".into()),
            avgprc: Some("100.50".into()),
        };
        let s = order_status_response_to_status(r, "fallback").unwrap();
        assert_eq!(s.broker_order_id, "NN-1");
        assert_eq!(s.state, OrderLifecycleState::Filled);
        assert_eq!(s.filled_qty.raw(), 7);
        assert_eq!(s.avg_fill_paise, 10050);
    }
}
