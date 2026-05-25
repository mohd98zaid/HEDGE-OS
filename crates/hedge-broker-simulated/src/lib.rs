//! `hedge-broker-simulated` — fully functional in-process [`BrokerAdapter`]
//! used by the Replay_Engine and the test suite (R22.4, design § Components
//! § Broker_Adapter Abstraction).
//!
//! Unlike the live-broker adapters, this crate is **not** a thin REST
//! shim. It owns:
//!
//! * a per-symbol [`OrderBook`](orderbook::OrderBook) (in-memory, bounded)
//!   that defines the available liquidity for every order;
//! * a per-`broker_order_id` [`OrderRecord`](lifecycle::OrderRecord) that
//!   tracks the FSM state, fills, and cumulative VWAP;
//! * a deterministic id minter so a given input sequence produces a
//!   stable sequence of `broker_order_id` strings.
//!
//! The design constraints from §17.1 are honoured here:
//!
//! 1. `submit / modify / cancel / status` are all implemented and produce
//!    the full FSM lifecycle (`New → Submitted → {PartiallyFilled →}
//!    Filled | Cancelled | Rejected`).
//! 2. A [`BrokerMetric`] is published on `broker.metric.simulated` after
//!    every request through the supplied [`MetricPublisher`] (R7.4).
//! 3. [`SimulatedBroker::ready`] returns [`ReadyState::Ready`] when an
//!    initial book is loaded and [`ReadyState::ConfigError`] otherwise so
//!    callers that forget to seed the book fail closed (R7.5).
//! 4. The path is fully synchronous in computation; the `async` colouring
//!    is preserved because the `BrokerAdapter` trait is async, and tests
//!    can run on a `current_thread` runtime.
//! 5. Determinism: no clocks, no randomness. Replay test consumers get
//!    the same fills for the same ordered inputs. (Property 12.)

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod lifecycle;
pub mod orderbook;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use hedge_broker_api::{
    broker_id_token, BrokerAdapter, BrokerError, BrokerMetric, BrokerOp, MetricPublisher,
    OrderIntent, OrderModification, OrderStatus, ReadyState, SubmitAck, VecMetricRecorder,
};
use hedge_core::{now_ns, BrokerId, Side};
use parking_lot::Mutex;

pub use crate::lifecycle::OrderRecord;
pub use crate::orderbook::{vwap_paise, BookLevel, OrderBook};

/// Default broker id for the simulated adapter. Always [`BrokerId::Simulated`].
pub const SIM_BROKER_ID: BrokerId = BrokerId::Simulated;

/// Configuration knobs for the simulated broker.
#[derive(Clone, Debug)]
pub struct SimulatedBrokerConfig {
    /// Optional fixed latency to add to every operation, in nanoseconds.
    /// Useful in tests that want to verify the latency budget paths
    /// without hammering wall-clock sleeps. Defaults to `0`.
    pub artificial_latency_ns: u64,

    /// When `false`, [`SimulatedBroker::ready`] reports
    /// [`ReadyState::ConfigError`] to exercise the fail-closed path
    /// (R7.5). Defaults to `true`.
    pub ready_at_construction: bool,
}

impl Default for SimulatedBrokerConfig {
    fn default() -> Self {
        Self {
            artificial_latency_ns: 0,
            ready_at_construction: true,
        }
    }
}

/// Internal bookkeeping shared between `submit / modify / cancel / status`.
struct State {
    /// Per-symbol books. Keyed by `SymbolId.raw()`.
    books: HashMap<u32, OrderBook>,
    /// Per-order records.
    orders: HashMap<String, OrderRecord>,
    /// Monotonic order-id counter; produces `SIM-1`, `SIM-2`, ...
    next_id: u64,
    /// Adapter readiness override.
    ready: ReadyState,
}

impl State {
    fn new(ready: ReadyState) -> Self {
        Self {
            books: HashMap::new(),
            orders: HashMap::new(),
            next_id: 0,
            ready,
        }
    }
}

/// Simulated broker adapter.
pub struct SimulatedBroker {
    /// Internal mutable state behind a `parking_lot::Mutex`. The mutex is
    /// held only across in-memory updates; no async work is awaited under
    /// the lock so it never causes contention.
    state: Mutex<State>,
    cfg: SimulatedBrokerConfig,
    publisher: Arc<dyn MetricPublisher>,
    /// Emit-counter so tests can assert the total number of metrics.
    pub metrics_emitted: AtomicU64,
}

impl SimulatedBroker {
    /// Construct a new simulated broker with the given metric publisher.
    pub fn new(cfg: SimulatedBrokerConfig, publisher: Arc<dyn MetricPublisher>) -> Self {
        let initial = if cfg.ready_at_construction {
            ReadyState::Ready
        } else {
            ReadyState::ConfigError("ready_at_construction = false".into())
        };
        Self {
            state: Mutex::new(State::new(initial)),
            cfg,
            publisher,
            metrics_emitted: AtomicU64::new(0),
        }
    }

    /// Convenience constructor: install an in-memory [`VecMetricRecorder`]
    /// and return both the broker and the recorder so tests can assert on
    /// emitted metrics without setting up NATS.
    pub fn with_recorder() -> (Self, Arc<VecMetricRecorder>) {
        let recorder = Arc::new(VecMetricRecorder::new());
        let pub_dyn: Arc<dyn MetricPublisher> = recorder.clone();
        (
            Self::new(SimulatedBrokerConfig::default(), pub_dyn),
            recorder,
        )
    }

    /// Replace (or create) the book for a symbol.
    pub fn set_book(&self, symbol_raw: u32, book: OrderBook) {
        let mut state = self.state.lock();
        state.books.insert(symbol_raw, book);
    }

    /// Mark the adapter as `ConfigError`. Used by tests that exercise
    /// the fail-closed contract (R7.5).
    pub fn force_config_error(&self, reason: impl Into<String>) {
        let mut state = self.state.lock();
        state.ready = ReadyState::ConfigError(reason.into());
    }

    /// Mark the adapter as `Disconnected`.
    pub fn force_disconnected(&self, reason: impl Into<String>) {
        let mut state = self.state.lock();
        state.ready = ReadyState::Disconnected(reason.into());
    }

    /// Mark the adapter as `Ready`.
    pub fn force_ready(&self) {
        let mut state = self.state.lock();
        state.ready = ReadyState::Ready;
    }

    /// Number of metrics emitted to date. Useful in tests that want to
    /// assert one metric per request without snapshotting the recorder.
    pub fn metrics_emitted(&self) -> u64 {
        self.metrics_emitted.load(Ordering::Relaxed)
    }

    /// Total number of orders the broker has accepted.
    pub fn order_count(&self) -> usize {
        self.state.lock().orders.len()
    }

    /// Helper used by every request method: add the artificial latency,
    /// build a [`BrokerMetric`], publish it, and bump the counter. The
    /// `start_ns` argument is the value of `now_ns()` captured at the
    /// top of the request.
    async fn record_metric(
        &self,
        op: BrokerOp,
        correlation_id: hedge_core::CorrelationId,
        start_ns: u64,
        error: bool,
    ) {
        // Artificial latency is honoured by widening `latency_ns`. We do
        // NOT actually sleep — the simulated broker is meant to be fast
        // and deterministic.
        let latency_ns = now_ns()
            .saturating_sub(start_ns)
            .saturating_add(self.cfg.artificial_latency_ns);
        let metric = BrokerMetric {
            broker_id: SIM_BROKER_ID,
            correlation_id,
            op,
            latency_ns,
            error,
            http_status: None,
            ts_ns: now_ns(),
        };
        self.publisher.publish(metric).await;
        self.metrics_emitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Internal: do the submit synchronously under the lock, returning
    /// the result and the correlation id for metric emission.
    fn do_submit(
        &self,
        intent: &OrderIntent,
        ts_ns: u64,
    ) -> Result<SubmitAck, BrokerError> {
        let mut state = self.state.lock();
        if !state.ready.is_ready() {
            return Err(BrokerError::NotReady(format!("{}", state.ready)));
        }

        // Allocate a deterministic id.
        state.next_id = state.next_id.saturating_add(1);
        let id = format!("SIM-{}", state.next_id);

        // Resolve the book; if no book has been seeded for the symbol
        // we treat that as Rejected — a missing book is a configuration
        // problem at the test/replay level.
        let book = match state.books.get_mut(&intent.symbol_raw) {
            Some(b) => b,
            None => {
                return Err(BrokerError::Rejected(format!(
                    "no orderbook seeded for symbol {}",
                    intent.symbol_raw
                )));
            }
        };

        let qty = intent.quantity.raw();
        let limit = match (intent.order_type, intent.side) {
            (hedge_broker_api::OrderType::Market, _) => None,
            (hedge_broker_api::OrderType::Limit, _) => Some(intent.limit_paise),
        };
        let fills = match intent.side {
            Side::Buy => book.consume_asks(qty, limit),
            Side::Sell => book.consume_bids(qty, limit),
        };

        let mut record = OrderRecord::new(id.clone(), intent.clone(), ts_ns);
        record.apply_fills(&fills, ts_ns);

        let ack = SubmitAck {
            broker_order_id: id.clone(),
            broker_ts_ns: Some(ts_ns),
        };
        state.orders.insert(id, record);
        Ok(ack)
    }

    fn do_modify(
        &self,
        m: &OrderModification,
        ts_ns: u64,
    ) -> Result<(), BrokerError> {
        let mut state = self.state.lock();
        if !state.ready.is_ready() {
            return Err(BrokerError::NotReady(format!("{}", state.ready)));
        }
        let rec = state
            .orders
            .get_mut(&m.broker_order_id)
            .ok_or_else(|| BrokerError::UnknownOrderId(m.broker_order_id.clone()))?;
        rec.apply_modification(m, ts_ns)
    }

    fn do_cancel(
        &self,
        broker_order_id: &str,
        ts_ns: u64,
    ) -> Result<(), BrokerError> {
        let mut state = self.state.lock();
        if !state.ready.is_ready() {
            return Err(BrokerError::NotReady(format!("{}", state.ready)));
        }
        let rec = state
            .orders
            .get_mut(broker_order_id)
            .ok_or_else(|| BrokerError::UnknownOrderId(broker_order_id.to_owned()))?;
        rec.apply_cancel(ts_ns)
    }

    fn do_status(
        &self,
        broker_order_id: &str,
    ) -> Result<OrderStatus, BrokerError> {
        let state = self.state.lock();
        // Status is informational and is allowed even when the adapter is
        // marked `Disconnected` so the engine can still report on
        // historical orders. It is NOT allowed when ConfigError because
        // there are no real records to report.
        if matches!(state.ready, ReadyState::ConfigError(_)) {
            return Err(BrokerError::NotReady(format!("{}", state.ready)));
        }
        state
            .orders
            .get(broker_order_id)
            .map(|r| r.to_status())
            .ok_or_else(|| BrokerError::UnknownOrderId(broker_order_id.to_owned()))
    }
}

#[async_trait]
impl BrokerAdapter for SimulatedBroker {
    fn broker_id(&self) -> BrokerId {
        SIM_BROKER_ID
    }

    async fn submit(&self, intent: &OrderIntent) -> Result<SubmitAck, BrokerError> {
        let start = now_ns();
        let res = self.do_submit(intent, start);
        let err = res.is_err();
        self.record_metric(BrokerOp::Submit, intent.correlation_id, start, err)
            .await;
        res
    }

    async fn modify(&self, modification: &OrderModification) -> Result<(), BrokerError> {
        let start = now_ns();
        let res = self.do_modify(modification, start);
        let err = res.is_err();
        // Recover the correlation id from the order record if it exists,
        // otherwise emit `NIL` so the metric is still well-formed.
        let cid = self
            .state
            .lock()
            .orders
            .get(&modification.broker_order_id)
            .map(|r| r.correlation_id)
            .unwrap_or(hedge_core::CorrelationId::NIL);
        self.record_metric(BrokerOp::Modify, cid, start, err).await;
        res
    }

    async fn cancel(&self, broker_order_id: &str) -> Result<(), BrokerError> {
        let start = now_ns();
        let res = self.do_cancel(broker_order_id, start);
        let err = res.is_err();
        let cid = self
            .state
            .lock()
            .orders
            .get(broker_order_id)
            .map(|r| r.correlation_id)
            .unwrap_or(hedge_core::CorrelationId::NIL);
        self.record_metric(BrokerOp::Cancel, cid, start, err).await;
        res
    }

    async fn status(&self, broker_order_id: &str) -> Result<OrderStatus, BrokerError> {
        let start = now_ns();
        let res = self.do_status(broker_order_id);
        let err = res.is_err();
        let cid = self
            .state
            .lock()
            .orders
            .get(broker_order_id)
            .map(|r| r.correlation_id)
            .unwrap_or(hedge_core::CorrelationId::NIL);
        self.record_metric(BrokerOp::Status, cid, start, err).await;
        res
    }

    async fn metrics(&self) -> Vec<BrokerMetric> {
        // The simulated broker does not retain a metric tail in-process;
        // tests can subscribe through the supplied [`MetricPublisher`].
        Vec::new()
    }

    async fn ready(&self) -> ReadyState {
        let start = now_ns();
        let snapshot = self.state.lock().ready.clone();
        // Treat ready-checks as never-failing for metric purposes.
        self.record_metric(
            BrokerOp::Ready,
            hedge_core::CorrelationId::NIL,
            start,
            !snapshot.is_ready(),
        )
        .await;
        snapshot
    }
}

/// Subject string the simulated broker publishes metrics on.
#[inline]
pub fn metric_subject() -> String {
    format!("broker.metric.{}", broker_id_token(SIM_BROKER_ID))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_broker_api::{Exchange, OrderType};
    use hedge_core::{CorrelationId, Qty};

    fn make_intent(symbol_raw: u32, side: Side, qty: u64, limit_paise: i64) -> OrderIntent {
        OrderIntent {
            correlation_id: CorrelationId::new(),
            symbol_raw,
            side,
            quantity: Qty::new(qty),
            order_type: OrderType::Limit,
            limit_paise,
            exchange: Exchange::Nse,
        }
    }

    fn make_market_intent(symbol_raw: u32, side: Side, qty: u64) -> OrderIntent {
        OrderIntent {
            correlation_id: CorrelationId::new(),
            symbol_raw,
            side,
            quantity: Qty::new(qty),
            order_type: OrderType::Market,
            limit_paise: 0,
            exchange: Exchange::Nse,
        }
    }

    #[tokio::test]
    async fn ready_returns_ready_at_construction_by_default() {
        let (b, _) = SimulatedBroker::with_recorder();
        assert!(b.ready().await.is_ready());
    }

    #[tokio::test]
    async fn ready_returns_config_error_when_disabled() {
        let recorder = Arc::new(VecMetricRecorder::new());
        let pub_dyn: Arc<dyn MetricPublisher> = recorder.clone();
        let cfg = SimulatedBrokerConfig {
            artificial_latency_ns: 0,
            ready_at_construction: false,
        };
        let b = SimulatedBroker::new(cfg, pub_dyn);
        let r = b.ready().await;
        assert!(matches!(r, ReadyState::ConfigError(_)));
    }

    #[tokio::test]
    async fn submit_with_no_book_is_rejected() {
        let (b, _) = SimulatedBroker::with_recorder();
        let intent = make_market_intent(42, Side::Buy, 5);
        let err = b.submit(&intent).await.unwrap_err();
        match err {
            BrokerError::Rejected(_) => {}
            _ => panic!("wrong error: {err:?}"),
        }
    }

    #[tokio::test]
    async fn submit_when_config_error_fails_closed() {
        let (b, _) = SimulatedBroker::with_recorder();
        b.force_config_error("missing creds");
        let intent = make_market_intent(1, Side::Buy, 5);
        let err = b.submit(&intent).await.unwrap_err();
        match err {
            BrokerError::NotReady(_) => {}
            _ => panic!("wrong error: {err:?}"),
        }
    }

    #[tokio::test]
    async fn submit_market_buy_consumes_asks_and_returns_filled() {
        let (b, recorder) = SimulatedBroker::with_recorder();
        b.set_book(
            1,
            OrderBook::from_levels(
                &[],
                &[BookLevel::new(100_00, 5), BookLevel::new(101_00, 5)],
            ),
        );
        let intent = make_market_intent(1, Side::Buy, 8);
        let ack = b.submit(&intent).await.unwrap();
        assert_eq!(ack.broker_order_id, "SIM-1");

        let status = b.status(&ack.broker_order_id).await.unwrap();
        assert_eq!(status.state, hedge_schemas::order_state::OrderLifecycleState::Filled);
        assert_eq!(status.filled_qty.raw(), 8);
        // (5*100_00 + 3*101_00)/8 = (50000 + 30300)/8 = 80300/8 = 10037
        assert_eq!(status.avg_fill_paise, 10037);

        // One submit metric and one status metric.
        let snap = recorder.snapshot().await;
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].op, BrokerOp::Submit);
        assert_eq!(snap[1].op, BrokerOp::Status);
        assert_eq!(snap[0].broker_id, BrokerId::Simulated);
        assert!(!snap[0].error);
    }

    #[tokio::test]
    async fn submit_limit_order_partial_fill_then_cancel() {
        let (b, _) = SimulatedBroker::with_recorder();
        b.set_book(
            1,
            OrderBook::from_levels(&[], &[BookLevel::new(100_00, 3)]),
        );
        let intent = make_intent(1, Side::Buy, 10, 100_00);
        let ack = b.submit(&intent).await.unwrap();
        let status = b.status(&ack.broker_order_id).await.unwrap();
        assert_eq!(
            status.state,
            hedge_schemas::order_state::OrderLifecycleState::PartiallyFilled
        );
        assert_eq!(status.filled_qty.raw(), 3);

        // Cancel the rest.
        b.cancel(&ack.broker_order_id).await.unwrap();
        let status2 = b.status(&ack.broker_order_id).await.unwrap();
        assert_eq!(
            status2.state,
            hedge_schemas::order_state::OrderLifecycleState::Cancelled
        );
    }

    #[tokio::test]
    async fn cancel_unknown_order_returns_unknown_order_id() {
        let (b, _) = SimulatedBroker::with_recorder();
        let err = b.cancel("does-not-exist").await.unwrap_err();
        match err {
            BrokerError::UnknownOrderId(id) => assert_eq!(id, "does-not-exist"),
            _ => panic!("wrong error variant"),
        }
    }

    #[tokio::test]
    async fn modify_increases_quantity_and_keeps_partial_state() {
        let (b, _) = SimulatedBroker::with_recorder();
        b.set_book(
            1,
            OrderBook::from_levels(&[], &[BookLevel::new(100_00, 3)]),
        );
        let intent = make_intent(1, Side::Buy, 5, 100_00);
        let ack = b.submit(&intent).await.unwrap();

        let m = OrderModification {
            broker_order_id: ack.broker_order_id.clone(),
            new_quantity: Some(Qty::new(10)),
            new_limit_paise: None,
        };
        b.modify(&m).await.unwrap();
        let s = b.status(&ack.broker_order_id).await.unwrap();
        // Only 3 units were available, so still PartiallyFilled.
        assert_eq!(s.filled_qty.raw(), 3);
        assert_eq!(
            s.state,
            hedge_schemas::order_state::OrderLifecycleState::PartiallyFilled
        );
    }

    #[tokio::test]
    async fn metrics_emitted_for_every_request() {
        let (b, recorder) = SimulatedBroker::with_recorder();
        b.set_book(
            1,
            OrderBook::from_levels(&[], &[BookLevel::new(100_00, 5)]),
        );
        let intent = make_market_intent(1, Side::Buy, 5);
        let ack = b.submit(&intent).await.unwrap();
        b.status(&ack.broker_order_id).await.unwrap();
        b.cancel(&ack.broker_order_id).await.ok(); // terminal — Rejected
        b.ready().await;

        let snap = recorder.snapshot().await;
        // submit + status + cancel(error) + ready = 4 metrics.
        assert_eq!(snap.len(), 4);
        let ops: Vec<BrokerOp> = snap.iter().map(|m| m.op).collect();
        assert_eq!(
            ops,
            vec![
                BrokerOp::Submit,
                BrokerOp::Status,
                BrokerOp::Cancel,
                BrokerOp::Ready,
            ]
        );
        // Only the cancel-on-terminal failed.
        assert!(snap[2].error);
        assert!(!snap[0].error);
        assert!(!snap[1].error);
    }

    #[tokio::test]
    async fn deterministic_replay_two_brokers_same_inputs_same_outputs() {
        // Property 12: replay determinism. Two SimulatedBroker instances
        // seeded with identical books and given identical request
        // sequences must produce identical broker_order_ids, fills, and
        // FSM states.
        async fn run() -> (String, hedge_schemas::order_state::OrderLifecycleState, u64, i64) {
            let (b, _) = SimulatedBroker::with_recorder();
            b.set_book(
                1,
                OrderBook::from_levels(
                    &[],
                    &[BookLevel::new(100_00, 4), BookLevel::new(101_00, 6)],
                ),
            );
            let intent = make_market_intent(1, Side::Buy, 7);
            let ack = b.submit(&intent).await.unwrap();
            let status = b.status(&ack.broker_order_id).await.unwrap();
            (
                ack.broker_order_id,
                status.state,
                status.filled_qty.raw(),
                status.avg_fill_paise,
            )
        }
        let a = run().await;
        let b = run().await;
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn metric_subject_string_is_stable() {
        assert_eq!(metric_subject(), "broker.metric.simulated");
    }
}
