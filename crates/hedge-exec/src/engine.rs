//! `ExecutionEngine` — the in-process orchestrator that consumes
//! approvals, submits to the active broker, drives the per-order
//! FSM, and produces fills (R6).
//!
//! ## Authority hierarchy enforcement (R6.8, R21.1)
//!
//! [`ExecutionEngine::submit`] is the **only** public entry point that
//! produces a broker-side order. The signature requires both an
//! [`ApprovalToken`] minted by the Risk_Engine and the canonical
//! `hedge_schemas::OrderIntent_v1` bytes the token was minted over.
//! HMAC verification happens before any broker contact; without a
//! valid token, no submission can happen.
//!
//! ## Replay mode (R22.4)
//!
//! When the engine is constructed with `ReplayMode::On`, the binary
//! binds both router slots to a `SimulatedBroker`; live brokers are
//! never contacted.
//!
//! ## Translation between intent representations
//!
//! The Risk_Engine signs HMAC over `hedge_schemas::OrderIntent` (the
//! canonical FlatBuffers projection). The `BrokerAdapter::submit`
//! takes `hedge_broker_api::OrderIntent` (a typed broker-agnostic
//! projection). [`translate_intent`] converts between the two without
//! losing fidelity — the FlatBuffers layout's enum bytes map 1:1 to
//! the broker-api enums.

use std::collections::HashSet;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use smallvec::SmallVec;
use tracing::instrument;

use hedge_broker_api::{
    BrokerAdapter, Exchange, OrderIntent as BrokerOrderIntent, OrderType,
};
use hedge_core::{now_ns, BrokerId, CorrelationId, Qty, Side};
use hedge_obs::LatencyEmitter;
use hedge_risk::{ApprovalToken, ApprovalVerifier};
use hedge_schemas::order_state::OrderLifecycleState;
use hedge_schemas::stage::Stage;
use hedge_schemas::{LatencyRecord, OrderIntent};

use crate::error::ExecError;
use crate::lifecycle::{LifecycleEvent, OrderLifecycleTracker};
use crate::retry::{retry_with_backoff, JitterSource, RetryPolicy, SeededJitter, Sleeper};
use crate::router::{BrokerRouter, FailoverEvent, Outcome};

/// Execution-routing budget in nanoseconds (R28.4).
pub const EXEC_ROUTE_BUDGET_NS: u64 = 5_000_000;

/// Replay flag toggled by the supervisor / config (R22.4).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ReplayMode {
    /// Live trading. Router routes through live broker adapters.
    Off,
    /// Replay or test. Router routes through `SimulatedBroker` only.
    On,
}

/// One published-event the engine asks the network layer to fan out.
/// The orchestrator binary translates each variant into the matching
/// NATS / Redis publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineEvent {
    /// `exec.order.<state>` — an FSM transition occurred.
    Lifecycle(LifecycleEvent),
    /// `exec.broker.failover` — the router swapped active broker.
    Failover(FailoverEvent),
    /// `obs.error.exec.<tag>` — a structured error event.
    Error {
        /// Stable short tag (`invalid_token`, `broker_rejected`, …).
        tag: &'static str,
        /// Affected correlation id, if known.
        correlation_id_hex: String,
        /// Stringified error.
        message: String,
    },
    /// `hedge.hot.fills` — a fill payload to append to the Redis Stream.
    Fill {
        /// Correlation id of the parent order.
        correlation_id: CorrelationId,
        /// Symbol the fill is for.
        symbol: u32,
        /// Side (encoded `Side::as_u8`).
        side: u8,
        /// **Per-fill** delta quantity (not cumulative — see
        /// [`ExecutionEngine::on_fill`] for the derivation).
        filled_qty: u64,
        /// **Per-fill** average price in paise.
        avg_fill_paise: i64,
        /// Wall-clock timestamp in nanoseconds.
        ts_ns: u64,
    },
}

/// Orchestrator owning the router, FSM tracker registry, and replay
/// mode flag.
///
/// Cloning the engine is cheap — internals are `Arc`-wrapped so
/// background tasks share a single state machine.
#[derive(Clone)]
pub struct ExecutionEngine {
    inner: Arc<ExecutionEngineInner>,
}

struct ExecutionEngineInner {
    router: Arc<BrokerRouter>,
    verifier: ApprovalVerifier,
    /// Trackers keyed by correlation id (low 128 bits as u128).
    trackers: DashMap<u128, Mutex<OrderLifecycleTracker>>,
    /// Set of consumed approval token byte-strings. A token is
    /// consumed at most once (R5.14). Restart resets the set.
    consumed_tokens: Mutex<HashSet<[u8; 32]>>,
    retry_policy: RetryPolicy,
    replay_mode: ReplayMode,
}

impl ExecutionEngine {
    /// Construct a new engine.
    pub fn new(
        router: Arc<BrokerRouter>,
        verifier: ApprovalVerifier,
        retry_policy: RetryPolicy,
        replay_mode: ReplayMode,
    ) -> Self {
        Self {
            inner: Arc::new(ExecutionEngineInner {
                router,
                verifier,
                trackers: DashMap::new(),
                consumed_tokens: Mutex::new(HashSet::new()),
                retry_policy,
                replay_mode,
            }),
        }
    }

    /// Borrow the configured retry policy.
    #[inline]
    pub fn retry_policy(&self) -> RetryPolicy {
        self.inner.retry_policy
    }

    /// Borrow the configured replay mode.
    #[inline]
    pub fn replay_mode(&self) -> ReplayMode {
        self.inner.replay_mode
    }

    /// Borrow the router for tests / supervisor wiring.
    #[inline]
    pub fn router(&self) -> &Arc<BrokerRouter> {
        &self.inner.router
    }

    /// **The only public entry point that submits an order to a broker**
    /// (R6.8, R21.1).
    ///
    /// The signature requires both the token *and* the intent so
    /// submission without a valid approval is unrepresentable: the
    /// verifier cross-checks the canonical bytes before any broker
    /// contact.
    ///
    /// `sized_quantity`, `ts_ns`, `sequence` are the engine-controlled
    /// extension fields the Risk_Engine signed alongside the intent.
    /// They MUST match exactly for verification to succeed.
    #[instrument(
        level = "debug",
        skip(self, token, intent, emitter, jitter, sleeper),
        fields(
            exec.intent.symbol = intent.symbol,
            exec.intent.qty = intent.quantity,
            exec.intent.side = intent.side,
            exec.replay = ?self.inner.replay_mode,
        )
    )]
    pub async fn submit<E, J, S>(
        &self,
        token: &ApprovalToken,
        intent: &OrderIntent,
        sized_quantity: u64,
        ts_ns: u64,
        sequence: u64,
        emitter: &E,
        jitter: &mut J,
        sleeper: &S,
    ) -> Result<SmallVec<[EngineEvent; 4]>, ExecError>
    where
        E: LatencyEmitter,
        J: JitterSource,
        S: Sleeper,
    {
        let cid = CorrelationId(u128::from_be_bytes(intent.correlation_id));
        // Latency timing for the routing stage (R28.4). We can't hold
        // a `LatencyTracer` across `.await` because the tracer is
        // `!Send`; instead we record the start time and emit on every
        // exit path explicitly.
        let stage_start_ns = now_ns();
        let emit_latency = |emitter: &E, breach_hint: bool| {
            let elapsed = now_ns().saturating_sub(stage_start_ns);
            let mut cid_bytes = [0u8; 16];
            cid_bytes.copy_from_slice(&cid.as_u128().to_be_bytes());
            let actual_breach = breach_hint || elapsed > EXEC_ROUTE_BUDGET_NS;
            let record = LatencyRecord {
                correlation_id: cid_bytes,
                stage: Stage::ExecutionRouting.as_u8(),
                nanos: elapsed,
                budget_nanos: EXEC_ROUTE_BUDGET_NS,
                breach: actual_breach,
            };
            emitter.emit_record(Stage::ExecutionRouting, &record);
            if actual_breach {
                emitter.emit_breach(Stage::ExecutionRouting, &record);
            }
        };

        // ---- Step 1: verify HMAC. ----
        if !self
            .inner
            .verifier
            .verify(token, intent, sized_quantity, ts_ns, sequence)
        {
            // R6.8: reject on mismatch. The caller publishes the
            // resulting `ExecError::InvalidApprovalToken` on
            // `obs.error.exec.invalid_token` (see `ExecError::tag()`).
            emit_latency(emitter, false);
            return Err(ExecError::InvalidApprovalToken {
                correlation_id_hex: cid_to_hex(cid),
            });
        }

        // ---- Step 2: enforce single-use. ----
        {
            let mut consumed = self.inner.consumed_tokens.lock();
            if !consumed.insert(*token.as_bytes()) {
                emit_latency(emitter, false);
                return Err(ExecError::DuplicateToken {
                    correlation_id_hex: cid_to_hex(cid),
                });
            }
        }

        // ---- Step 3: pick adapter. ----
        // Adaptive routing today picks the active slot. Once
        // `RiskApproval.execution_params` is populated the broker hint
        // would feed `pick_for_intent`.
        let adapter: Arc<dyn BrokerAdapter> = self.inner.router.pick_for_intent(None);
        let broker_id = adapter.broker_id();

        // ---- Step 4: install a tracker. ----
        let tracker = OrderLifecycleTracker::new(cid, broker_id, sized_quantity);
        self.inner
            .trackers
            .entry(cid.as_u128())
            .or_insert_with(|| Mutex::new(tracker));

        let mut events: SmallVec<[EngineEvent; 4]> = SmallVec::new();

        // ---- Step 5: translate intent to the broker-api projection. ----
        let broker_intent = translate_intent(intent, sized_quantity).map_err(|reason| {
            ExecError::Internal(format!("intent translation failed: {}", reason))
        })?;

        // ---- Step 6: drive submit through retry-with-backoff. ----
        let policy = self.inner.retry_policy;
        let adapter_for_loop = Arc::clone(&adapter);
        let intent_for_loop = broker_intent;
        let submit_result = retry_with_backoff(policy, jitter, sleeper, |attempt| {
            let adapter = Arc::clone(&adapter_for_loop);
            let intent = intent_for_loop.clone();
            async move {
                match adapter.submit(&intent).await {
                    Ok(ack) => Ok(ack),
                    Err(err) => Err(ExecError::from_broker(broker_id, attempt, err)),
                }
            }
        })
        .await;

        // ---- Step 7: fold submit outcome into the FSM and the
        // router's sliding window.
        let submit_now = now_ns();
        match submit_result {
            Ok(ack) => {
                self.handle_submit_ok(cid, ack, submit_now, &mut events)?;
                emit_latency(emitter, false);
            }
            Err(err) => {
                self.handle_submit_err(cid, &err, submit_now, &mut events);
                emit_latency(emitter, true);
                return Err(err);
            }
        }

        Ok(events)
    }

    /// Process a fill update streamed from the active broker. Drives
    /// the FSM and emits the appropriate events.
    ///
    /// `cum_filled_qty` and `cum_avg_fill_paise` are the **cumulative**
    /// values reported by the broker (matching `OrderState_v1`). The
    /// emitted [`EngineEvent::Fill`] payload carries the **per-fill
    /// delta** because the Position_Engine's `apply_fill` consumes
    /// per-fill quantities, not cumulative.
    pub fn on_fill(
        &self,
        correlation_id: CorrelationId,
        symbol: u32,
        side: u8,
        cum_filled_qty: u64,
        cum_avg_fill_paise: i64,
        ts_ns: u64,
    ) -> Result<SmallVec<[EngineEvent; 4]>, ExecError> {
        let mut events: SmallVec<[EngineEvent; 4]> = SmallVec::new();
        let entry = self
            .inner
            .trackers
            .get(&correlation_id.as_u128())
            .ok_or_else(|| {
                ExecError::Internal(format!(
                    "no tracker for correlation_id {}",
                    cid_to_hex(correlation_id)
                ))
            })?;
        // Snapshot the prior cumulative values before mutating the FSM,
        // then compute the per-fill delta.
        let (prior_cum_qty, prior_cum_avg_paise) = {
            let t = entry.lock();
            (t.filled_qty(), t.avg_fill_paise())
        };
        let lifecycle = {
            let mut t = entry.lock();
            t.partial_fill(cum_filled_qty, cum_avg_fill_paise, ts_ns)?
        };
        events.push(EngineEvent::Lifecycle(lifecycle));

        // Compute per-fill delta. The FSM rejected regressions in
        // partial_fill so cum >= prior is guaranteed.
        let delta_qty = cum_filled_qty.saturating_sub(prior_cum_qty);
        if delta_qty == 0 {
            // Idempotent re-delivery — same cumulative numbers. Don't
            // emit a synthetic zero-size Fill.
            return Ok(events);
        }
        let cum_notional = (cum_avg_fill_paise as i128) * (cum_filled_qty as i128);
        let prior_notional = (prior_cum_avg_paise as i128) * (prior_cum_qty as i128);
        let delta_notional = cum_notional - prior_notional;
        let fill_px_paise = (delta_notional / delta_qty as i128) as i64;

        events.push(EngineEvent::Fill {
            correlation_id,
            symbol,
            side,
            filled_qty: delta_qty,
            avg_fill_paise: fill_px_paise,
            ts_ns,
        });
        Ok(events)
    }

    /// Cancel an in-flight order. Used by the trader-intent handler.
    pub async fn cancel(
        &self,
        correlation_id: CorrelationId,
    ) -> Result<EngineEvent, ExecError> {
        // Look up the broker_order_id from the tracker.
        let (broker, broker_order_id) = {
            let entry = self
                .inner
                .trackers
                .get(&correlation_id.as_u128())
                .ok_or_else(|| {
                    ExecError::Internal(format!(
                        "no tracker for correlation_id {}",
                        cid_to_hex(correlation_id)
                    ))
                })?;
            let t = entry.lock();
            (
                t.broker(),
                t.broker_order_id()
                    .map(str::to_owned)
                    .unwrap_or_default(),
            )
        };

        let adapter = self.inner.router.active_adapter();
        adapter
            .cancel(&broker_order_id)
            .await
            .map_err(|e| ExecError::from_broker(broker, 1, e))?;
        let entry = self
            .inner
            .trackers
            .get(&correlation_id.as_u128())
            .expect("tracker existed seconds ago");
        let event = entry.lock().cancel(now_ns())?;
        Ok(EngineEvent::Lifecycle(event))
    }

    /// Snapshot the FSM state for `correlation_id`, if any.
    pub fn state_of(
        &self,
        correlation_id: CorrelationId,
    ) -> Option<OrderLifecycleState> {
        self.inner
            .trackers
            .get(&correlation_id.as_u128())
            .map(|entry| entry.lock().state())
    }

    /// Number of trackers currently registered.
    pub fn tracked_orders(&self) -> usize {
        self.inner.trackers.len()
    }

    fn handle_submit_ok(
        &self,
        cid: CorrelationId,
        ack: hedge_broker_api::SubmitAck,
        ts_ns: u64,
        events: &mut SmallVec<[EngineEvent; 4]>,
    ) -> Result<(), ExecError> {
        // 1. Update FSM: New -> Submitted.
        let entry = self
            .inner
            .trackers
            .get(&cid.as_u128())
            .ok_or_else(|| ExecError::Internal("tracker disappeared".into()))?;
        let lifecycle = {
            let mut t = entry.lock();
            t.submit(Some(ack.broker_order_id.clone()), ts_ns)?
        };
        events.push(EngineEvent::Lifecycle(lifecycle));

        // 2. Record success on the router. Broker acks don't carry a
        //    latency value in the broker-api SubmitAck; we approximate
        //    the latency from the engine-side stage timer (which is in
        //    submit_inner). For now we record 0 ms — the real latency
        //    is recorded against `hedge_broker_latency_ns{broker}` by
        //    the adapter via `BrokerMetric`.
        if let Some(failover) = self
            .inner
            .router
            .record_outcome(Outcome::Success, 0, ts_ns)
        {
            events.push(EngineEvent::Failover(failover));
        }
        Ok(())
    }

    fn handle_submit_err(
        &self,
        cid: CorrelationId,
        err: &ExecError,
        ts_ns: u64,
        events: &mut SmallVec<[EngineEvent; 4]>,
    ) {
        // Emit a structured error event for the orchestrator to publish on
        // `obs.error.exec.<tag>`.
        events.push(EngineEvent::Error {
            tag: err.tag(),
            correlation_id_hex: cid_to_hex(cid),
            message: err.to_string(),
        });

        // Drive the FSM into Rejected. The FSM only allows
        // Submitted -> Rejected, so push through Submitted first if
        // the order never made it past New.
        if let Some(entry) = self.inner.trackers.get(&cid.as_u128()) {
            let mut t = entry.lock();
            if t.state() == OrderLifecycleState::New {
                if let Ok(submitted) = t.submit(None, ts_ns) {
                    events.push(EngineEvent::Lifecycle(submitted));
                }
            }
            if let Ok(rejected) = t.reject(ts_ns) {
                events.push(EngineEvent::Lifecycle(rejected));
            }
        }

        // Record the failure on the router only if the variant counts
        // toward failover (R6.5: NotReady / Auth do NOT count).
        if err.counts_toward_failover() {
            if let Some(failover) = self.inner.router.record_outcome(
                Outcome::Failure,
                u32::MAX,
                ts_ns,
            ) {
                events.push(EngineEvent::Failover(failover));
            }
        }
    }
}

/// Translate the canonical `hedge_schemas::OrderIntent` into the
/// broker-agnostic `hedge_broker_api::OrderIntent` accepted by every
/// adapter. Returns `Err(reason)` if a wire enum byte does not
/// correspond to a known variant.
fn translate_intent(
    intent: &OrderIntent,
    sized_quantity: u64,
) -> Result<BrokerOrderIntent, &'static str> {
    let side = match intent.side {
        0 => Side::Buy,
        1 => Side::Sell,
        _ => return Err("unknown side discriminant"),
    };
    let order_type = OrderType::from_u8(intent.order_type)
        .ok_or("unknown order_type discriminant")?;
    let exchange = Exchange::from_i8(intent.exchange)
        .ok_or("unknown exchange discriminant")?;
    Ok(BrokerOrderIntent {
        correlation_id: CorrelationId(u128::from_be_bytes(intent.correlation_id)),
        symbol_raw: intent.symbol,
        side,
        quantity: Qty::new(sized_quantity),
        order_type,
        limit_paise: intent.limit_paise,
        exchange,
    })
}

/// Convert a `CorrelationId` into its 32-char lowercase hex form.
fn cid_to_hex(cid: CorrelationId) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(32);
    for byte in cid.as_u128().to_be_bytes() {
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

/// Default jitter source used by the engine's submit loop. Seeded
/// from the process id so different replicas decorrelate naturally.
pub fn default_jitter_source() -> SeededJitter {
    SeededJitter::new((std::process::id() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::{NoJitter, RecordingSleeper};
    use crate::router::FailoverThresholds;
    use async_trait::async_trait;
    use hedge_broker_api::{
        BrokerError, BrokerMetric, OrderModification, OrderStatus, ReadyState, SubmitAck,
    };
    use hedge_risk::ApprovalSigner;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Test adapter that responds according to a programmed script.
    struct ScriptedAdapter {
        id: BrokerId,
        /// Counter; the adapter returns a transient error while the
        /// counter is positive, then succeeds.
        fail_until: AtomicU32,
    }

    #[async_trait]
    impl BrokerAdapter for ScriptedAdapter {
        fn broker_id(&self) -> BrokerId {
            self.id
        }
        async fn submit(
            &self,
            _intent: &BrokerOrderIntent,
        ) -> Result<SubmitAck, BrokerError> {
            let prev = self.fail_until.fetch_sub(1, Ordering::Relaxed);
            if prev > 0 {
                return Err(BrokerError::Transient("scripted-transient".into()));
            }
            Ok(SubmitAck {
                broker_order_id: format!("ORD-{:?}", self.id),
                broker_ts_ns: Some(1_000),
            })
        }
        async fn modify(&self, _m: &OrderModification) -> Result<(), BrokerError> {
            Ok(())
        }
        async fn cancel(&self, _id: &str) -> Result<(), BrokerError> {
            Ok(())
        }
        async fn status(&self, _id: &str) -> Result<OrderStatus, BrokerError> {
            unreachable!()
        }
        async fn metrics(&self) -> Vec<BrokerMetric> {
            Vec::new()
        }
        async fn ready(&self) -> ReadyState {
            ReadyState::Ready
        }
    }

    fn make_engine(
        primary_fail_until: u32,
        backup_fail_until: u32,
    ) -> (ExecutionEngine, ApprovalSigner) {
        let signer = ApprovalSigner::from_key(b"test-hmac-key-32-bytes-padding!!".to_vec());
        let verifier = signer.paired_verifier();
        let primary: Arc<dyn BrokerAdapter> = Arc::new(ScriptedAdapter {
            id: BrokerId::Zerodha,
            fail_until: AtomicU32::new(primary_fail_until),
        });
        let backup: Arc<dyn BrokerAdapter> = Arc::new(ScriptedAdapter {
            id: BrokerId::Dhan,
            fail_until: AtomicU32::new(backup_fail_until),
        });
        let router = Arc::new(BrokerRouter::new(
            primary,
            backup,
            FailoverThresholds {
                error_rate_bps: 5_000,
                p99_latency_ms: 100_000,
                min_samples: 4,
            },
        ));
        let policy = RetryPolicy {
            max_attempts: 4,
            base_backoff_ns: 1,
            max_backoff_ns: 1,
            jitter_bps: 0,
        };
        let engine = ExecutionEngine::new(router, verifier, policy, ReplayMode::Off);
        (engine, signer)
    }

    fn sample_intent() -> OrderIntent {
        OrderIntent {
            correlation_id: 1u128.to_be_bytes(),
            symbol: 42,
            side: 0,
            quantity: 10,
            order_type: 0,
            limit_paise: 100_000,
            exchange: 0,
        }
    }

    /// Happy path: token verifies, adapter succeeds on attempt 1.
    #[tokio::test]
    async fn submit_happy_path_emits_submitted_lifecycle() {
        let (engine, signer) = make_engine(0, 0);
        let intent = sample_intent();
        let token = signer.sign(&intent, 10, 1_000, 1);
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let events = engine
            .submit(&token, &intent, 10, 1_000, 1, &hedge_obs::NoopEmitter, &mut j, &s)
            .await
            .expect("submit should succeed");

        let first = events.first().expect("at least one event");
        match first {
            EngineEvent::Lifecycle(l) => {
                assert_eq!(l.state, OrderLifecycleState::Submitted);
            }
            other => panic!("expected Lifecycle, got {:?}", other),
        }

        assert_eq!(engine.tracked_orders(), 1);
        let cid = CorrelationId(1);
        assert_eq!(engine.state_of(cid), Some(OrderLifecycleState::Submitted));
    }

    /// Tampered intent fails HMAC verification. No broker contact.
    #[tokio::test]
    async fn submit_rejects_when_token_does_not_match_intent() {
        let (engine, signer) = make_engine(0, 0);
        let intent = sample_intent();
        let token = signer.sign(&intent, 10, 1_000, 1);
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let mut tampered = intent;
        tampered.side = 1;

        let err = engine
            .submit(&token, &tampered, 10, 1_000, 1, &hedge_obs::NoopEmitter, &mut j, &s)
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::InvalidApprovalToken { .. }));
        assert_eq!(engine.tracked_orders(), 0);
    }

    /// Single-use enforcement.
    #[tokio::test]
    async fn submit_rejects_replayed_token() {
        let (engine, signer) = make_engine(0, 0);
        let intent = sample_intent();
        let token = signer.sign(&intent, 10, 1_000, 1);
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        engine
            .submit(&token, &intent, 10, 1_000, 1, &hedge_obs::NoopEmitter, &mut j, &s)
            .await
            .unwrap();
        let err = engine
            .submit(&token, &intent, 10, 1_000, 1, &hedge_obs::NoopEmitter, &mut j, &s)
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::DuplicateToken { .. }));
    }

    /// Retry path: adapter fails twice, succeeds third.
    #[tokio::test]
    async fn submit_retries_transient_then_succeeds() {
        let (engine, signer) = make_engine(2, 0);
        let intent = sample_intent();
        let token = signer.sign(&intent, 10, 1_000, 1);
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let events = engine
            .submit(&token, &intent, 10, 1_000, 1, &hedge_obs::NoopEmitter, &mut j, &s)
            .await
            .expect("third attempt succeeds");
        let lifecycle_count = events
            .iter()
            .filter(|e| matches!(e, EngineEvent::Lifecycle(_)))
            .count();
        assert_eq!(lifecycle_count, 1);
        assert_eq!(s.recorded().len(), 2, "two retries -> two sleeps");
    }

    /// Retry exhaustion lands FSM in Rejected.
    #[tokio::test]
    async fn submit_exhausts_retries_and_records_rejection() {
        let (engine, signer) = make_engine(99, 0);
        let intent = sample_intent();
        let token = signer.sign(&intent, 10, 1_000, 1);
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let err = engine
            .submit(&token, &intent, 10, 1_000, 1, &hedge_obs::NoopEmitter, &mut j, &s)
            .await
            .unwrap_err();
        assert!(matches!(err, ExecError::RetryExhausted { .. }));

        let cid = CorrelationId(1);
        assert_eq!(engine.state_of(cid), Some(OrderLifecycleState::Rejected));
    }

    /// on_fill computes per-fill delta correctly.
    #[tokio::test]
    async fn on_fill_drives_partial_then_filled() {
        let (engine, signer) = make_engine(0, 0);
        let intent = sample_intent();
        let token = signer.sign(&intent, 10, 1_000, 1);
        let mut j = NoJitter;
        let s = RecordingSleeper::new();
        engine
            .submit(&token, &intent, 10, 1_000, 1, &hedge_obs::NoopEmitter, &mut j, &s)
            .await
            .unwrap();
        let cid = CorrelationId(1);

        // First partial fill: 4 @ 99.50 cumulative.
        let evts = engine.on_fill(cid, 42, 0, 4, 99_50, 200).unwrap();
        assert!(evts.iter().any(|e| matches!(
            e,
            EngineEvent::Lifecycle(l) if l.state == OrderLifecycleState::PartiallyFilled
        )));
        let first_delta = evts
            .iter()
            .find_map(|e| match e {
                EngineEvent::Fill {
                    filled_qty,
                    avg_fill_paise,
                    ..
                } => Some((*filled_qty, *avg_fill_paise)),
                _ => None,
            })
            .expect("Fill emitted");
        assert_eq!(first_delta.0, 4);
        assert_eq!(first_delta.1, 99_50);

        // Second fill: cumulative 10 @ 100.00 — delta is 6 @ 100.33.
        // cum_notional = 10000*10 = 100_000;
        // prior_notional = 9950*4 = 39_800;
        // delta_notional = 60_200;
        // delta_qty = 6;
        // px = 60_200 / 6 = 10_033 paise.
        let evts = engine.on_fill(cid, 42, 0, 10, 100_00, 300).unwrap();
        assert!(evts.iter().any(|e| matches!(
            e,
            EngineEvent::Lifecycle(l) if l.state == OrderLifecycleState::Filled
        )));
        let second_delta = evts
            .iter()
            .find_map(|e| match e {
                EngineEvent::Fill {
                    filled_qty,
                    avg_fill_paise,
                    ..
                } => Some((*filled_qty, *avg_fill_paise)),
                _ => None,
            })
            .expect("Fill emitted");
        assert_eq!(second_delta.0, 6);
        assert_eq!(second_delta.1, 10_033);
        assert_eq!(first_delta.0 + second_delta.0, 10);
    }

    /// translate_intent maps every wire byte to the broker-api enum.
    #[test]
    fn translate_intent_round_trip() {
        let intent = sample_intent();
        let broker_intent = translate_intent(&intent, 7).unwrap();
        assert_eq!(broker_intent.symbol_raw, 42);
        assert_eq!(broker_intent.side, Side::Buy);
        assert_eq!(broker_intent.quantity, Qty::new(7));
        assert_eq!(broker_intent.order_type, OrderType::Market);
        assert_eq!(broker_intent.exchange, Exchange::Nse);
        assert_eq!(broker_intent.limit_paise, 100_000);
    }

    /// translate_intent rejects unknown wire bytes.
    #[test]
    fn translate_intent_rejects_unknown_discriminants() {
        let mut intent = sample_intent();
        intent.side = 99;
        assert!(translate_intent(&intent, 1).is_err());

        let mut intent = sample_intent();
        intent.order_type = 7;
        assert!(translate_intent(&intent, 1).is_err());

        let mut intent = sample_intent();
        intent.exchange = 9;
        assert!(translate_intent(&intent, 1).is_err());
    }

    /// Replay mode is observable.
    #[test]
    fn replay_mode_is_observable() {
        let signer = ApprovalSigner::from_key(b"k".to_vec());
        let primary: Arc<dyn BrokerAdapter> = Arc::new(ScriptedAdapter {
            id: BrokerId::Simulated,
            fail_until: AtomicU32::new(0),
        });
        let backup: Arc<dyn BrokerAdapter> = Arc::new(ScriptedAdapter {
            id: BrokerId::Simulated,
            fail_until: AtomicU32::new(0),
        });
        let router = Arc::new(BrokerRouter::new(
            primary,
            backup,
            FailoverThresholds {
                error_rate_bps: 5_000,
                p99_latency_ms: 100_000,
                min_samples: 4,
            },
        ));
        let engine = ExecutionEngine::new(
            router,
            signer.paired_verifier(),
            RetryPolicy::default(),
            ReplayMode::On,
        );
        assert_eq!(engine.replay_mode(), ReplayMode::On);
    }
}
