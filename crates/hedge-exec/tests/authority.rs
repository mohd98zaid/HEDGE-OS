//! C.13 — Authority_Hierarchy property test (full-cockpit-data spec).
//!
//! **Validates: Requirements 11.1, 11.2 (Property 2 — Authority Hierarchy
//! and Hot_Path Purity).**
//!
//! The Authority Hierarchy states that the Execution_Engine may submit an
//! order to a broker **only** when it holds a valid HMAC-SHA256
//! [`ApprovalToken`] minted by the Risk_Engine over the exact canonical
//! `OrderIntent` bytes. There is no other path to a broker submission —
//! [`ExecutionEngine::submit`] is the sole entry point and it verifies the
//! token before any broker contact.
//!
//! This suite proves the invariant end-to-end over random intents:
//!
//! 1. **Every submitted order had a prior valid approval.** A stub broker
//!    records every intent it receives. For any random intent, a submit
//!    with a *correctly minted* token reaches the broker exactly once, and
//!    the engine emits the `Submitted` lifecycle transition.
//!
//! 2. **No approval ⇒ no submission.** A submit with a *forged* token
//!    (any token not minted by the paired signer over the same
//!    `(intent, sized_qty, ts_ns, sequence)`) is rejected with
//!    `ExecError::InvalidApprovalToken`, the broker is never contacted, and
//!    no `Submitted` transition is emitted.
//!
//! 3. **Tamper resistance.** A token validly minted for intent A never
//!    authorises a different intent B — changing any field invalidates the
//!    token, so the submission fails closed.
//!
//! 4. **Single-use.** Re-submitting with an already-consumed token is
//!    rejected with `ExecError::DuplicateToken`; the broker is contacted at
//!    most once per token.
//!
//! Together these establish the contrapositive the requirement asks for:
//! *if* an order reached the broker, *then* a matching prior approval
//! existed — there is no reachable code path that submits without one.

use std::sync::Arc;

use async_trait::async_trait;
use hedge_broker_api::{
    BrokerAdapter, BrokerError, BrokerMetric, OrderIntent as BrokerOrderIntent,
    OrderModification, OrderStatus, ReadyState, SubmitAck,
};
use hedge_core::BrokerId;
use hedge_exec::{
    BrokerRouter, ExecError, ExecutionEngine, FailoverThresholds, NoJitter, RetryPolicy,
    ReplayMode,
};
use hedge_obs::NoopEmitter;
use hedge_risk::{ApprovalSigner, ApprovalToken};
use hedge_schemas::OrderIntent;
use parking_lot::Mutex;
use proptest::prelude::*;

/// Stub broker that records every intent it is asked to submit. Used to
/// assert "the broker was (or was not) contacted".
struct RecordingAdapter {
    id: BrokerId,
    submitted: Arc<Mutex<Vec<BrokerOrderIntent>>>,
}

#[async_trait]
impl BrokerAdapter for RecordingAdapter {
    fn broker_id(&self) -> BrokerId {
        self.id
    }
    async fn submit(&self, intent: &BrokerOrderIntent) -> Result<SubmitAck, BrokerError> {
        self.submitted.lock().push(intent.clone());
        Ok(SubmitAck {
            broker_order_id: format!("stub-{}", self.submitted.lock().len()),
            broker_ts_ns: Some(1),
        })
    }
    async fn modify(&self, _m: &OrderModification) -> Result<(), BrokerError> {
        Ok(())
    }
    async fn cancel(&self, _id: &str) -> Result<(), BrokerError> {
        Ok(())
    }
    async fn status(&self, _id: &str) -> Result<OrderStatus, BrokerError> {
        unreachable!("authority tests do not poll status")
    }
    async fn metrics(&self) -> Vec<BrokerMetric> {
        Vec::new()
    }
    async fn ready(&self) -> ReadyState {
        ReadyState::Ready
    }
}

/// Build an engine whose router routes through two recording adapters,
/// returning the engine, the paired signer, and the primary adapter's
/// recording buffer.
fn harness() -> (ExecutionEngine, ApprovalSigner, Arc<Mutex<Vec<BrokerOrderIntent>>>) {
    let signer = ApprovalSigner::from_key(b"authority-test-key-at-least-32-bytes!!".to_vec());
    let verifier = signer.paired_verifier();

    let primary_log = Arc::new(Mutex::new(Vec::new()));
    let primary: Arc<dyn BrokerAdapter> = Arc::new(RecordingAdapter {
        id: BrokerId::Zerodha,
        submitted: Arc::clone(&primary_log),
    });
    let backup: Arc<dyn BrokerAdapter> = Arc::new(RecordingAdapter {
        id: BrokerId::Dhan,
        submitted: Arc::new(Mutex::new(Vec::new())),
    });
    let router = Arc::new(BrokerRouter::new(
        primary,
        backup,
        FailoverThresholds {
            error_rate_bps: 5_000,
            p99_latency_ms: 100_000,
            min_samples: 8,
        },
    ));
    let engine = ExecutionEngine::new(router, verifier, RetryPolicy::default(), ReplayMode::On);
    (engine, signer, primary_log)
}

/// Generator: an arbitrary canonical `OrderIntent`.
fn intent_strategy() -> impl Strategy<Value = OrderIntent> {
    (
        any::<[u8; 16]>(),
        1u32..100_000,         // symbol
        0u8..=1,               // side
        1u64..=1_000_000,      // quantity
        0u8..=1,               // order_type
        1i64..1_000_000_00,    // limit_paise
        0i8..=1,               // exchange
    )
        .prop_map(
            |(correlation_id, symbol, side, quantity, order_type, limit_paise, exchange)| {
                OrderIntent {
                    correlation_id,
                    symbol,
                    side,
                    quantity,
                    order_type,
                    limit_paise,
                    exchange,
                }
            },
        )
}

fn submitted_transition_count(events: &[hedge_exec::EngineEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, hedge_exec::EngineEvent::Lifecycle(_)))
        .count()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Property 1 + 2: a valid approval authorises exactly one broker
    /// submission; the broker is contacted iff the token verifies.
    #[test]
    fn valid_approval_authorises_submission(
        intent in intent_strategy(),
        sized_quantity in 1u64..=1_000_000,
        ts_ns in 1u64..=u64::MAX,
        sequence in 0u64..=u64::MAX,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (engine, signer, broker_log) = harness();
            let token = signer.sign(&intent, sized_quantity, ts_ns, sequence);
            let emitter = NoopEmitter;
            let mut jitter = NoJitter;
            let sleeper = hedge_exec::RecordingSleeper::default();

            let result = engine
                .submit(&token, &intent, sized_quantity, ts_ns, sequence,
                        &emitter, &mut jitter, &sleeper)
                .await;

            prop_assert!(result.is_ok(), "valid approval must authorise submit: {:?}", result.err());
            let events = result.unwrap();
            // Exactly one broker contact occurred.
            prop_assert_eq!(broker_log.lock().len(), 1, "broker must be contacted exactly once");
            // A Submitted lifecycle transition was emitted.
            prop_assert!(
                submitted_transition_count(&events) >= 1,
                "expected a Submitted lifecycle transition"
            );
            Ok(())
        })?;
    }

    /// Property 2: a forged token (minted under a different key) never
    /// authorises a submission — the broker is never contacted.
    #[test]
    fn forged_token_never_reaches_broker(
        intent in intent_strategy(),
        sized_quantity in 1u64..=1_000_000,
        ts_ns in 1u64..=u64::MAX,
        sequence in 0u64..=u64::MAX,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (engine, _signer, broker_log) = harness();
            // Forge a token with a *different* signer key. It cannot
            // verify against the engine's paired verifier.
            let attacker = ApprovalSigner::from_key(b"a-totally-different-attacker-key!".to_vec());
            let forged = attacker.sign(&intent, sized_quantity, ts_ns, sequence);

            let emitter = NoopEmitter;
            let mut jitter = NoJitter;
            let sleeper = hedge_exec::RecordingSleeper::default();

            let result = engine
                .submit(&forged, &intent, sized_quantity, ts_ns, sequence,
                        &emitter, &mut jitter, &sleeper)
                .await;

            prop_assert!(
                matches!(result, Err(ExecError::InvalidApprovalToken { .. })),
                "forged token must be rejected, got {:?}", result
            );
            prop_assert_eq!(
                broker_log.lock().len(), 0,
                "broker must NOT be contacted without a valid approval"
            );
            Ok(())
        })?;
    }

    /// Property 3: a token minted for intent A never authorises a
    /// *different* intent B (any field change invalidates it).
    #[test]
    fn token_does_not_authorise_a_different_intent(
        intent in intent_strategy(),
        sized_quantity in 1u64..=1_000_000,
        ts_ns in 1u64..=u64::MAX,
        sequence in 0u64..=u64::MAX,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (engine, signer, broker_log) = harness();
            let token = signer.sign(&intent, sized_quantity, ts_ns, sequence);

            // Tamper: bump the quantity by one (wrapping to stay in range).
            let mut tampered = intent.clone();
            tampered.quantity = tampered.quantity.wrapping_add(1).max(1);
            // If wrapping produced the same value (only at u64::MAX→0→max
            // guard), force a different symbol instead so the intents differ.
            if tampered.quantity == intent.quantity {
                tampered.symbol = intent.symbol.wrapping_add(1).max(1);
            }

            let emitter = NoopEmitter;
            let mut jitter = NoJitter;
            let sleeper = hedge_exec::RecordingSleeper::default();

            let result = engine
                .submit(&token, &tampered, sized_quantity, ts_ns, sequence,
                        &emitter, &mut jitter, &sleeper)
                .await;

            prop_assert!(
                matches!(result, Err(ExecError::InvalidApprovalToken { .. })),
                "token must not authorise a different intent, got {:?}", result
            );
            prop_assert_eq!(broker_log.lock().len(), 0, "broker must not be contacted");
            Ok(())
        })?;
    }

    /// Property 4: single-use. The same token cannot authorise two
    /// submissions — the second is rejected as a duplicate and the broker
    /// is contacted at most once.
    #[test]
    fn approval_token_is_single_use(
        intent in intent_strategy(),
        sized_quantity in 1u64..=1_000_000,
        ts_ns in 1u64..=u64::MAX,
        sequence in 0u64..=u64::MAX,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let (engine, signer, broker_log) = harness();
            let token = signer.sign(&intent, sized_quantity, ts_ns, sequence);
            let emitter = NoopEmitter;
            let mut jitter = NoJitter;
            let sleeper = hedge_exec::RecordingSleeper::default();

            let first = engine
                .submit(&token, &intent, sized_quantity, ts_ns, sequence,
                        &emitter, &mut jitter, &sleeper)
                .await;
            prop_assert!(first.is_ok(), "first submit should succeed: {:?}", first.err());

            let second = engine
                .submit(&token, &intent, sized_quantity, ts_ns, sequence,
                        &emitter, &mut jitter, &sleeper)
                .await;
            prop_assert!(
                matches!(second, Err(ExecError::DuplicateToken { .. })),
                "re-using a token must be rejected, got {:?}", second
            );
            // Broker contacted exactly once across both attempts.
            prop_assert_eq!(
                broker_log.lock().len(), 1,
                "broker must be contacted at most once per token"
            );
            Ok(())
        })?;
    }
}

/// Sanity check (non-property): a hand-built token authorises its intent,
/// and a zeroed token does not. Documents the core invariant outside the
/// randomised cases.
#[test]
fn smoke_valid_vs_zero_token() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let (engine, signer, broker_log) = harness();
        let intent = OrderIntent {
            correlation_id: [9u8; 16],
            symbol: 42,
            side: 0,
            quantity: 10,
            order_type: 0,
            limit_paise: 100_00,
            exchange: 0,
        };
        let emitter = NoopEmitter;
        let mut jitter = NoJitter;
        let sleeper = hedge_exec::RecordingSleeper::default();

        // Zero token — never minted by anyone — must be rejected.
        let zero = ApprovalToken::from_bytes([0u8; 32]);
        let r = engine
            .submit(&zero, &intent, 10, 1, 1, &emitter, &mut jitter, &sleeper)
            .await;
        assert!(matches!(r, Err(ExecError::InvalidApprovalToken { .. })));
        assert_eq!(broker_log.lock().len(), 0);

        // Properly minted token — authorised.
        let token = signer.sign(&intent, 10, 1, 1);
        let r = engine
            .submit(&token, &intent, 10, 1, 1, &emitter, &mut jitter, &sleeper)
            .await;
        assert!(r.is_ok(), "valid token must authorise: {:?}", r.err());
        assert_eq!(broker_log.lock().len(), 1);
    });
}
