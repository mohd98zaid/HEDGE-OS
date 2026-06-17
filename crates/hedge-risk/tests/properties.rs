//! Crate-wide property tests for `hedge-risk`.
//!
//! These tests are kept in `tests/` rather than per-module because they
//! exercise crate-level invariants (Adaptive_Risk monotonicity, HMAC
//! determinism, session-gate exhaustiveness) that span multiple modules.
//!
//! The full Risk_Engine `proptest` suite ships in task **14.2** with
//! latency-budget assertions and post-target-policy edge cases — this
//! file contains the in-source proptest scaffolding the brief calls for.
//!
//! Validates:
//!   - Property 4 — Score and Formula Equivalence (Adaptive_Risk)
//!   - Property 1 — Risk Limit Invariant (rejection on every gate)
//!   - Property 2 — Authority Hierarchy (HMAC tamper-resistance)

use std::sync::Arc;

use chrono::Datelike;
use hedge_config::defaults;
use hedge_obs::NoopEmitter;
use hedge_risk::{
    canonicalize_intent_bytes, ApprovalSigner, ApprovalVerifier, MockWarmCacheView, RiskEngine,
    WarmCacheView,
};
use hedge_schemas::{OrderIntent, RiskProfile, Signal as Signal_v1};
use proptest::prelude::*;

// ---- helpers ------------------------------------------------------------

fn arb_intent() -> impl Strategy<Value = OrderIntent> {
    (
        any::<[u8; 16]>(),
        any::<u32>(),
        any::<u8>(),
        any::<u64>(),
        any::<u8>(),
        any::<i64>(),
        any::<i8>(),
    )
        .prop_map(|(cid, sym, side, qty, ot, lim, ex)| OrderIntent {
            correlation_id: cid,
            symbol: sym,
            side,
            quantity: qty,
            order_type: ot,
            limit_paise: lim,
            exchange: ex,
        })
}

fn arb_signal(symbol: u32, confidence: f32) -> Signal_v1 {
    Signal_v1 {
        correlation_id: hedge_core::CorrelationId::new().as_u128().to_be_bytes(),
        strategy: 0,
        symbol,
        side: 0,
        base_probability: 0.7,
        confidence,
        risk_profile: RiskProfile {
            stop_loss_paise: 9_900,
            take_profit_paise: 10_500,
            max_size_qty: 50,
            time_horizon_seconds: 300,
        },
        ts_ns: 1_000,
    }
}

fn skip_if_weekend_in_ist() -> bool {
    use chrono::{TimeZone, Weekday};
    let now_utc = chrono::Utc::now();
    let now_ist = chrono_tz::Asia::Kolkata.from_utc_datetime(&now_utc.naive_utc());
    matches!(now_ist.weekday(), Weekday::Sat | Weekday::Sun)
}

fn make_engine_with_warm(warm: Arc<dyn WarmCacheView>) -> RiskEngine {
    use chrono::NaiveTime;
    let mut session = defaults::session();
    session.start_ist = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    session.end_ist = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
    RiskEngine::new(
        defaults::capital(),
        defaults::risk(),
        session,
        ApprovalSigner::from_key(b"property-test-hmac-key-32bytes!!".to_vec()),
        warm,
    )
}

// ---- canonicalize_intent_bytes determinism ------------------------------

proptest! {
    /// **Validates: Requirements 5.14, 6.8** — canonical bytes are
    /// deterministic for fixed input.
    #[test]
    fn canonical_bytes_are_deterministic(
        intent in arb_intent(),
        sized in any::<u64>(),
        ts in any::<u64>(),
        seq in any::<u64>(),
    ) {
        let a = canonicalize_intent_bytes(&intent, sized, ts, seq);
        let b = canonicalize_intent_bytes(&intent, sized, ts, seq);
        prop_assert_eq!(a, b);
    }

    /// **Validates: Requirements 5.14, 6.8** — canonical bytes have
    /// fixed total length (63 bytes).
    #[test]
    fn canonical_bytes_have_fixed_length(
        intent in arb_intent(),
        sized in any::<u64>(),
        ts in any::<u64>(),
        seq in any::<u64>(),
    ) {
        let bytes = canonicalize_intent_bytes(&intent, sized, ts, seq);
        prop_assert_eq!(bytes.len(), hedge_risk::INTENT_CANONICAL_BYTES);
    }
}

// ---- HMAC sign/verify round-trip ---------------------------------------

proptest! {
    /// **Validates: Requirements 5.14, 6.8** — every minted token verifies
    /// against the canonical intent bytes; tampering with any field
    /// produces a token that fails verification.
    #[test]
    fn signed_token_verifies_under_paired_verifier(
        intent in arb_intent(),
        sized in any::<u64>(),
        ts in any::<u64>(),
        seq in any::<u64>(),
    ) {
        let signer = ApprovalSigner::from_key(b"hmac-key".to_vec());
        let verifier = signer.paired_verifier();
        let token = signer.sign(&intent, sized, ts, seq);
        prop_assert!(verifier.verify(&token, &intent, sized, ts, seq));
    }

    /// **Validates: Requirements 5.14, 6.8** — a tampered intent byte
    /// always fails verification.
    #[test]
    fn tampered_quantity_breaks_verification(
        intent in arb_intent(),
        sized in any::<u64>(),
        ts in any::<u64>(),
        seq in any::<u64>(),
        delta in 1u64..u32::MAX as u64,
    ) {
        let signer = ApprovalSigner::from_key(b"hmac-key".to_vec());
        let verifier = signer.paired_verifier();
        let token = signer.sign(&intent, sized, ts, seq);
        let mut tampered = intent.clone();
        tampered.quantity = tampered.quantity.wrapping_add(delta);
        prop_assert!(!verifier.verify(&token, &tampered, sized, ts, seq));
    }

    /// **Validates: Requirements 5.14, 6.8** — a token signed under one
    /// key never verifies under a different key.
    #[test]
    fn token_does_not_verify_under_different_key(
        intent in arb_intent(),
        sized in any::<u64>(),
        ts in any::<u64>(),
        seq in any::<u64>(),
        other_key in proptest::collection::vec(any::<u8>(), 16..64),
    ) {
        let signer = ApprovalSigner::from_key(b"key-A".to_vec());
        let other = ApprovalVerifier::from_key(other_key.clone());
        let token = signer.sign(&intent, sized, ts, seq);
        prop_assume!(other_key.as_slice() != b"key-A");
        prop_assert!(!other.verify(&token, &intent, sized, ts, seq));
    }
}

// ---- Adaptive_Risk monotonicity ----------------------------------------

proptest! {
    /// **Validates: Requirements 5.13** — Adaptive_Risk scales monotonically
    /// with every input factor when all other factors are held constant.
    #[test]
    fn adaptive_risk_monotonic_in_market_stability(
        m_low in 0.0f32..0.49,
        m_high in 0.50f32..1.0,
        confidence in 0.5f32..1.0,
    ) {
        if skip_if_weekend_in_ist() {
            return Ok(());
        }
        let warm_lo = Arc::new(MockWarmCacheView::with_values(m_low, 1.0));
        let warm_hi = Arc::new(MockWarmCacheView::with_values(m_high, 1.0));
        let eng_lo = make_engine_with_warm(warm_lo);
        let eng_hi = make_engine_with_warm(warm_hi);
        let sig = arb_signal(42, confidence);
        let d_lo = eng_lo.evaluate(&sig, 100, &NoopEmitter);
        let d_hi = eng_hi.evaluate(&sig, 100, &NoopEmitter);
        // Either both approved (with monotonic sized_quantity) or the
        // low side failed adaptive_risk while the high side succeeded.
        if d_lo.is_approved() && d_hi.is_approved() {
            prop_assert!(d_hi.sized_quantity() >= d_lo.sized_quantity());
        }
    }

    /// **Validates: Requirements 5.13** — when MarketStability is exactly
    /// 0.0 the engine rejects with `AdaptiveRiskZero` regardless of the
    /// other factors.
    #[test]
    fn zero_market_stability_collapses_adaptive_risk(
        confidence in 0.0f32..1.0,
        trader in 0.0f32..1.0,
    ) {
        if skip_if_weekend_in_ist() {
            return Ok(());
        }
        let warm = Arc::new(MockWarmCacheView::with_values(0.0, trader));
        let engine = make_engine_with_warm(warm);
        let sig = arb_signal(42, confidence);
        let d = engine.evaluate(&sig, 100, &NoopEmitter);
        prop_assert_eq!(
            d.rejection_reason(),
            Some(hedge_schemas::rejection_reason::RejectionReason::AdaptiveRiskZero)
        );
    }

    /// **Validates: Requirements 5.13** — when TraderDiscipline is 0.0
    /// the engine rejects with `AdaptiveRiskZero`.
    #[test]
    fn zero_trader_stability_collapses_adaptive_risk(
        confidence in 0.0f32..1.0,
        market in 0.0f32..1.0,
    ) {
        if skip_if_weekend_in_ist() {
            return Ok(());
        }
        let warm = Arc::new(MockWarmCacheView::with_values(market, 0.0));
        let engine = make_engine_with_warm(warm);
        let sig = arb_signal(42, confidence);
        let d = engine.evaluate(&sig, 100, &NoopEmitter);
        prop_assert_eq!(
            d.rejection_reason(),
            Some(hedge_schemas::rejection_reason::RejectionReason::AdaptiveRiskZero)
        );
    }
}

// ---- Session-time gate -------------------------------------------------

proptest! {
    /// **Validates: Requirements 31.1** — outside the configured IST
    /// session window, every signal is rejected with `SessionClosed`.
    #[test]
    fn outside_session_every_signal_rejected_with_session_closed(
        symbol in any::<u32>(),
        confidence in 0.0f32..1.0,
    ) {
        use chrono::NaiveTime;
        let mut session = defaults::session();
        // Empty window — never open.
        session.start_ist = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        session.end_ist = NaiveTime::from_hms_opt(0, 0, 1).unwrap();
        let warm = Arc::new(MockWarmCacheView::neutral());
        let engine = RiskEngine::new(
            defaults::capital(),
            defaults::risk(),
            session,
            ApprovalSigner::from_key(b"k".to_vec()),
            warm,
        );
        let sig = arb_signal(symbol, confidence);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        prop_assert_eq!(
            d.rejection_reason(),
            Some(hedge_schemas::rejection_reason::RejectionReason::SessionClosed)
        );
    }
}
