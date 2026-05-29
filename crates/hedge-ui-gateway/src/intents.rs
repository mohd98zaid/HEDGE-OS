//! Trader-intent publishing on the `/control` channel.
//!
//! Trader → server messages with `"type": "intent"` cause the gateway
//! to publish a `trader.intent.*` event on NATS. The mapping from
//! [`crate::protocol::IntentKind`] → NATS subject is fixed by
//! [`IntentKind::nats_subject`](crate::protocol::IntentKind::nats_subject)
//! and aligns with the design's
//! [Authority Hierarchy and Decision Flow](../../.kiro/specs/project-hedge/design.md):
//! every trader intent flows through the Risk_Engine, which has final
//! authority and may reject or modify the intent.
//!
//! ### Validation
//!
//! Each intent kind has a minimum-shape requirement enforced before
//! publishing:
//!
//! * `killswitch` — `payload.active` must be a boolean.
//! * `strategy_toggle` — `payload.strategy_id` (string) and
//!   `payload.enabled` (boolean) must both be present.
//! * `priority` — `payload.symbol` (string) and `payload.tier`
//!   (`"P1"`/`"P2"`/`"P3"`/`"P4"`) must both be present.
//! * `order` — `payload.symbol` (string), `payload.side`
//!   (`"buy"`/`"sell"`), and `payload.quantity` (positive integer) must
//!   all be present.
//!
//! Validation failures surface as a [`crate::protocol::ServerMsg::Error`]
//! with [`ErrorCode::InvalidIntent`] and never reach NATS.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use thiserror::Error;

use crate::protocol::IntentKind;

/// Errors that can occur while publishing a trader intent.
#[derive(Debug, Error)]
pub enum IntentError {
    /// The supplied JSON payload did not satisfy the kind's shape contract.
    #[error("invalid {kind:?} payload: {reason}")]
    InvalidPayload {
        /// The intent kind that was rejected.
        kind: IntentKind,
        /// Human-readable reason for the rejection.
        reason: String,
    },
    /// The downstream NATS publish failed.
    #[error("nats publish to {subject} failed: {message}")]
    PublishFailed {
        /// Subject that was being published.
        subject: String,
        /// Error message from the underlying transport.
        message: String,
    },
}

/// Abstraction over the intent-publish backend so tests can substitute a
/// recording fake.
#[async_trait]
pub trait IntentPublisher: Send + Sync + 'static {
    /// Publish `payload` (already validated) on `subject`.
    async fn publish(&self, subject: &str, payload: Bytes) -> Result<(), IntentError>;
}

/// Validate a trader-intent payload against its kind's shape contract.
pub fn validate_intent(kind: IntentKind, payload: &Value) -> Result<(), IntentError> {
    let invalid = |reason: &str| IntentError::InvalidPayload {
        kind,
        reason: reason.to_owned(),
    };
    match kind {
        IntentKind::Killswitch => {
            payload
                .get("active")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid("missing boolean field `active`"))?;
        }
        IntentKind::StrategyToggle => {
            payload
                .get("strategy_id")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("missing string field `strategy_id`"))?;
            payload
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid("missing boolean field `enabled`"))?;
        }
        IntentKind::Priority => {
            payload
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("missing string field `symbol`"))?;
            let tier = payload
                .get("tier")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("missing string field `tier`"))?;
            if !matches!(tier, "P1" | "P2" | "P3" | "P4") {
                return Err(invalid("`tier` must be one of P1/P2/P3/P4"));
            }
        }
        IntentKind::Order => {
            payload
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("missing string field `symbol`"))?;
            let side = payload
                .get("side")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("missing string field `side`"))?;
            if !matches!(side, "buy" | "sell") {
                return Err(invalid("`side` must be `buy` or `sell`"));
            }
            let qty = payload
                .get("quantity")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid("missing integer field `quantity`"))?;
            if qty <= 0 {
                return Err(invalid("`quantity` must be positive"));
            }
        }
        IntentKind::TradingMode => {
            payload
                .get("live")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid("missing boolean field `live`"))?;
        }
    }
    Ok(())
}

/// Publish a validated trader intent through `publisher` on the canonical
/// NATS subject for `kind`.
pub async fn publish_intent<P: IntentPublisher + ?Sized>(
    publisher: &P,
    kind: IntentKind,
    payload: &Value,
) -> Result<(), IntentError> {
    validate_intent(kind, payload)?;
    let bytes = Bytes::from(serde_json::to_vec(payload).map_err(|e| {
        IntentError::InvalidPayload {
            kind,
            reason: format!("payload not JSON-serialisable: {}", e),
        }
    })?);
    publisher.publish(kind.nats_subject(), bytes).await
}

/// In-memory recording publisher for tests.
#[derive(Debug, Clone, Default)]
pub struct RecordingPublisher {
    inner: Arc<parking_lot::Mutex<Vec<(String, Bytes)>>>,
}

impl RecordingPublisher {
    /// Construct an empty recording publisher.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot every (subject, payload) pair seen so far.
    pub fn published(&self) -> Vec<(String, Bytes)> {
        self.inner.lock().clone()
    }
}

#[async_trait]
impl IntentPublisher for RecordingPublisher {
    async fn publish(&self, subject: &str, payload: Bytes) -> Result<(), IntentError> {
        self.inner.lock().push((subject.to_owned(), payload));
        Ok(())
    }
}

/// Production publisher that forwards to a typed `NatsPublisher`.
///
/// The trader-intent NATS account (`ui_gateway`) has publish permission
/// on `trader.*` only, so a misconfigured subject surfaces as a NATS ACL
/// rejection at this layer.
pub struct NatsIntentPublisher {
    client: hedge_bus::NatsClient,
}

impl NatsIntentPublisher {
    /// Construct a publisher from a connected `NatsClient`.
    pub fn new(client: hedge_bus::NatsClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl IntentPublisher for NatsIntentPublisher {
    async fn publish(&self, subject: &str, payload: Bytes) -> Result<(), IntentError> {
        self.client
            .raw()
            .publish(subject.to_owned(), payload)
            .await
            .map_err(|e| IntentError::PublishFailed {
                subject: subject.to_owned(),
                message: e.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn killswitch_requires_active_bool() {
        assert!(validate_intent(IntentKind::Killswitch, &json!({"active": true})).is_ok());
        assert!(validate_intent(IntentKind::Killswitch, &json!({"active": false})).is_ok());
        assert!(validate_intent(IntentKind::Killswitch, &json!({})).is_err());
        assert!(validate_intent(IntentKind::Killswitch, &json!({"active": "yes"})).is_err());
    }

    #[test]
    fn strategy_toggle_requires_id_and_enabled() {
        assert!(validate_intent(
            IntentKind::StrategyToggle,
            &json!({"strategy_id": "vwap_pullback", "enabled": false})
        )
        .is_ok());
        assert!(validate_intent(
            IntentKind::StrategyToggle,
            &json!({"enabled": true})
        )
        .is_err());
        assert!(validate_intent(
            IntentKind::StrategyToggle,
            &json!({"strategy_id": "x"})
        )
        .is_err());
    }

    #[test]
    fn priority_requires_symbol_and_valid_tier() {
        assert!(validate_intent(
            IntentKind::Priority,
            &json!({"symbol": "RELIANCE", "tier": "P1"})
        )
        .is_ok());
        assert!(validate_intent(
            IntentKind::Priority,
            &json!({"symbol": "RELIANCE", "tier": "P5"})
        )
        .is_err());
    }

    #[test]
    fn order_requires_symbol_side_qty() {
        assert!(validate_intent(
            IntentKind::Order,
            &json!({"symbol": "RELIANCE", "side": "buy", "quantity": 10})
        )
        .is_ok());
        assert!(validate_intent(
            IntentKind::Order,
            &json!({"symbol": "RELIANCE", "side": "buy", "quantity": 0})
        )
        .is_err());
        assert!(validate_intent(
            IntentKind::Order,
            &json!({"symbol": "RELIANCE", "side": "long", "quantity": 1})
        )
        .is_err());
    }

    #[tokio::test]
    async fn recording_publisher_captures_subject_and_payload() {
        let pub_ = RecordingPublisher::new();
        let payload = json!({"active": true});
        publish_intent(&pub_, IntentKind::Killswitch, &payload)
            .await
            .unwrap();
        let seen = pub_.published();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "trader.intent.killswitch");
        let v: Value = serde_json::from_slice(&seen[0].1).unwrap();
        assert_eq!(v, payload);
    }

    #[tokio::test]
    async fn invalid_payload_does_not_reach_publisher() {
        let pub_ = RecordingPublisher::new();
        let res = publish_intent(&pub_, IntentKind::Order, &json!({"symbol": "X"})).await;
        assert!(res.is_err());
        assert!(pub_.published().is_empty());
    }
}
