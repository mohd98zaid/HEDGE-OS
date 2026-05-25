//! Unified error type for the Execution_Engine.
//!
//! [`ExecError`] is the engine-internal error currency. Adapter-level
//! failures arrive as [`hedge_broker_api::BrokerError`] and are folded
//! into [`ExecError::Broker`] with the originating [`BrokerId`] for
//! attribution. The engine retry / failover policy reads [`broker_error`]
//! to classify retryability and failover relevance — `ExecError`
//! delegates those classifications to the underlying `BrokerError`
//! when present.

use thiserror::Error;

use hedge_broker_api::BrokerError;
use hedge_core::BrokerId;

/// The single error type produced by every public entry point of the
/// Execution_Engine.
#[derive(Debug, Error)]
pub enum ExecError {
    /// `ApprovalToken` HMAC failed verification against the canonical
    /// `OrderIntent_v1` bytes (R6.8). The Risk_Engine is the only
    /// process holding the signing key, so this almost always indicates
    /// either a tampered intent or an attempt to re-use a token that
    /// was minted for a different intent.
    ///
    /// On this error the engine emits `obs.error.exec.invalid_token`
    /// (R6.8) and rejects the submission without contacting any
    /// broker.
    #[error("approval token HMAC mismatch (intent.correlation_id_hex={correlation_id_hex})")]
    InvalidApprovalToken {
        /// Hex form of the 16-byte correlation id from the order intent.
        correlation_id_hex: String,
    },

    /// The token has already been consumed by a previous `submit`.
    /// `ApprovalToken`s are single-use (R5.14).
    #[error("approval token already consumed (intent.correlation_id_hex={correlation_id_hex})")]
    DuplicateToken {
        /// Hex form of the 16-byte correlation id from the order intent.
        correlation_id_hex: String,
    },

    /// FSM transition is illegal for the current state (Property 9).
    #[error("invalid FSM transition: {from:?} → {to:?}")]
    InvalidFsmTransition {
        /// Source state.
        from: hedge_schemas::order_state::OrderLifecycleState,
        /// Attempted target state.
        to: hedge_schemas::order_state::OrderLifecycleState,
    },

    /// Adapter-side failure surfaced by [`BrokerAdapter`](hedge_broker_api::BrokerAdapter).
    /// The carried [`BrokerError`] preserves the variant taxonomy — the
    /// retry layer reads `broker_err.is_retryable()` and the failover
    /// layer reads `broker_err.counts_toward_failover()`.
    #[error("broker {broker:?} (attempt {attempt}): {source}")]
    Broker {
        /// Which broker produced the error (the active slot at submit time).
        broker: BrokerId,
        /// 1-indexed attempt number that failed.
        attempt: u32,
        /// Underlying broker error.
        #[source]
        source: BrokerError,
    },

    /// Bus publish or stream consume failed.
    #[error("bus error: {0}")]
    Bus(#[from] hedge_bus::BusError),

    /// Configuration is missing or malformed.
    #[error("configuration error: {0}")]
    Config(String),

    /// Retry budget exhausted without success. Carries the last
    /// transient error so callers can introspect.
    #[error("retry budget exhausted after {attempts} attempts; last error: {last_error}")]
    RetryExhausted {
        /// Number of attempts made (= configured `max_attempts`).
        attempts: u32,
        /// Stringified last underlying error.
        last_error: String,
    },

    /// Internal invariant violation. Should never happen in production;
    /// if it does, the engine fails closed and the supervisor
    /// restarts the binary.
    #[error("internal error: {0}")]
    Internal(String),
}

impl ExecError {
    /// Returns `true` when the error is retryable. Delegates to
    /// [`BrokerError::is_retryable`] for `Broker` errors; every other
    /// variant is fatal for the current intent.
    #[inline]
    pub fn is_retryable(&self) -> bool {
        match self {
            ExecError::Broker { source, .. } => source.is_retryable(),
            _ => false,
        }
    }

    /// Returns `true` when this error counts toward the broker-failover
    /// sliding window (R6.5). Delegates to
    /// [`BrokerError::counts_toward_failover`].
    #[inline]
    pub fn counts_toward_failover(&self) -> bool {
        match self {
            ExecError::Broker { source, .. } => source.counts_toward_failover(),
            _ => false,
        }
    }

    /// Returns the broker associated with the error, if any. Used by
    /// the router to attribute failover-relevant failures.
    #[inline]
    pub const fn broker(&self) -> Option<BrokerId> {
        match self {
            ExecError::Broker { broker, .. } => Some(*broker),
            _ => None,
        }
    }

    /// Stable short tag used as the trailing segment of
    /// `obs.error.exec.<tag>` subjects and metric labels.
    #[inline]
    pub fn tag(&self) -> &'static str {
        match self {
            ExecError::InvalidApprovalToken { .. } => "invalid_token",
            ExecError::DuplicateToken { .. } => "duplicate_token",
            ExecError::InvalidFsmTransition { .. } => "invalid_fsm_transition",
            ExecError::Broker { source, .. } => match source {
                BrokerError::NotReady(_) => "broker_not_ready",
                BrokerError::Rejected(_) => "broker_rejected",
                BrokerError::Transient(_) => "broker_transient",
                BrokerError::Network(_) => "broker_network",
                BrokerError::Http { .. } => "broker_http",
                BrokerError::Auth(_) => "broker_auth",
                BrokerError::InvalidApprovalToken => "broker_invalid_token",
                BrokerError::UnknownOrderId(_) => "broker_unknown_order",
                BrokerError::Internal(_) => "broker_internal",
            },
            ExecError::Bus(_) => "bus",
            ExecError::Config(_) => "config",
            ExecError::RetryExhausted { .. } => "retry_exhausted",
            ExecError::Internal(_) => "internal",
        }
    }

    /// Convenience constructor: wrap a `BrokerError` with broker + attempt
    /// attribution.
    #[inline]
    pub fn from_broker(broker: BrokerId, attempt: u32, source: BrokerError) -> Self {
        Self::Broker { broker, attempt, source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_schemas::order_state::OrderLifecycleState;

    #[test]
    fn is_retryable_only_for_transient_or_network() {
        assert!(ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::Transient("timeout".into())
        )
        .is_retryable());
        assert!(ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::Network("dns".into())
        )
        .is_retryable());

        assert!(!ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::Rejected("bad lot".into())
        )
        .is_retryable());
        assert!(!ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::Auth("401".into())
        )
        .is_retryable());

        assert!(!ExecError::InvalidApprovalToken {
            correlation_id_hex: "00".repeat(16),
        }
        .is_retryable());
        assert!(!ExecError::Config("missing primary".into()).is_retryable());
    }

    #[test]
    fn counts_toward_failover_matches_broker_error() {
        assert!(ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::Transient("x".into())
        )
        .counts_toward_failover());
        assert!(ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::Rejected("x".into())
        )
        .counts_toward_failover());
        assert!(!ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::Auth("x".into())
        )
        .counts_toward_failover());
        assert!(!ExecError::from_broker(
            BrokerId::Zerodha,
            1,
            BrokerError::NotReady("x".into())
        )
        .counts_toward_failover());
    }

    #[test]
    fn broker_returns_id_for_broker_variant() {
        assert_eq!(
            ExecError::from_broker(BrokerId::Dhan, 2, BrokerError::Transient("x".into())).broker(),
            Some(BrokerId::Dhan)
        );
        assert_eq!(
            ExecError::Internal("x".into()).broker(),
            None,
            "non-broker variants must not attribute to a broker"
        );
    }

    #[test]
    fn tags_route_through_broker_error_taxonomy() {
        let cases: Vec<(&'static str, ExecError)> = vec![
            (
                "invalid_token",
                ExecError::InvalidApprovalToken {
                    correlation_id_hex: "00".repeat(16),
                },
            ),
            (
                "duplicate_token",
                ExecError::DuplicateToken {
                    correlation_id_hex: "00".repeat(16),
                },
            ),
            (
                "invalid_fsm_transition",
                ExecError::InvalidFsmTransition {
                    from: OrderLifecycleState::Filled,
                    to: OrderLifecycleState::Submitted,
                },
            ),
            (
                "broker_rejected",
                ExecError::from_broker(
                    BrokerId::Zerodha,
                    1,
                    BrokerError::Rejected("x".into()),
                ),
            ),
            (
                "broker_transient",
                ExecError::from_broker(
                    BrokerId::Zerodha,
                    1,
                    BrokerError::Transient("x".into()),
                ),
            ),
            (
                "broker_not_ready",
                ExecError::from_broker(
                    BrokerId::Zerodha,
                    1,
                    BrokerError::NotReady("missing".into()),
                ),
            ),
            (
                "broker_auth",
                ExecError::from_broker(
                    BrokerId::Zerodha,
                    1,
                    BrokerError::Auth("401".into()),
                ),
            ),
            (
                "retry_exhausted",
                ExecError::RetryExhausted {
                    attempts: 3,
                    last_error: "x".into(),
                },
            ),
        ];
        for (expected, e) in cases {
            assert_eq!(e.tag(), expected, "wrong tag for {:?}", e);
        }
    }
}
