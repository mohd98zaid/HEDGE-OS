//! Property-based tests for `hedge-broker-api` (task 17.2).
//!
//! **Validates: Requirements 7.2, 7.4, 7.5**

use hedge_broker_api::{BrokerError, Exchange, OrderType, ReadyState};
use proptest::prelude::*;

proptest! {
    #[test]
    fn order_type_roundtrip_valid(byte in 0u8..2u8) {
        if let Some(ot) = OrderType::from_u8(byte) {
            prop_assert_eq!(ot.as_u8(), byte);
        }
    }

    #[test]
    fn order_type_from_u8_none_for_invalid(byte in 2u8..=255u8) {
        prop_assert!(OrderType::from_u8(byte).is_none());
    }

    #[test]
    fn exchange_roundtrip_valid(byte in 0i8..2i8) {
        if let Some(ex) = Exchange::from_i8(byte) {
            prop_assert_eq!(ex.as_i8(), byte);
        }
    }

    #[test]
    fn exchange_from_i8_none_for_invalid(byte in 2i8..=127i8) {
        prop_assert!(Exchange::from_i8(byte).is_none());
    }

    #[test]
    fn exchange_as_str_valid(byte in 0i8..2i8) {
        if let Some(ex) = Exchange::from_i8(byte) {
            let s = ex.as_str();
            prop_assert!(s == "NSE" || s == "BSE");
        }
    }

    #[test]
    fn retryable_implies_counts_toward_failover(
        variant in 0u8..9,
        msg in ".*",
    ) {
        let err = match variant {
            0 => BrokerError::NotReady(msg),
            1 => BrokerError::Rejected(msg),
            2 => BrokerError::Transient(msg),
            3 => BrokerError::Network(msg),
            4 => BrokerError::Http { status: 500, body: msg },
            5 => BrokerError::Auth(msg),
            6 => BrokerError::InvalidApprovalToken,
            7 => BrokerError::UnknownOrderId(msg),
            _ => BrokerError::Internal(msg),
        };
        if err.is_retryable() {
            prop_assert!(err.counts_toward_failover());
        }
    }

    #[test]
    fn auth_never_retryable(msg in ".*") {
        let err = BrokerError::Auth(msg);
        prop_assert!(!err.is_retryable());
    }

    #[test]
    fn not_ready_never_retryable(msg in ".*") {
        let err = BrokerError::NotReady(msg);
        prop_assert!(!err.is_retryable());
    }

    #[test]
    fn auth_never_failover(msg in ".*") {
        let err = BrokerError::Auth(msg);
        prop_assert!(!err.counts_toward_failover());
    }

    #[test]
    fn not_ready_never_failover(msg in ".*") {
        let err = BrokerError::NotReady(msg);
        prop_assert!(!err.counts_toward_failover());
    }

    #[test]
    fn ready_state_config_error_consistent(msg in ".*") {
        let rs = ReadyState::ConfigError(msg);
        prop_assert!(!rs.is_ready());
        prop_assert!(rs.is_unready());
    }

    #[test]
    fn ready_state_disconnected_consistent(msg in ".*") {
        let rs = ReadyState::Disconnected(msg);
        prop_assert!(!rs.is_ready());
        prop_assert!(rs.is_unready());
    }
}

#[test]
fn ready_state_ready_consistent() {
    let rs = ReadyState::Ready;
    assert!(rs.is_ready());
    assert!(!rs.is_unready());
}
