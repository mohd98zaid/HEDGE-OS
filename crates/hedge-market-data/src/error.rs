//! Error type for the Market_Data_Engine.
//!
//! All fallible entry points in this crate funnel through [`MarketDataError`]
//! so call sites in the engine, bin, and tests can pattern-match on the
//! same shape. The variants intentionally collapse the underlying
//! `tokio_tungstenite` and `serde_json` errors into `String` because
//! neither is `Clone` and we never reroute on the inner type — only on
//! the variant.

use thiserror::Error;

/// Top-level error returned by the Market_Data_Engine.
#[derive(Debug, Error)]
pub enum MarketDataError {
    /// WebSocket transport error (connect, read, write, or close).
    #[error("websocket error on source `{source_name}`: {message}")]
    WebSocket {
        /// Logical feed name, e.g. `"nse_l1"` or `"bse_l2"`.
        source_name: String,
        /// Lossy rendering of the underlying `tokio_tungstenite::Error`.
        message: String,
    },

    /// Protocol parser rejected a payload.
    ///
    /// `protocol_name` is the value returned by [`crate::protocol::MarketDataProtocol::name`]
    /// so logs can identify which placeholder parser produced the error.
    #[error("protocol `{protocol_name}` failed to parse payload: {message}")]
    Parse {
        /// Name of the protocol implementation that rejected the payload.
        protocol_name: &'static str,
        /// Human-readable reason.
        message: String,
    },

    /// Bus operation (NATS publish, subscribe) failed.
    #[error("bus error: {0}")]
    Bus(#[from] hedge_bus::BusError),

    /// WebSocket adapter exhausted its bounded reconnect budget.
    ///
    /// Currently informational — the live adapter loops the reconnect
    /// schedule indefinitely (capped at 30 s) so this variant is reserved
    /// for tests and future supervisor-driven shutdown.
    #[error("source `{source_name}` exceeded reconnect budget after {attempts} attempts")]
    ReconnectBudgetExhausted {
        /// Logical feed name.
        source_name: String,
        /// Number of reconnect attempts that ran before giving up.
        attempts: u32,
    },

    /// Configuration was invalid at startup (missing creds, bad URL, etc.).
    #[error("configuration error: {0}")]
    Config(String),
}

impl MarketDataError {
    /// Construct a [`MarketDataError::WebSocket`] from any displayable error.
    #[inline]
    pub fn websocket<E: std::fmt::Display>(source_name: impl Into<String>, err: E) -> Self {
        Self::WebSocket {
            source_name: source_name.into(),
            message: err.to_string(),
        }
    }

    /// Construct a [`MarketDataError::Parse`] from any displayable error.
    #[inline]
    pub fn parse<E: std::fmt::Display>(protocol_name: &'static str, err: E) -> Self {
        Self::Parse {
            protocol_name,
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_helper_renders_source_and_message() {
        let e = MarketDataError::websocket("nse_l1", "connection refused");
        let s = format!("{}", e);
        assert!(s.contains("nse_l1"), "{}", s);
        assert!(s.contains("connection refused"), "{}", s);
    }

    #[test]
    fn parse_helper_carries_static_protocol_name() {
        let e = MarketDataError::parse("NseProtocolPlaceholder", "expected object");
        match e {
            MarketDataError::Parse { protocol_name, message } => {
                assert_eq!(protocol_name, "NseProtocolPlaceholder");
                assert!(message.contains("expected object"));
            }
            other => panic!("wrong variant: {:?}", other),
        }
    }
}
