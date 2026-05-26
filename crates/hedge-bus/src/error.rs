//! Unified error type for the bus.
//!
//! Wraps the underlying transport error types (`async_nats`, `redis`) and the
//! codec errors so call sites only have to deal with [`BusError`].

use thiserror::Error;

/// Top-level error returned by every bus operation.
#[derive(Debug, Error)]
pub enum BusError {
    /// Failure to connect to the NATS server.
    ///
    /// `async_nats::ConnectError` is not `Clone` and contains a non-public
    /// inner type, so we render it to a `String` at construction time rather
    /// than carrying the original around. The lossy conversion is acceptable
    /// because callers route on the variant, not on the underlying type.
    #[error("nats connect failed: {0}")]
    NatsConnect(String),

    /// NATS publish failed (broker rejection, network drop, etc).
    #[error("nats publish failed on {subject}: {message}")]
    NatsPublish {
        /// The subject the publish targeted.
        subject: String,
        /// Underlying error message.
        message: String,
    },

    /// NATS subscribe failed (ACL rejection, malformed subject, etc).
    #[error("nats subscribe failed on {subject}: {message}")]
    NatsSubscribe {
        /// The subject the subscribe targeted.
        subject: String,
        /// Underlying error message.
        message: String,
    },

    /// Redis call failed (connection drop, command error, etc).
    #[error("redis error on stream {stream}: {source}")]
    Redis {
        /// The stream key the operation targeted.
        stream: String,
        /// Wrapped `redis::RedisError`.
        #[source]
        source: redis::RedisError,
    },

    /// Codec encode failed.
    #[error("codec encode failed: {0}")]
    Encode(String),

    /// Codec decode failed.
    #[error("codec decode failed: {0}")]
    Decode(String),

    /// Stream-entry payload was malformed (e.g. missing the `payload` field).
    #[error("malformed stream entry id={id}: {reason}")]
    MalformedEntry {
        /// The Redis Stream entry ID.
        id: String,
        /// Human-readable reason.
        reason: &'static str,
    },

    /// The subscriber's underlying channel closed before a message arrived.
    /// Callers should treat this as terminal for the subscription.
    #[error("subscription closed for {subject}")]
    SubscriptionClosed {
        /// The subject whose subscription terminated.
        subject: String,
    },
}

impl BusError {
    /// Helper for converting an `async_nats` publish error into a [`BusError`]
    /// without losing the subject context.
    #[inline]
    pub fn publish<E: std::fmt::Display>(subject: impl Into<String>, source: E) -> Self {
        Self::NatsPublish {
            subject: subject.into(),
            message: source.to_string(),
        }
    }

    /// Helper for converting an `async_nats` subscribe error into a [`BusError`].
    #[inline]
    pub fn subscribe<E: std::fmt::Display>(subject: impl Into<String>, source: E) -> Self {
        Self::NatsSubscribe {
            subject: subject.into(),
            message: source.to_string(),
        }
    }

    /// Helper for wrapping a `redis::RedisError` together with the stream key.
    #[inline]
    pub fn redis(stream: impl Into<String>, source: redis::RedisError) -> Self {
        Self::Redis {
            stream: stream.into(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_renders_subject_for_publish_error() {
        let e = BusError::publish("md.tick.42", "broker said no");
        let s = format!("{}", e);
        assert!(s.contains("md.tick.42"), "subject missing: {}", s);
        assert!(s.contains("broker said no"), "source missing: {}", s);
    }

    #[test]
    fn display_renders_stream_for_redis_error() {
        // Construct a representative `redis::RedisError` via `From<io::Error>`,
        // which is the documented public conversion.
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "down");
        let redis_err: redis::RedisError = io_err.into();
        let e = BusError::redis("hedge.hot.signals", redis_err);
        let s = format!("{}", e);
        assert!(s.contains("hedge.hot.signals"), "stream missing: {}", s);
    }

    #[test]
    fn malformed_entry_carries_id_and_reason() {
        let e = BusError::MalformedEntry {
            id: "1700000000000-0".into(),
            reason: "missing payload field",
        };
        let s = format!("{}", e);
        assert!(s.contains("1700000000000-0"));
        assert!(s.contains("missing payload field"));
    }
}
