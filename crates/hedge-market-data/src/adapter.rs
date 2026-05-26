//! WebSocket adapter abstraction and live implementation.
//!
//! Implements R1.1 ("ingest NSE/BSE WebSocket connections"), R1.6 ("emit a
//! connection-status event ... attempt reconnection"), and the design's
//! `WsAdapter<*>` shape (one Tokio task per upstream connection).
//!
//! ### Reconnect schedule
//!
//! Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms, 3200ms, 6400ms,
//! 12800ms, 25600ms, then capped at 30000ms for every subsequent attempt.
//!
//! ### Connection-status events
//!
//! Every disconnect and every reconnect publishes a JSON
//! [`ConnectionEvent`] on `md.connection.<source>` via the
//! [`hedge_bus::JsonCodec`]. The adapter also increments
//! `hedge_websocket_drops_total{source}` on every disconnect.

use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures::SinkExt;
use futures::StreamExt;
use hedge_bus::{subjects, JsonCodec, NatsClient, NatsPublisher, Subject};
use hedge_obs::metrics;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tracing::instrument;

use crate::error::MarketDataError;
use crate::protocol::{MarketDataProtocol, RawTick};

/// Backoff schedule used by [`LiveWsAdapter::reconnect`].
///
/// Index `i` returns the delay for the *i-th* (zero-based) reconnect
/// attempt. After the table is exhausted every subsequent attempt uses
/// the cap of 30 000 ms.
const RECONNECT_BACKOFF_MS: &[u64] =
    &[100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600];

/// Hard cap for the backoff schedule, in milliseconds.
pub const RECONNECT_CAP_MS: u64 = 30_000;

/// Compute the reconnect delay for attempt `n` (zero-based).
///
/// Returns the n-th entry in [`RECONNECT_BACKOFF_MS`] when `n` is in range,
/// and [`RECONNECT_CAP_MS`] thereafter.
pub fn reconnect_delay_for(attempt: u32) -> Duration {
    let idx = attempt as usize;
    let ms = RECONNECT_BACKOFF_MS
        .get(idx)
        .copied()
        .unwrap_or(RECONNECT_CAP_MS);
    Duration::from_millis(ms)
}

/// Connection-status event published on `md.connection.<source>` (R1.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ConnectionEvent {
    /// Logical source identifier (`"nse_l1"`, `"bse_l2"`, `"options"`).
    pub source: String,
    /// Human-readable status (`"disconnected"` or `"reconnected"`).
    pub status: ConnectionStatus,
    /// Best-effort reason text on disconnect; `None` on reconnect.
    pub reason: Option<String>,
    /// Reconnect attempt number that fired this event (zero on the very
    /// first connection).
    pub attempt: u32,
    /// RFC 3339 wall-clock timestamp at emission.
    pub at: String,
}

/// Connection status discriminant.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    /// The adapter just lost its upstream connection.
    Disconnected,
    /// The adapter just successfully reconnected.
    Reconnected,
}

/// Trait shared by every concrete WebSocket adapter.
///
/// `MarketDataProtocol` is the generic parameter so the same trait shape
/// works for `LiveWsAdapter<NseProtocolPlaceholder>`,
/// `LiveWsAdapter<BseProtocolPlaceholder>`, and
/// `LiveWsAdapter<OptionsChainProtocolPlaceholder>`.
///
/// Uses `async fn` in trait (stable since Rust 1.75); not object-safe by
/// design — the engine binds adapters via generic type parameters, never
/// behind `dyn`.
pub trait WsAdapter<P: MarketDataProtocol>: Send {
    /// Receive the next normalized message.
    ///
    /// Implementations are expected to return `MarketDataError::WebSocket`
    /// on a transport drop so the engine can branch into [`Self::reconnect`].
    async fn next_message(&mut self) -> Result<RawTick, MarketDataError>;

    /// Drop the current connection and re-open it. Implementations must
    /// honour the [`RECONNECT_BACKOFF_MS`] schedule.
    async fn reconnect(&mut self) -> Result<(), MarketDataError>;
}

/// Live `tokio-tungstenite`-backed WebSocket adapter.
///
/// Owns the raw WebSocket stream, a clone of the protocol parser, and a
/// NATS publisher used to emit `md.connection.<source>` events. The
/// adapter's reconnect loop is exposed as a public method
/// [`reconnect`](Self::reconnect) so the engine task can call it on
/// transport failure without duplicating the schedule.
pub struct LiveWsAdapter<P: MarketDataProtocol> {
    /// Logical source identifier; surfaced on metrics labels and
    /// `md.connection.<source>` subjects.
    source: String,
    /// WebSocket URL the adapter (re)connects to. Held so the reconnect
    /// path does not need additional state.
    url: String,
    /// Optional connected stream. Becomes `None` while the adapter is in
    /// the reconnect loop.
    stream: Option<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
    /// Protocol parser. Stateless and trivially cloneable in practice
    /// (placeholder implementations are zero-sized POD).
    protocol: P,
    /// Publisher for `md.connection.<source>`.
    connection_publisher: NatsPublisher<ConnectionEvent, JsonCodec<ConnectionEvent>>,
    /// Reconnect-attempt counter. Resets to zero on a successful
    /// connection.
    attempts: u32,
}

impl<P: MarketDataProtocol> LiveWsAdapter<P> {
    /// Construct a new adapter and open its initial WebSocket connection.
    ///
    /// On initial-connect failure this returns the underlying error; the
    /// caller (typically the engine startup path) chooses whether to
    /// retry or fail closed. After construction, transient drops are
    /// handled internally by [`reconnect`](Self::reconnect).
    #[instrument(level = "info", skip(nats, protocol, source, url))]
    pub async fn connect(
        nats: &NatsClient,
        source: impl Into<String>,
        url: impl Into<String>,
        protocol: P,
    ) -> Result<Self, MarketDataError> {
        let source = source.into();
        let url = url.into();
        tracing::info!(%source, %url, "connecting market-data adapter");

        let subject: Subject<ConnectionEvent> = subjects::md_connection(&source);
        let publisher = nats.publisher(subject, JsonCodec::<ConnectionEvent>::new());

        let (stream, _) = connect_async(&url)
            .await
            .map_err(|e| MarketDataError::websocket(&source, e))?;

        Ok(Self {
            source,
            url,
            stream: Some(stream),
            protocol,
            connection_publisher: publisher,
            attempts: 0,
        })
    }

    /// Construct an adapter from a pre-opened stream, primarily for tests
    /// (so a `tokio::io::duplex`-backed handshake can stand in for a real
    /// WebSocket dial).
    pub fn from_stream(
        nats: &NatsClient,
        source: impl Into<String>,
        url: impl Into<String>,
        stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
        protocol: P,
    ) -> Self {
        let source = source.into();
        let subject: Subject<ConnectionEvent> = subjects::md_connection(&source);
        let publisher = nats.publisher(subject, JsonCodec::<ConnectionEvent>::new());
        Self {
            source,
            url: url.into(),
            stream: Some(stream),
            protocol,
            connection_publisher: publisher,
            attempts: 0,
        }
    }

    /// The logical feed source identifier.
    #[inline]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Number of reconnect attempts since the last successful connection.
    #[inline]
    pub fn attempt_count(&self) -> u32 {
        self.attempts
    }

    /// Receive the next raw tick from the WebSocket.
    ///
    /// Skips control frames (Ping, Pong, Close), surfaces transport errors
    /// as [`MarketDataError::WebSocket`], and routes binary or text frames
    /// through the configured protocol parser.
    #[instrument(level = "trace", skip(self), fields(source = %self.source))]
    pub async fn next_message(&mut self) -> Result<RawTick, MarketDataError> {
        loop {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| MarketDataError::websocket(&self.source, "stream is None"))?;
            match stream.next().await {
                Some(Ok(Message::Text(t))) => {
                    return self.protocol.parse(t.as_bytes());
                }
                Some(Ok(Message::Binary(b))) => {
                    return self.protocol.parse(&b);
                }
                Some(Ok(Message::Ping(payload))) => {
                    // Best-effort pong reply; ignore failures here — the
                    // next next_message call will surface the underlying
                    // error if the stream is broken.
                    let _ = stream.send(Message::Pong(payload)).await;
                    continue;
                }
                Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {
                    continue;
                }
                Some(Ok(Message::Close(reason))) => {
                    let msg = match reason {
                        Some(frame) => format!("close: {} {}", frame.code, frame.reason),
                        None => "close: no reason".to_string(),
                    };
                    return Err(MarketDataError::websocket(&self.source, msg));
                }
                Some(Err(e)) => {
                    return Err(MarketDataError::websocket(&self.source, e));
                }
                None => {
                    return Err(MarketDataError::websocket(&self.source, "stream ended"));
                }
            }
        }
    }

    /// Drop the current connection and reopen it under the documented
    /// backoff schedule. On every disconnect the adapter:
    ///
    /// * increments `hedge_websocket_drops_total{source}`,
    /// * publishes a `md.connection.<source>` JSON event with
    ///   `status: "disconnected"`,
    /// * sleeps for [`reconnect_delay_for`] of the current attempt,
    /// * dials the WebSocket URL again,
    /// * publishes `md.connection.<source>` with
    ///   `status: "reconnected"` on success.
    #[instrument(level = "info", skip(self), fields(source = %self.source, attempt = self.attempts))]
    pub async fn reconnect(&mut self) -> Result<(), MarketDataError> {
        // 1. Drop the existing stream and increment the metric.
        let prior = self.stream.take();
        if let Some(mut s) = prior {
            let _ = s.close(None).await;
        }
        metrics()
            .websocket_drops_total
            .with_label_values(&[self.source.as_str()])
            .inc();

        // 2. Emit the disconnected event.
        let disconnected = ConnectionEvent {
            source: self.source.clone(),
            status: ConnectionStatus::Disconnected,
            reason: Some("transport drop".to_string()),
            attempt: self.attempts,
            at: Utc::now().to_rfc3339(),
        };
        if let Err(err) = self.connection_publisher.publish(&disconnected).await {
            tracing::warn!(error = %err, "publish md.connection disconnected failed");
        }

        // 3. Wait the backoff for this attempt. The adapter is recovery
        //    code, not steady-state polling — exempt from the
        //    no-polling-loops rule per `docs/hot-path-purity.md`.
        let delay = reconnect_delay_for(self.attempts);
        tokio::time::sleep(delay).await; // hedge-allow: polling-loop

        // 4. Try to dial.
        let attempt_for_event = self.attempts;
        self.attempts = self.attempts.saturating_add(1);
        let (stream, _) = connect_async(&self.url)
            .await
            .map_err(|e| MarketDataError::websocket(&self.source, e))?;
        self.stream = Some(stream);

        // 5. Successful: emit reconnected event and reset attempts.
        let reconnected = ConnectionEvent {
            source: self.source.clone(),
            status: ConnectionStatus::Reconnected,
            reason: None,
            attempt: attempt_for_event,
            at: Utc::now().to_rfc3339(),
        };
        if let Err(err) = self.connection_publisher.publish(&reconnected).await {
            tracing::warn!(error = %err, "publish md.connection reconnected failed");
        }
        self.attempts = 0;
        Ok(())
    }
}

impl<P: MarketDataProtocol> WsAdapter<P> for LiveWsAdapter<P> {
    async fn next_message(&mut self) -> Result<RawTick, MarketDataError> {
        Self::next_message(self).await
    }

    async fn reconnect(&mut self) -> Result<(), MarketDataError> {
        Self::reconnect(self).await
    }
}

/// Build the JSON payload published when the adapter loses its upstream
/// connection. Exposed publicly so the supervisor and integration tests
/// can construct identical events without holding a live `LiveWsAdapter`.
pub fn build_disconnected_event(
    source: impl Into<String>,
    attempt: u32,
    reason: impl Into<String>,
) -> ConnectionEvent {
    ConnectionEvent {
        source: source.into(),
        status: ConnectionStatus::Disconnected,
        reason: Some(reason.into()),
        attempt,
        at: Utc::now().to_rfc3339(),
    }
}

/// Build the JSON payload published after a successful reconnection.
pub fn build_reconnected_event(source: impl Into<String>, attempt: u32) -> ConnectionEvent {
    ConnectionEvent {
        source: source.into(),
        status: ConnectionStatus::Reconnected,
        reason: None,
        attempt,
        at: Utc::now().to_rfc3339(),
    }
}

/// Helper exposed for tests: encode a [`ConnectionEvent`] through the JSON
/// codec the adapter uses on the wire. The function never panics for
/// well-formed events.
#[doc(hidden)]
pub fn encode_connection_event(event: &ConnectionEvent) -> Bytes {
    let codec: JsonCodec<ConnectionEvent> = JsonCodec::new();
    use hedge_bus::Codec;
    codec.encode(event).expect("connection event serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_first_nine_attempts_match_table() {
        let expected = [100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600];
        for (i, ms) in expected.iter().enumerate() {
            assert_eq!(
                reconnect_delay_for(i as u32),
                Duration::from_millis(*ms),
                "attempt {} should have delay {} ms",
                i,
                ms
            );
        }
    }

    #[test]
    fn reconnect_backoff_caps_at_30_seconds() {
        for attempt in 9u32..=20 {
            assert_eq!(
                reconnect_delay_for(attempt),
                Duration::from_millis(RECONNECT_CAP_MS),
                "attempt {} should cap at 30s",
                attempt
            );
        }
    }

    #[test]
    fn connection_event_serializes_with_documented_fields() {
        let evt = ConnectionEvent {
            source: "nse_l1".into(),
            status: ConnectionStatus::Disconnected,
            reason: Some("transport drop".into()),
            attempt: 3,
            at: "2025-01-01T00:00:00+00:00".into(),
        };
        let payload = encode_connection_event(&evt);
        let s = std::str::from_utf8(&payload).unwrap();
        assert!(s.contains("\"source\":\"nse_l1\""), "{}", s);
        assert!(s.contains("\"status\":\"disconnected\""), "{}", s);
        assert!(s.contains("\"attempt\":3"), "{}", s);
    }

    #[test]
    fn connection_event_round_trips_through_json_codec() {
        let evt = ConnectionEvent {
            source: "bse_l2".into(),
            status: ConnectionStatus::Reconnected,
            reason: None,
            attempt: 0,
            at: "2025-01-01T00:00:00+00:00".into(),
        };
        use hedge_bus::Codec;
        let codec: JsonCodec<ConnectionEvent> = JsonCodec::new();
        let bytes = codec.encode(&evt).unwrap();
        let decoded = codec.decode(&bytes).unwrap();
        assert_eq!(decoded, evt);
    }

    #[test]
    fn build_disconnected_event_uses_disconnected_status_and_reason() {
        let evt = build_disconnected_event("nse_l1", 2, "transport drop");
        assert_eq!(evt.source, "nse_l1");
        assert_eq!(evt.status, ConnectionStatus::Disconnected);
        assert_eq!(evt.reason.as_deref(), Some("transport drop"));
        assert_eq!(evt.attempt, 2);
        assert!(!evt.at.is_empty(), "rfc3339 timestamp must be populated");
    }

    #[test]
    fn build_reconnected_event_clears_reason() {
        let evt = build_reconnected_event("bse_l2", 5);
        assert_eq!(evt.source, "bse_l2");
        assert_eq!(evt.status, ConnectionStatus::Reconnected);
        assert!(evt.reason.is_none());
        assert_eq!(evt.attempt, 5);
    }

    /// Property: the reconnect schedule strictly doubles between adjacent
    /// table entries until the cap is reached. This test demonstrates the
    /// schedule progression a mocked-connector reconnect test would
    /// observe attempt-by-attempt.
    #[test]
    fn reconnect_backoff_doubles_then_caps() {
        for i in 0..(RECONNECT_BACKOFF_MS.len() - 1) {
            let cur = reconnect_delay_for(i as u32).as_millis();
            let next = reconnect_delay_for((i + 1) as u32).as_millis();
            assert_eq!(
                next,
                cur * 2,
                "attempt {} -> {}: {} ms -> {} ms (expected doubling)",
                i,
                i + 1,
                cur,
                next,
            );
        }
        // Final transition: last table entry to cap.
        let last_table = *RECONNECT_BACKOFF_MS.last().unwrap();
        assert!(last_table < RECONNECT_CAP_MS);
        assert_eq!(
            reconnect_delay_for(RECONNECT_BACKOFF_MS.len() as u32).as_millis() as u64,
            RECONNECT_CAP_MS,
        );
    }

    /// Compile-time check: [`LiveWsAdapter`] implements [`WsAdapter`] for
    /// every concrete placeholder protocol parser we ship.
    #[test]
    fn live_ws_adapter_implements_ws_adapter_for_every_placeholder() {
        fn assert_impl<P: MarketDataProtocol, A: WsAdapter<P>>() {}
        assert_impl::<
            crate::protocol::NseProtocolPlaceholder,
            LiveWsAdapter<crate::protocol::NseProtocolPlaceholder>,
        >();
        assert_impl::<
            crate::protocol::BseProtocolPlaceholder,
            LiveWsAdapter<crate::protocol::BseProtocolPlaceholder>,
        >();
        assert_impl::<
            crate::protocol::OptionsChainProtocolPlaceholder,
            LiveWsAdapter<crate::protocol::OptionsChainProtocolPlaceholder>,
        >();
    }
}
