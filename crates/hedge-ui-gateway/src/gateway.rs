//! WebSocket gateway server.
//!
//! Accepts inbound TCP connections, upgrades each to a WebSocket, and
//! drives a [`Dispatcher`] for that connection. The gateway-wide NATS
//! fan-out is owned by the binary entry point in `main.rs`; this module
//! only knows about per-connection lifecycle.
//!
//! ### Connection lifecycle
//!
//! 1. Accept TCP, upgrade to WebSocket via `tokio_tungstenite`.
//! 2. Spawn a per-connection [`Dispatcher`].
//! 3. On every inbound text frame, decode as [`ClientMsg`] and call
//!    [`Dispatcher::handle_client`]. Encode the resulting
//!    [`ServerMsg`] frames and send them back.
//! 4. Subscribe the connection to the gateway-wide NATS event channel
//!    (a `tokio::sync::broadcast::Receiver<NatsEvent>`) and forward
//!    matching events as text frames.
//! 5. On disconnect, the per-connection state is dropped and any
//!    half-formed signal joins are GC'd by the next periodic flush.
//!
//! ### Test surface
//!
//! [`run_session`] is the connection driver split out so tests can run
//! it against an in-memory pair of `tokio::io::duplex` streams without
//! standing up a real TCP listener.

use std::sync::Arc;

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, error, info, instrument, warn};

use crate::dispatcher::{Dispatcher, DispatcherState, NatsEvent};
use crate::protocol::{ClientMsg, ErrorCode, ServerMsg};

/// Top-level gateway configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Address to bind, e.g. `"127.0.0.1:8088"`.
    pub bind: String,
    /// Capacity of the gateway-wide NATS event broadcast channel.
    pub broadcast_capacity: usize,
    /// Periodic interval at which signal-only flushes are produced.
    pub signal_flush_interval: std::time::Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8088".into(),
            broadcast_capacity: 4096,
            signal_flush_interval: std::time::Duration::from_secs(2),
        }
    }
}

/// Gateway server entry point.
///
/// `state` is the shared [`DispatcherState`] (signals joiner, alerts
/// buffer, volatility tracker, intent publisher). `events_rx_factory` is
/// a closure that produces a fresh subscriber to the gateway-wide NATS
/// event broadcast — the gateway calls it once per accepted connection.
///
/// This function returns an `Err` only on bind failure. Per-connection
/// errors are logged and the connection is dropped without aborting the
/// listener.
#[instrument(level = "info", skip(state, events_rx_factory), fields(bind = %cfg.bind))]
pub async fn serve<F>(
    cfg: GatewayConfig,
    state: Arc<DispatcherState>,
    events_rx_factory: F,
) -> Result<()>
where
    F: Fn() -> broadcast::Receiver<NatsEvent> + Send + Sync + 'static,
{
    let listener = TcpListener::bind(&cfg.bind)
        .await
        .with_context(|| format!("failed to bind to {}", &cfg.bind))?;
    info!("ui-gateway listening on {}", cfg.bind);

    let factory = Arc::new(events_rx_factory);

    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "tcp accept failed; continuing");
                continue;
            }
        };
        let state = state.clone();
        let factory = factory.clone();
        let flush_interval = cfg.signal_flush_interval;

        tokio::spawn(async move {
            let events_rx = (factory)();
            if let Err(e) = run_tcp_session(sock, state, events_rx, flush_interval).await {
                warn!(peer = %peer, error = %e, "per-connection session ended with error");
            } else {
                debug!(peer = %peer, "per-connection session ended cleanly");
            }
        });
    }
}

/// Drive one accepted TCP connection through its WebSocket lifecycle.
#[instrument(level = "debug", skip_all)]
async fn run_tcp_session(
    sock: TcpStream,
    state: Arc<DispatcherState>,
    events_rx: broadcast::Receiver<NatsEvent>,
    flush_interval: std::time::Duration,
) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(sock)
        .await
        .context("websocket handshake failed")?;
    run_session(ws, state, events_rx, flush_interval).await
}

/// Drive an already-accepted WebSocket through its lifecycle.
///
/// Split out from [`run_tcp_session`] so tests can construct a
/// `WebSocketStream` against `tokio::io::duplex` and exercise the full
/// protocol surface without a real TCP socket.
pub async fn run_session<S>(
    ws: tokio_tungstenite::WebSocketStream<S>,
    state: Arc<DispatcherState>,
    mut events_rx: broadcast::Receiver<NatsEvent>,
    flush_interval: std::time::Duration,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let dispatcher = Dispatcher::new(state);
    let (mut sink, mut stream) = ws.split();
    let mut flush = tokio::time::interval(flush_interval);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // ---- Inbound client frame ----
            maybe_msg = stream.next() => {
                let Some(msg) = maybe_msg else { break };
                let msg = msg.context("websocket recv failed")?;
                match msg {
                    Message::Text(text) => {
                        match serde_json::from_str::<ClientMsg>(&text) {
                            Ok(client_msg) => {
                                let replies = dispatcher.handle_client(client_msg).await;
                                send_all(&mut sink, replies).await?;
                            }
                            Err(e) => {
                                let reply = ServerMsg::Error {
                                    code: ErrorCode::BadFrame,
                                    message: format!("malformed client message: {}", e),
                                    request_id: None,
                                };
                                send_all(&mut sink, vec![reply]).await?;
                            }
                        }
                    }
                    Message::Binary(_) => {
                        let reply = ServerMsg::Error {
                            code: ErrorCode::BadFrame,
                            message: "binary frames are not supported".into(),
                            request_id: None,
                        };
                        send_all(&mut sink, vec![reply]).await?;
                    }
                    Message::Ping(p) => {
                        sink.send(Message::Pong(p)).await.context("ws pong failed")?;
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }

            // ---- Outbound NATS event ----
            recv = events_rx.recv() => {
                match recv {
                    Ok(ev) => {
                        let mut replies = dispatcher.handle_nats_event(&ev);
                        replies.extend(dispatcher.ingest_for_alerts(&ev));
                        send_all(&mut sink, replies).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "broadcast lagged; some events dropped");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            // ---- Periodic signal-only flush ----
            _ = flush.tick() => {
                let replies = dispatcher.drain_signal_flushes();
                send_all(&mut sink, replies).await?;
            }
        }
    }
    Ok(())
}

async fn send_all<S>(
    sink: &mut futures::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>,
    msgs: Vec<ServerMsg>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    for m in msgs {
        let json = serde_json::to_string(&m).context("server msg encode failed")?;
        sink.send(Message::Text(json))
            .await
            .context("websocket send failed")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::AlertBuffer;
    use crate::intents::RecordingPublisher;
    use crate::protocol::{Channel, IntentKind};
    use crate::signals_join::{AiShadowFilter, SignalsJoiner};
    use crate::volatility::VolatilityTracker;
    use serde_json::json;
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::protocol::Message;

    fn make_state() -> (Arc<DispatcherState>, Arc<RecordingPublisher>) {
        let intents = Arc::new(RecordingPublisher::new());
        let state = Arc::new(DispatcherState {
            signals: Arc::new(SignalsJoiner::new(
                Duration::from_secs(2),
                256,
                Arc::new(AiShadowFilter::default()),
            )),
            alerts: Arc::new(AlertBuffer::new(64)),
            volatility: Arc::new(VolatilityTracker::new(0.05)),
            intents: intents.clone(),
        });
        (state, intents)
    }

    /// End-to-end test through an in-memory `tokio::io::duplex` pair.
    /// Drives the gateway loop on one side and a test client on the
    /// other, exercising subscribe/event/ack/intent/error paths.
    #[tokio::test]
    async fn end_to_end_subscribe_event_intent() {
        let (state, intents) = make_state();
        let (tx, _) = broadcast::channel::<NatsEvent>(64);
        let server_rx = tx.subscribe();

        let (client_io, server_io) = tokio::io::duplex(8192);

        // Spawn the server side.
        let server_state = state.clone();
        let server_handle = tokio::spawn(async move {
            let server_ws = tokio_tungstenite::accept_async(server_io).await.unwrap();
            run_session(
                server_ws,
                server_state,
                server_rx,
                Duration::from_millis(50),
            )
            .await
        });

        // Drive the client side.
        let (mut client_ws, _resp) = tokio_tungstenite::client_async(
            "ws://localhost/test",
            client_io,
        )
        .await
        .unwrap();

        // 1. Subscribe to /risk
        let sub = serde_json::to_string(&ClientMsg::Subscribe {
            request_id: Some("r1".into()),
            channel: Channel::Risk,
            topics: vec![],
        })
        .unwrap();
        client_ws.send(Message::Text(sub)).await.unwrap();

        // 2. Read the ack.
        let frame = client_ws.next().await.unwrap().unwrap();
        let s = match frame {
            Message::Text(t) => t,
            other => panic!("expected text frame, got {:?}", other),
        };
        let ack: ServerMsg = serde_json::from_str(&s).unwrap();
        assert!(matches!(
            ack,
            ServerMsg::Ack { request_id, channel } if request_id.as_deref() == Some("r1") && channel == Channel::Risk
        ));

        // 3. Publish a NATS event on risk.decision.approved → must arrive.
        tx.send(NatsEvent {
            subject: "risk.decision.approved".into(),
            topic_suffix: "approved".into(),
            payload: json!({"id": 1}),
            ts_ns: 1,
        })
        .unwrap();
        let frame = client_ws.next().await.unwrap().unwrap();
        let s = match frame {
            Message::Text(t) => t,
            other => panic!("expected text, got {:?}", other),
        };
        let ev_msg: ServerMsg = serde_json::from_str(&s).unwrap();
        match ev_msg {
            ServerMsg::Event { channel, payload } => {
                assert_eq!(channel, Channel::Risk);
                assert_eq!(payload["id"], 1);
            }
            other => panic!("expected Event, got {:?}", other),
        }

        // 4. Subscribe to /control then publish an intent.
        let sub_ctrl = serde_json::to_string(&ClientMsg::Subscribe {
            request_id: None,
            channel: Channel::Control,
            topics: vec![],
        })
        .unwrap();
        client_ws.send(Message::Text(sub_ctrl)).await.unwrap();
        let _ = client_ws.next().await.unwrap().unwrap(); // ack
        let intent = serde_json::to_string(&ClientMsg::Intent {
            request_id: Some("i1".into()),
            kind: IntentKind::Killswitch,
            payload: json!({"active": true}),
        })
        .unwrap();
        client_ws.send(Message::Text(intent)).await.unwrap();
        let frame = client_ws.next().await.unwrap().unwrap();
        let s = match frame {
            Message::Text(t) => t,
            other => panic!("expected text, got {:?}", other),
        };
        let ack: ServerMsg = serde_json::from_str(&s).unwrap();
        assert!(matches!(
            ack,
            ServerMsg::Ack { request_id, channel } if request_id.as_deref() == Some("i1") && channel == Channel::Control
        ));
        let seen = intents.published();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "trader.intent.killswitch");

        // 5. Close the client. Server task must end cleanly.
        client_ws.close(None).await.unwrap();
        // Drain to drive close handshake.
        while client_ws.next().await.is_some() {}
        let _ = server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn malformed_frame_returns_bad_frame_error() {
        let (state, _) = make_state();
        let (_tx, server_rx) = broadcast::channel::<NatsEvent>(64);
        let (client_io, server_io) = tokio::io::duplex(4096);

        let server_handle = tokio::spawn(async move {
            let ws = tokio_tungstenite::accept_async(server_io).await.unwrap();
            run_session(ws, state, server_rx, Duration::from_millis(50)).await
        });

        let (mut client_ws, _) =
            tokio_tungstenite::client_async("ws://localhost/test", client_io)
                .await
                .unwrap();
        client_ws
            .send(Message::Text("not-json".into()))
            .await
            .unwrap();
        let frame = client_ws.next().await.unwrap().unwrap();
        let s = match frame {
            Message::Text(t) => t,
            other => panic!("expected text, got {:?}", other),
        };
        let err: ServerMsg = serde_json::from_str(&s).unwrap();
        match err {
            ServerMsg::Error { code, .. } => assert_eq!(code, ErrorCode::BadFrame),
            other => panic!("expected Error, got {:?}", other),
        }

        client_ws.close(None).await.unwrap();
        while client_ws.next().await.is_some() {}
        let _ = server_handle.await.unwrap();
    }
}
