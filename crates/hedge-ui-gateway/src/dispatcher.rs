//! Per-connection inbound/outbound dispatcher.
//!
//! [`Dispatcher`] is the in-process glue between
//!
//! * the **client → server** stream of [`ClientMsg`] frames decoded from
//!   the WebSocket, and
//! * the **server → client** stream of [`ServerMsg`] frames the gateway
//!   loop will encode and send back, plus
//! * the **NATS event** stream (one classified message per delivered
//!   subject) the gateway loop fans out to every connection.
//!
//! Decoupling the dispatcher from the WebSocket transport lets us unit
//! test the entire protocol surface — subscribe / unsubscribe /
//! intent / event / mode — without standing up a real `tokio_tungstenite`
//! socket.
//!
//! ### Threading model
//!
//! `Dispatcher` is owned by exactly one connection. The shared state
//! (`SignalsJoiner`, `AlertBuffer`, `VolatilityTracker`,
//! `IntentPublisher`) is held behind `Arc` and cloned per connection, so
//! many concurrent connections share the same join state and the same
//! NATS publishing surface.

use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, instrument, warn};

use crate::alerts::{severity_for_subject, AlertBuffer, UiAlert};
use crate::channels::classify_subject;
use crate::intents::{publish_intent, IntentError, IntentPublisher};
use crate::protocol::{Channel, ClientMsg, ErrorCode, IntentKind, ServerMsg};
use crate::signals_join::{JoinOutcome, SignalsJoiner};
use crate::subscriptions::Subscriptions;
use crate::volatility::VolatilityTracker;

/// One classified NATS event from the gateway-wide subscription pool.
///
/// `subject` is the original NATS subject string (e.g. `md.tick.42`).
/// `topic_suffix` is the last segment, used by the per-connection topic
/// filter (see [`Subscriptions::accepts`]).
#[derive(Clone, Debug)]
pub struct NatsEvent {
    /// Original NATS subject.
    pub subject: String,
    /// Last segment of the subject (`"42"` for `md.tick.42`). Empty when
    /// the subject has no parameter segment.
    pub topic_suffix: String,
    /// JSON payload — the gateway converts FlatBuffers payloads upstream
    /// before handing them to the dispatcher.
    pub payload: Value,
    /// Wall-clock nanoseconds at the moment the gateway received the
    /// event from NATS. Used by the alerts buffer for ordering.
    pub ts_ns: u128,
}

/// Shared, connection-independent state used by every [`Dispatcher`].
pub struct DispatcherState {
    /// `sig.emitted` × `ai.rank.*` joiner.
    pub signals: Arc<SignalsJoiner>,
    /// UI alerts bounded buffer.
    pub alerts: Arc<AlertBuffer>,
    /// High-volatility presentation mode tracker.
    pub volatility: Arc<VolatilityTracker>,
    /// Trader-intent publisher (NATS in production, recording in tests).
    pub intents: Arc<dyn IntentPublisher>,
}

/// Per-connection dispatcher.
///
/// Construction is cheap — it only allocates the per-connection
/// [`Subscriptions`] map. Every other surface is `Arc`-shared across the
/// whole process.
pub struct Dispatcher {
    state: Arc<DispatcherState>,
    subs: Subscriptions,
}

impl Dispatcher {
    /// Construct a dispatcher with the given shared state.
    pub fn new(state: Arc<DispatcherState>) -> Self {
        Self { state, subs: Subscriptions::new() }
    }

    /// Borrow the per-connection subscription map. Used by tests.
    pub fn subscriptions(&self) -> &Subscriptions {
        &self.subs
    }

    /// Apply one inbound [`ClientMsg`] and produce zero or more
    /// [`ServerMsg`] replies. Network sending is the caller's job.
    #[instrument(level = "debug", skip(self, msg), fields(msg_kind = msg_kind(&msg)))]
    pub async fn handle_client(&self, msg: ClientMsg) -> Vec<ServerMsg> {
        match msg {
            ClientMsg::Subscribe { request_id, channel, topics } => {
                self.subs.subscribe(channel, &topics);
                vec![ServerMsg::Ack { request_id, channel }]
            }
            ClientMsg::Unsubscribe { request_id, channel } => {
                self.subs.unsubscribe(channel);
                vec![ServerMsg::Ack { request_id, channel }]
            }
            ClientMsg::Intent { request_id, kind, payload } => {
                self.handle_intent(request_id, kind, payload).await
            }
            ClientMsg::Ping { request_id } => vec![ServerMsg::Pong { request_id }],
        }
    }

    async fn handle_intent(
        &self,
        request_id: Option<String>,
        kind: IntentKind,
        payload: Value,
    ) -> Vec<ServerMsg> {
        // Authority Hierarchy: `intent` is only valid on /control. The
        // gateway requires the client to have subscribed to /control
        // before publishing intents — this is a defence-in-depth check
        // on top of the NATS ACL on `trader.*`.
        if !self.subs.is_subscribed(Channel::Control) {
            return vec![ServerMsg::Error {
                code: ErrorCode::IntentOnNonControl,
                message: "client must subscribe to /control before publishing intents".into(),
                request_id,
            }];
        }
        match publish_intent(&*self.state.intents, kind, &payload).await {
            Ok(()) => vec![ServerMsg::Ack {
                request_id,
                channel: Channel::Control,
            }],
            Err(IntentError::InvalidPayload { reason, .. }) => vec![ServerMsg::Error {
                code: ErrorCode::InvalidIntent,
                message: reason,
                request_id,
            }],
            Err(IntentError::PublishFailed { subject, message }) => {
                warn!(subject = %subject, error = %message, "trader intent NATS publish failed");
                vec![ServerMsg::Error {
                    code: ErrorCode::Internal,
                    message: format!("nats publish failed: {}", message),
                    request_id,
                }]
            }
        }
    }

    /// Apply one inbound NATS event and produce zero or more
    /// [`ServerMsg`] frames to send to this connection.
    ///
    /// The classification:
    ///
    /// 1. Map the subject to its UI channel(s) via [`classify_subject`].
    /// 2. For each matched channel, decide whether *this* connection has
    ///    a matching subscription (and matching topic filter).
    /// 3. Apply per-channel transforms:
    ///    * `Signals` — feed the signals joiner; emit only on join or
    ///      flush.
    ///    * `Alerts` — never reached directly; alerts are produced by
    ///      [`Self::ingest_for_alerts`] from the same NATS event.
    ///    * `Market` (volatility breadth) — feed the volatility
    ///      tracker; emit a `Mode` transition on flip.
    /// 4. Emit `Event` frames for the remaining matched channels.
    pub fn handle_nats_event(&self, ev: &NatsEvent) -> Vec<ServerMsg> {
        let mut out = Vec::with_capacity(2);

        // 1. High-volatility breadth update — applies to every connection
        //    on /market regardless of subscription topics.
        if ev.subject == "md.breadth.volatility" {
            if let Some(flipped) = self.state.volatility.observe(&ev.payload) {
                if self.subs.is_subscribed(Channel::Market) {
                    out.push(ServerMsg::Mode { high_volatility: flipped });
                }
            }
        }

        // 2. Per-channel routing.
        let m = classify_subject(&ev.subject);
        for ch in m.iter() {
            if !self.subs.is_subscribed(ch) {
                continue;
            }

            // Special-case: /signals goes through the joiner.
            if ch == Channel::Signals {
                if let Some(payloads) = self.feed_signals_join(ev) {
                    for p in payloads {
                        out.push(ServerMsg::Event {
                            channel: Channel::Signals,
                            payload: p,
                        });
                    }
                }
                continue;
            }

            // Topic filter (for channels that carry per-symbol streams).
            if !self.subs.accepts(ch, &ev.topic_suffix) {
                continue;
            }

            out.push(ServerMsg::Event {
                channel: ch,
                payload: ev.payload.clone(),
            });
        }

        out
    }

    /// Feed a NATS event into the signals join state. Returns the
    /// joined payload(s) ready for emission, or `None` when the event
    /// is buffered and waiting for its other half.
    fn feed_signals_join(&self, ev: &NatsEvent) -> Option<Vec<Value>> {
        // Subjects routed to /signals are: `sig.emitted` and `ai.rank.<cid>`.
        if ev.subject == "sig.emitted" {
            // The Signal_Engine embeds the correlation id in the JSON
            // payload as `correlation_id` (canonical hex form). Default
            // to the raw payload itself if the field is absent — the
            // joiner will still buffer with an empty-string key, which
            // is harmless because every produced subject under
            // `ai.rank.*` has a non-empty hex segment.
            let cid = ev
                .payload
                .get("correlation_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            let outcome = self.state.signals.feed_signal(&cid, ev.payload.clone());
            wrap_outcome(outcome)
        } else if ev.subject.starts_with("ai.rank.") {
            // The hex correlation id is the last segment of the subject.
            let cid = &ev.topic_suffix;
            let outcome = self.state.signals.feed_rank(cid, ev.payload.clone());
            wrap_outcome(outcome)
        } else {
            None
        }
    }

    /// Update the alert buffer if `ev` is alert-shaped, then return any
    /// alerts that should be emitted on `/alerts` to this connection.
    pub fn ingest_for_alerts(&self, ev: &NatsEvent) -> Vec<ServerMsg> {
        let Some(severity) = severity_for_subject(&ev.subject) else {
            return Vec::new();
        };
        let alert = UiAlert {
            severity,
            source: ev.subject.clone(),
            ts_ns: ev.ts_ns,
            payload: ev.payload.clone(),
        };
        self.state.alerts.push(alert);

        if !self.subs.is_subscribed(Channel::Alerts) {
            return Vec::new();
        }
        let ordered = self.state.alerts.ordered();
        let payload = serde_json::to_value(ordered).unwrap_or(Value::Null);
        vec![ServerMsg::Event {
            channel: Channel::Alerts,
            payload,
        }]
    }

    /// Produce signal-only flushes for any joined signals whose TTL has
    /// elapsed without a matching ranking. Called on a periodic tick by
    /// the gateway.
    pub fn drain_signal_flushes(&self) -> Vec<ServerMsg> {
        if !self.subs.is_subscribed(Channel::Signals) {
            return Vec::new();
        }
        let flushed = self.state.signals.flush_expired();
        flushed
            .into_iter()
            .map(|payload| ServerMsg::Event {
                channel: Channel::Signals,
                payload,
            })
            .collect()
    }
}

fn wrap_outcome(outcome: JoinOutcome) -> Option<Vec<Value>> {
    match outcome {
        JoinOutcome::Joined(payload) | JoinOutcome::SignalOnly(payload) => Some(vec![payload]),
        JoinOutcome::Pending { .. } => {
            debug!("signals join pending");
            None
        }
    }
}

fn msg_kind(m: &ClientMsg) -> &'static str {
    match m {
        ClientMsg::Subscribe { .. } => "subscribe",
        ClientMsg::Unsubscribe { .. } => "unsubscribe",
        ClientMsg::Intent { .. } => "intent",
        ClientMsg::Ping { .. } => "ping",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intents::RecordingPublisher;
    use serde_json::json;
    use std::time::Duration;

    fn dispatcher_with(intents: Arc<dyn IntentPublisher>) -> Dispatcher {
        let state = Arc::new(DispatcherState {
            signals: Arc::new(SignalsJoiner::new(
                Duration::from_secs(2),
                256,
                Arc::new(crate::signals_join::AiShadowFilter::default()),
            )),
            alerts: Arc::new(AlertBuffer::new(64)),
            volatility: Arc::new(VolatilityTracker::new(0.05)),
            intents,
        });
        Dispatcher::new(state)
    }

    #[tokio::test]
    async fn subscribe_acks_with_request_id() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let out = d
            .handle_client(ClientMsg::Subscribe {
                request_id: Some("r1".into()),
                channel: Channel::Risk,
                topics: vec![],
            })
            .await;
        assert_eq!(out.len(), 1);
        match &out[0] {
            ServerMsg::Ack { request_id, channel } => {
                assert_eq!(request_id.as_deref(), Some("r1"));
                assert_eq!(*channel, Channel::Risk);
            }
            other => panic!("expected Ack, got {:?}", other),
        }
        assert!(d.subscriptions().is_subscribed(Channel::Risk));
    }

    #[tokio::test]
    async fn intent_on_non_control_channel_is_rejected() {
        let pub_ = Arc::new(RecordingPublisher::new());
        let d = dispatcher_with(pub_.clone());
        let out = d
            .handle_client(ClientMsg::Intent {
                request_id: Some("r1".into()),
                kind: IntentKind::Killswitch,
                payload: json!({"active": true}),
            })
            .await;
        match &out[0] {
            ServerMsg::Error { code, .. } => {
                assert_eq!(*code, ErrorCode::IntentOnNonControl);
            }
            other => panic!("expected Error, got {:?}", other),
        }
        assert!(pub_.published().is_empty());
    }

    #[tokio::test]
    async fn intent_on_control_channel_publishes_to_nats_subject() {
        let pub_ = Arc::new(RecordingPublisher::new());
        let d = dispatcher_with(pub_.clone());
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Control,
                topics: vec![],
            })
            .await;
        let out = d
            .handle_client(ClientMsg::Intent {
                request_id: Some("r1".into()),
                kind: IntentKind::Killswitch,
                payload: json!({"active": true}),
            })
            .await;
        match &out[0] {
            ServerMsg::Ack { request_id, channel } => {
                assert_eq!(request_id.as_deref(), Some("r1"));
                assert_eq!(*channel, Channel::Control);
            }
            other => panic!("expected Ack, got {:?}", other),
        }
        let seen = pub_.published();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "trader.intent.killswitch");
    }

    #[tokio::test]
    async fn invalid_intent_payload_returns_invalid_intent_error() {
        let pub_ = Arc::new(RecordingPublisher::new());
        let d = dispatcher_with(pub_.clone());
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Control,
                topics: vec![],
            })
            .await;
        let out = d
            .handle_client(ClientMsg::Intent {
                request_id: None,
                kind: IntentKind::Order,
                payload: json!({"symbol": "X"}),
            })
            .await;
        match &out[0] {
            ServerMsg::Error { code, .. } => assert_eq!(*code, ErrorCode::InvalidIntent),
            other => panic!("expected Error, got {:?}", other),
        }
        assert!(pub_.published().is_empty());
    }

    #[tokio::test]
    async fn ping_responds_with_pong_echoing_request_id() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let out = d
            .handle_client(ClientMsg::Ping { request_id: Some("p1".into()) })
            .await;
        match &out[0] {
            ServerMsg::Pong { request_id } => assert_eq!(request_id.as_deref(), Some("p1")),
            other => panic!("expected Pong, got {:?}", other),
        }
    }

    fn ev(subject: &str, payload: Value) -> NatsEvent {
        let topic_suffix = subject.rsplit('.').next().unwrap_or("").to_owned();
        NatsEvent {
            subject: subject.to_owned(),
            topic_suffix,
            payload,
            ts_ns: 0,
        }
    }

    #[tokio::test]
    async fn nats_market_tick_routes_to_market_subscribers_only() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Market,
                topics: vec![],
            })
            .await;
        let out = d.handle_nats_event(&ev("md.tick.42", json!({"ltp": 100})));
        assert_eq!(out.len(), 1);
        match &out[0] {
            ServerMsg::Event { channel, .. } => assert_eq!(*channel, Channel::Market),
            other => panic!("expected Event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn nats_event_dropped_when_topic_filter_does_not_match() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Market,
                topics: vec!["7".into()],
            })
            .await;
        let out = d.handle_nats_event(&ev("md.tick.42", json!({})));
        assert!(out.is_empty(), "topic filter must drop non-matching subjects");
        let out = d.handle_nats_event(&ev("md.tick.7", json!({})));
        assert_eq!(out.len(), 1);
    }

    #[tokio::test]
    async fn nats_event_for_unsubscribed_channel_is_dropped() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        // No subscriptions.
        let out = d.handle_nats_event(&ev("md.tick.42", json!({})));
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn signals_join_emits_only_when_both_halves_present() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Signals,
                topics: vec![],
            })
            .await;
        // sig.emitted first — pending
        let out = d.handle_nats_event(&ev(
            "sig.emitted",
            json!({"correlation_id": "ABC", "strategy": "obr"}),
        ));
        assert!(out.is_empty(), "expected pending join");

        // ai.rank.ABC — completes the join
        let out = d.handle_nats_event(&ev(
            "ai.rank.ABC",
            json!({"source": "ranking", "score": 0.7}),
        ));
        assert_eq!(out.len(), 1);
        match &out[0] {
            ServerMsg::Event { channel, payload } => {
                assert_eq!(*channel, Channel::Signals);
                assert_eq!(payload["signal"]["strategy"], "obr");
                assert_eq!(payload["ranks"][0]["score"], 0.7);
            }
            other => panic!("expected Event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn shadowed_rank_never_appears_on_signals_channel() {
        let shadow = Arc::new(crate::signals_join::AiShadowFilter::from_iter(["ranking"]));
        let state = Arc::new(DispatcherState {
            signals: Arc::new(SignalsJoiner::new(
                Duration::from_millis(15),
                256,
                shadow,
            )),
            alerts: Arc::new(AlertBuffer::new(64)),
            volatility: Arc::new(VolatilityTracker::new(0.05)),
            intents: Arc::new(RecordingPublisher::new()),
        });
        let d = Dispatcher::new(state);
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Signals,
                topics: vec![],
            })
            .await;

        // Feed a shadowed rank — never delivered as a signal half
        let out = d.handle_nats_event(&ev(
            "ai.rank.ABC",
            json!({"source": "ranking", "score": 0.99}),
        ));
        assert!(out.is_empty());

        // Now the signal — pending, the only rank was shadowed.
        let out = d.handle_nats_event(&ev(
            "sig.emitted",
            json!({"correlation_id": "ABC", "strategy": "obr"}),
        ));
        assert!(out.is_empty(), "no rank means no immediate join");

        // After TTL → flush emits signal-only with shadowed_sources annotation
        std::thread::sleep(Duration::from_millis(25));
        let flushes = d.drain_signal_flushes();
        assert_eq!(flushes.len(), 1);
        match &flushes[0] {
            ServerMsg::Event { channel, payload } => {
                assert_eq!(*channel, Channel::Signals);
                assert_eq!(payload["ranks"], json!([]));
                assert_eq!(payload["shadowed_sources"], json!(["ranking"]));
            }
            other => panic!("expected Event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn high_volatility_breadth_emits_mode_flip_then_quiet() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Market,
                topics: vec![],
            })
            .await;
        // 0.04 < 0.05 → no flip.
        let out = d.handle_nats_event(&ev(
            "md.breadth.volatility",
            json!({"value": 0.04}),
        ));
        // /market is also matched as an event — but `md.breadth.volatility`
        // routes to /market the same way as a tick. So we expect
        // exactly one Event (the breadth value forwarded).
        assert!(out.iter().all(|m| !matches!(m, ServerMsg::Mode { .. })));

        // 0.10 > 0.05 → flips on. Both Mode and Event emitted.
        let out = d.handle_nats_event(&ev(
            "md.breadth.volatility",
            json!({"value": 0.10}),
        ));
        let modes: Vec<_> = out.iter().filter_map(|m| match m {
            ServerMsg::Mode { high_volatility } => Some(*high_volatility),
            _ => None,
        }).collect();
        assert_eq!(modes, vec![true]);

        // 0.12 still high → no flip.
        let out = d.handle_nats_event(&ev(
            "md.breadth.volatility",
            json!({"value": 0.12}),
        ));
        assert!(out.iter().all(|m| !matches!(m, ServerMsg::Mode { .. })));

        // Drop below → flips off.
        let out = d.handle_nats_event(&ev(
            "md.breadth.volatility",
            json!({"value": 0.01}),
        ));
        let modes: Vec<_> = out.iter().filter_map(|m| match m {
            ServerMsg::Mode { high_volatility } => Some(*high_volatility),
            _ => None,
        }).collect();
        assert_eq!(modes, vec![false]);
    }

    #[tokio::test]
    async fn alerts_severity_sort_critical_above_warning() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Alerts,
                topics: vec![],
            })
            .await;
        let _ = d.ingest_for_alerts(&NatsEvent {
            subject: "exec.broker.failover".into(),
            topic_suffix: "".into(),
            payload: json!({}),
            ts_ns: 1,
        });
        let out = d.ingest_for_alerts(&NatsEvent {
            subject: "risk.killswitch.activated".into(),
            topic_suffix: "".into(),
            payload: json!({}),
            ts_ns: 2,
        });
        // Last call returns the *current* ordered snapshot.
        match &out[0] {
            ServerMsg::Event { channel, payload } => {
                assert_eq!(*channel, Channel::Alerts);
                let xs = payload.as_array().unwrap();
                assert_eq!(xs[0]["severity"], "critical");
                assert_eq!(xs[1]["severity"], "warning");
            }
            other => panic!("expected Event, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn unsubscribe_stops_event_delivery() {
        let d = dispatcher_with(Arc::new(RecordingPublisher::new()));
        let _ = d
            .handle_client(ClientMsg::Subscribe {
                request_id: None,
                channel: Channel::Risk,
                topics: vec![],
            })
            .await;
        assert!(d.subscriptions().is_subscribed(Channel::Risk));
        let _ = d
            .handle_client(ClientMsg::Unsubscribe {
                request_id: None,
                channel: Channel::Risk,
            })
            .await;
        assert!(!d.subscriptions().is_subscribed(Channel::Risk));
        let out = d.handle_nats_event(&ev("risk.decision.approved", json!({})));
        assert!(out.is_empty());
    }
}
