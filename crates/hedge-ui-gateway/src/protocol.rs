//! WebSocket topic-subscription protocol.
//!
//! The `ui-gateway` exposes a *single* WebSocket endpoint with a
//! topic-subscription protocol (design § Data Models § WebSocket Channels).
//! Payloads are JSON for UI ergonomics — the React cockpit consumes them
//! directly without an intermediate FlatBuffers reader.
//!
//! ### Message taxonomy
//!
//! Two directions, each with a small fixed set of message variants:
//!
//! * **Client → Server (`ClientMsg`)**:
//!   - `subscribe { channel, topics? }` — join one of the curated UI
//!     channels (`market`, `signals`, `risk`, ...). The optional `topics`
//!     field allows a per-symbol or per-correlation-id refinement (e.g.
//!     `{ "channel": "market", "topics": ["RELIANCE", "NIFTY"] }`).
//!   - `unsubscribe { channel }` — leave a channel.
//!   - `intent { kind, payload }` — publish a `trader.intent.*` event on
//!     NATS via the Risk_Engine (see [`IntentKind`]). Only valid on the
//!     `/control` channel; the gateway enforces this at the publish step.
//!   - `ping` — application-level liveness probe (the WS layer also has
//!     its own pings; this is the JSON envelope the React client uses).
//!
//! * **Server → Client (`ServerMsg`)**:
//!   - `event { channel, payload }` — every NATS message fanned out to
//!     this WebSocket. `payload` is an opaque JSON value the cockpit
//!     interprets per-channel.
//!   - `ack { request_id?, channel }` — acknowledges a `subscribe`,
//!     `unsubscribe`, or `intent`.
//!   - `error { code, message, request_id? }` — the gateway rejected the
//!     last message (unknown channel, invalid intent, etc.).
//!   - `mode { high_volatility }` — high-volatility presentation toggle
//!     (R20.4). The cockpit increases refresh rate and hides secondary
//!     visual elements while `high_volatility = true`.
//!   - `pong` — reply to a `ping`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One of the curated UI channels exposed by the gateway.
///
/// The wire form is the lowercase channel name without the leading `/`:
/// `"market"`, `"orderflow"`, `"signals"`, `"risk"`, `"exec"`, `"news"`,
/// `"psych"`, `"alerts"`, `"replay"`, `"latency"`, `"control"`.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    /// `md.*` events filtered to subscribed symbols.
    Market,
    /// Orderflow heatmap deltas (R2.4, R20.3).
    Orderflow,
    /// `sig.emitted` joined with `ai.rank.*` by `correlation_id` (R20.3).
    Signals,
    /// `risk.*`, `pos.risk_state`, `pos.update.*` (R20.3).
    Risk,
    /// `exec.*` (R20.3).
    Exec,
    /// `ai.news.impact.*` (R20.3).
    News,
    /// `ai.psych.*` (R20.3).
    Psych,
    /// UI-formatted alerts, severity-sorted (R20.5).
    Alerts,
    /// Replay control plane and frame stream (R20.3, R22.3).
    Replay,
    /// `obs.latency.*` aggregated for the Latency Dashboard (R20.3, R27.4).
    Latency,
    /// Trader → server intents (R20.6, R20.7, R20.8).
    Control,
}

impl Channel {
    /// All channels in declaration order. Used by tests and the router
    /// initialiser to enumerate the curated channel set.
    pub const ALL: [Channel; 11] = [
        Channel::Market,
        Channel::Orderflow,
        Channel::Signals,
        Channel::Risk,
        Channel::Exec,
        Channel::News,
        Channel::Psych,
        Channel::Alerts,
        Channel::Replay,
        Channel::Latency,
        Channel::Control,
    ];

    /// Stable lowercase wire token.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Channel::Market => "market",
            Channel::Orderflow => "orderflow",
            Channel::Signals => "signals",
            Channel::Risk => "risk",
            Channel::Exec => "exec",
            Channel::News => "news",
            Channel::Psych => "psych",
            Channel::Alerts => "alerts",
            Channel::Replay => "replay",
            Channel::Latency => "latency",
            Channel::Control => "control",
        }
    }

    /// Parse a channel from its wire token. Returns `None` for unknown
    /// names.
    pub fn parse(name: &str) -> Option<Channel> {
        Some(match name {
            "market" => Channel::Market,
            "orderflow" => Channel::Orderflow,
            "signals" => Channel::Signals,
            "risk" => Channel::Risk,
            "exec" => Channel::Exec,
            "news" => Channel::News,
            "psych" => Channel::Psych,
            "alerts" => Channel::Alerts,
            "replay" => Channel::Replay,
            "latency" => Channel::Latency,
            "control" => Channel::Control,
            _ => return None,
        })
    }
}

/// Trader-intent kind published on the `/control` channel.
///
/// Wire form is the snake_case token following `trader.intent.*` in the
/// NATS subject namespace.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum IntentKind {
    /// `trader.intent.killswitch` — toggle the kill-switch.
    Killswitch,
    /// `trader.intent.strategy_toggle` — enable/disable a strategy.
    StrategyToggle,
    /// `trader.intent.priority` — change the priority tier of a symbol.
    Priority,
    /// `trader.intent.order` — manual trader order intent.
    Order,
    /// `trader.intent.trading_mode` — switch live vs paper execution.
    TradingMode,
}

impl IntentKind {
    /// Canonical NATS subject for this intent.
    #[inline]
    pub const fn nats_subject(self) -> &'static str {
        match self {
            IntentKind::Killswitch => hedge_bus::TRADER_INTENT_KILLSWITCH,
            IntentKind::StrategyToggle => hedge_bus::TRADER_INTENT_STRATEGY_TOGGLE,
            IntentKind::Priority => hedge_bus::TRADER_INTENT_PRIORITY,
            IntentKind::Order => hedge_bus::TRADER_INTENT_ORDER,
            IntentKind::TradingMode => hedge_bus::TRADER_INTENT_TRADING_MODE,
        }
    }
}

/// Client → Server message variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Join one of the curated UI channels.
    Subscribe {
        /// Optional client-side request id, echoed in the ack/error.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        /// Channel to join.
        channel: Channel,
        /// Optional per-channel filter list (e.g. symbol identifiers for
        /// the `market` channel). Empty / missing means "all topics".
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        topics: Vec<String>,
    },
    /// Leave a channel.
    Unsubscribe {
        /// Optional client-side request id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        /// Channel to leave.
        channel: Channel,
    },
    /// Publish a `trader.intent.*` event. Only valid on the `/control`
    /// channel; the gateway enforces this.
    Intent {
        /// Optional client-side request id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        /// Intent kind (sets the NATS subject).
        kind: IntentKind,
        /// Opaque JSON payload forwarded verbatim to NATS.
        payload: Value,
    },
    /// Application-level liveness probe.
    Ping {
        /// Optional client-side request id.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
}

/// Server → Client message variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// A NATS event fanned out to this connection.
    Event {
        /// Channel this event belongs to.
        channel: Channel,
        /// Optional NATS subject to help routing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<String>,
        /// Optional timestamp.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ns: Option<u64>,
        /// JSON payload (per-channel shape; opaque to the gateway).
        payload: Value,
    },
    /// Acknowledges a `subscribe`/`unsubscribe`/`intent`.
    Ack {
        /// Echo of the client's request id, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        /// Channel the action targeted.
        channel: Channel,
    },
    /// Negative acknowledgement.
    Error {
        /// Stable error code suitable for UI branching.
        code: ErrorCode,
        /// Human-readable explanation.
        message: String,
        /// Echo of the client's request id, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    /// High-volatility presentation toggle (R20.4).
    Mode {
        /// `true` while `md.breadth.volatility > ui.high_vol_threshold`.
        high_volatility: bool,
    },
    /// Reply to a `ping`.
    Pong {
        /// Echo of the client's request id, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
}

/// Stable error codes returned by the gateway.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// JSON could not be parsed into a [`ClientMsg`].
    BadFrame,
    /// Channel name was not in the curated set.
    UnknownChannel,
    /// `intent` was sent on a channel other than `/control`.
    IntentOnNonControl,
    /// Intent payload failed validation (missing fields, wrong types).
    InvalidIntent,
    /// Internal server error (NATS publish failure, etc.).
    Internal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_round_trips_through_wire_token() {
        for ch in Channel::ALL {
            assert_eq!(Channel::parse(ch.as_str()), Some(ch));
        }
        assert_eq!(Channel::parse("not-a-channel"), None);
    }

    #[test]
    fn intent_kind_maps_to_canonical_nats_subjects() {
        assert_eq!(IntentKind::Killswitch.nats_subject(), "trader.intent.killswitch");
        assert_eq!(IntentKind::StrategyToggle.nats_subject(), "trader.intent.strategy_toggle");
        assert_eq!(IntentKind::Priority.nats_subject(), "trader.intent.priority");
        assert_eq!(IntentKind::Order.nats_subject(), "trader.intent.order");
    }

    #[test]
    fn client_subscribe_round_trip() {
        let raw = r#"{"type":"subscribe","channel":"market","topics":["RELIANCE","NIFTY"]}"#;
        let msg: ClientMsg = serde_json::from_str(raw).unwrap();
        match msg {
            ClientMsg::Subscribe { channel, topics, .. } => {
                assert_eq!(channel, Channel::Market);
                assert_eq!(topics, vec!["RELIANCE", "NIFTY"]);
            }
            other => panic!("expected Subscribe, got {:?}", other),
        }
    }

    #[test]
    fn client_intent_round_trip() {
        let raw = r#"{"type":"intent","kind":"killswitch","payload":{"active":true}}"#;
        let msg: ClientMsg = serde_json::from_str(raw).unwrap();
        match msg {
            ClientMsg::Intent { kind, payload, .. } => {
                assert_eq!(kind, IntentKind::Killswitch);
                assert_eq!(payload["active"], serde_json::json!(true));
            }
            other => panic!("expected Intent, got {:?}", other),
        }
    }

    #[test]
    fn server_event_serialises_with_type_tag() {
        let msg = ServerMsg::Event {
            channel: Channel::Risk,
            subject: None,
            ts_ns: None,
            payload: serde_json::json!({"foo": 1}),
        };
        let s = serde_json::to_string(&msg).unwrap();
        assert!(s.contains(r#""type":"event""#));
        assert!(s.contains(r#""channel":"risk""#));
    }

    #[test]
    fn server_mode_serialises_high_volatility_field() {
        let msg = ServerMsg::Mode { high_volatility: true };
        let s = serde_json::to_string(&msg).unwrap();
        assert_eq!(s, r#"{"type":"mode","high_volatility":true}"#);
    }

    #[test]
    fn unknown_channel_token_fails_to_parse() {
        let raw = r#"{"type":"subscribe","channel":"unknown"}"#;
        let res: Result<ClientMsg, _> = serde_json::from_str(raw);
        assert!(res.is_err(), "unknown channel must be rejected at parse time");
    }
}
