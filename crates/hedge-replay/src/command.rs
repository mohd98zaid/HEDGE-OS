//! `/replay` UI control plane.
//!
//! The Human_Control_UI talks to the Replay_Engine through a small
//! request-reply protocol carried over NATS subjects in the
//! `replay.command.*` namespace (R22.3, design § Components §
//! Replay_Engine — UI control plane). A thin HTTP gateway in front of
//! the same protocol can be added later without changing the wire
//! format.
//!
//! ### Subjects
//!
//! | Subject                         | Verb | Request body                | Response body                 |
//! |---------------------------------|------|-----------------------------|-------------------------------|
//! | `replay.command.list`           | GET  | `{}`                        | `{ "sessions": [u64, ...] }`  |
//! | `replay.command.open`           | POST | `{ "session_id": u64, ... }`| `{ "ok": true, "total": u64 }`|
//! | `replay.command.scrub`          | POST | `{ "sequence_no": u64 }`    | `{ "ok": true, "cursor": u64 }`|
//! | `replay.command.step`           | POST | `{}`                        | `{ "record": ReplayRecordWire? }`|
//! | `replay.command.play`           | POST | `{ "speed": "x1"|"x10"|"max" }` | `{ "ok": true }`          |
//! | `replay.command.status`         | GET  | `{}`                        | `{ "session_id": u64?, "cursor": u64, "total": u64, "speed": str }` |
//!
//! All payloads are JSON. The Hot_Path side of the bus uses
//! FlatBuffers for `md.*`/`sig.*`/etc., but the replay control plane
//! is a thin developer-facing protocol where ergonomics dominate.
//!
//! ### Design notes
//!
//! * The command subjects live outside the `hedge.hot.*` Redis stream
//!   namespace because they carry control commands, not data. Redis
//!   Streams are append-only — they are not the right substrate for
//!   request-reply.
//! * NATS `request` is the canonical request-reply primitive; the
//!   `hedge-bus` crate exposes the underlying `async_nats::Client`
//!   for callers that need it.

use serde::{Deserialize, Serialize};

use crate::record::{AISource, RecordKind};

/// Subject prefix for every replay command.
pub const REPLAY_COMMAND_PREFIX: &str = "replay.command";

/// `replay.command.list` — list every recorded session.
pub const REPLAY_COMMAND_LIST: &str = "replay.command.list";

/// `replay.command.open` — open a session for replay.
pub const REPLAY_COMMAND_OPEN: &str = "replay.command.open";

/// `replay.command.scrub` — move the cursor to a sequence_no.
pub const REPLAY_COMMAND_SCRUB: &str = "replay.command.scrub";

/// `replay.command.step` — advance the cursor by one record.
pub const REPLAY_COMMAND_STEP: &str = "replay.command.step";

/// `replay.command.play` — start paced playback.
pub const REPLAY_COMMAND_PLAY: &str = "replay.command.play";

/// `replay.command.status` — return current player status.
pub const REPLAY_COMMAND_STATUS: &str = "replay.command.status";

/// Request to `replay.command.list`. Has no parameters but is sent as
/// an empty JSON object so the wire form is symmetric with other
/// commands.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ListSessionsRequest {}

/// Response for `replay.command.list`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ListSessionsResponse {
    /// Recorded sessions, ascending by `session_id`.
    pub sessions: Vec<u64>,
}

/// Request to `replay.command.open`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenSessionRequest {
    /// Session id to open.
    pub session_id: u64,
    /// Optional initial speed override. Defaults to the player's
    /// configured `default_speed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<String>,
}

/// Response for `replay.command.open`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenSessionResponse {
    /// Whether the session was successfully loaded.
    pub ok: bool,
    /// Total number of records in the session.
    pub total: u64,
}

/// Request to `replay.command.scrub`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScrubRequest {
    /// Target sequence_no to seek to.
    pub sequence_no: u64,
}

/// Response for `replay.command.scrub` and `replay.command.step`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CursorResponse {
    /// Whether the operation succeeded.
    pub ok: bool,
    /// New cursor position.
    pub cursor: u64,
    /// Optional record returned by `replay.command.step` — the record
    /// the cursor just consumed, in JSON-serialised wire form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<ReplayRecordWire>,
}

/// Request to `replay.command.play`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlayRequest {
    /// Speed token: `"x1"`, `"x10"`, or `"max"`.
    pub speed: String,
}

/// Response for `replay.command.play`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AckResponse {
    /// Whether the command was accepted.
    pub ok: bool,
    /// Optional human-readable detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Response for `replay.command.status`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Currently-open session, or `None` if no session is open.
    pub session_id: Option<u64>,
    /// Cursor position within the session.
    pub cursor: u64,
    /// Total records.
    pub total: u64,
    /// Current speed token.
    pub speed: String,
}

/// JSON-friendly wire form of [`ReplayRecord`](crate::ReplayRecord).
///
/// We do not ship the rkyv archive over the control plane — the UI
/// is a JSON consumer. Instead, when the control plane needs to
/// surface a record (e.g. on `step`), it converts to this lossless
/// JSON form. Round-tripping rkyv ↔ JSON ↔ rkyv is not required, so
/// the wire form does not have to be a bijection of the rkyv
/// archive.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayRecordWire {
    /// Session id.
    pub session_id: u64,
    /// Sequence number.
    pub sequence_no: u64,
    /// Monotonic ns at record time.
    pub monotonic_ns: u64,
    /// Wall-clock UTC ns at record time.
    pub wallclock_utc: i64,
    /// Record kind.
    pub kind: RecordKindWire,
    /// Payload bytes, base64-encoded for JSON transport.
    pub payload_b64: String,
}

/// JSON-friendly version of [`RecordKind`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKindWire {
    /// `md.tick.<symbol>`.
    Tick,
    /// `md.book.<symbol>`.
    OrderBook,
    /// `md.oi.<symbol>`.
    OpenInterest,
    /// News_Intelligence_Engine event.
    NewsEvent,
    /// `sig.emitted`.
    SignalEmitted,
    /// `risk.decision.*`.
    RiskDecision,
    /// `exec.order.submitted`.
    OrderSubmitted,
    /// Order modification.
    OrderModified,
    /// `exec.order.cancelled`.
    OrderCancelled,
    /// `exec.fill.<symbol>`.
    Fill,
    /// `trader.intent.*`.
    TraderAction,
    /// `ai.*` Warm_AI_Pipeline event. The `source` token preserves
    /// the originating subject family.
    AiDecision {
        /// Originating Warm_AI_Pipeline source.
        source: AiSourceWire,
    },
    /// Aggregate market-condition snapshot.
    MarketConditionSnapshot,
}

/// JSON-friendly version of [`AISource`].
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiSourceWire {
    /// `ai.rank.<correlation_id>`.
    Ranking,
    /// `ai.regime.changed`.
    Regime,
    /// `ai.news.impact.<symbol>`.
    News,
    /// `ai.psych.*`.
    Psychology,
    /// `ai.priority.changed.<symbol>`.
    Priority,
    /// `ai.journal.entry`.
    Journal,
    /// `ai.gov.action`.
    Governance,
    /// Anything not matching the above.
    Other,
}

impl From<RecordKind> for RecordKindWire {
    fn from(k: RecordKind) -> Self {
        match k {
            RecordKind::Tick => Self::Tick,
            RecordKind::OrderBook => Self::OrderBook,
            RecordKind::OpenInterest => Self::OpenInterest,
            RecordKind::NewsEvent => Self::NewsEvent,
            RecordKind::SignalEmitted => Self::SignalEmitted,
            RecordKind::RiskDecision => Self::RiskDecision,
            RecordKind::OrderSubmitted => Self::OrderSubmitted,
            RecordKind::OrderModified => Self::OrderModified,
            RecordKind::OrderCancelled => Self::OrderCancelled,
            RecordKind::Fill => Self::Fill,
            RecordKind::TraderAction => Self::TraderAction,
            RecordKind::AIDecision(src) => Self::AiDecision {
                source: src.into(),
            },
            RecordKind::MarketConditionSnapshot => Self::MarketConditionSnapshot,
        }
    }
}

impl From<AISource> for AiSourceWire {
    fn from(s: AISource) -> Self {
        match s {
            AISource::Ranking => Self::Ranking,
            AISource::Regime => Self::Regime,
            AISource::News => Self::News,
            AISource::Psychology => Self::Psychology,
            AISource::Priority => Self::Priority,
            AISource::Journal => Self::Journal,
            AISource::Governance => Self::Governance,
            AISource::Other => Self::Other,
        }
    }
}

impl From<&crate::record::ReplayRecord> for ReplayRecordWire {
    fn from(r: &crate::record::ReplayRecord) -> Self {
        // Inline base64 alphabet (RFC 4648 standard, no padding-strip).
        // We avoid pulling the `base64` crate transitively into
        // `hedge-replay` because the dep is not yet on the workspace
        // for this crate.
        let payload_b64 = base64_encode(&r.payload);
        Self {
            session_id: r.session_id,
            sequence_no: r.sequence_no,
            monotonic_ns: r.monotonic_ns,
            wallclock_utc: r.wallclock_utc,
            kind: r.kind.into(),
            payload_b64,
        }
    }
}

/// RFC-4648 base64 with `+/` and `=` padding. Pulled inline to avoid
/// expanding the transitive dep set on the recorder/player.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 3 <= input.len() {
        let n = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8) | (input[i + 2] as u32);
        out.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3F) as usize] as char);
        out.push(TABLE[(n & 0x3F) as usize] as char);
        i += 3;
    }
    let rem = input.len() - i;
    if rem == 1 {
        let n = (input[i] as u32) << 16;
        out.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let n = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8);
        out.push(TABLE[((n >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3F) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3F) as usize] as char);
        out.push('=');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{RecordKind, ReplayRecord};

    #[test]
    fn subject_constants_are_exact() {
        assert_eq!(REPLAY_COMMAND_PREFIX, "replay.command");
        assert_eq!(REPLAY_COMMAND_LIST, "replay.command.list");
        assert_eq!(REPLAY_COMMAND_OPEN, "replay.command.open");
        assert_eq!(REPLAY_COMMAND_SCRUB, "replay.command.scrub");
        assert_eq!(REPLAY_COMMAND_STEP, "replay.command.step");
        assert_eq!(REPLAY_COMMAND_PLAY, "replay.command.play");
        assert_eq!(REPLAY_COMMAND_STATUS, "replay.command.status");
    }

    #[test]
    fn list_response_round_trips_through_json() {
        let r = ListSessionsResponse {
            sessions: vec![100, 200, 300],
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: ListSessionsResponse = serde_json::from_str(&s).unwrap();
        assert_eq!(r.sessions, back.sessions);
    }

    #[test]
    fn record_kind_wire_form_matches_design_tokens() {
        let s = serde_json::to_string(&RecordKindWire::Tick).unwrap();
        assert_eq!(s, "\"tick\"");
        let s = serde_json::to_string(&RecordKindWire::SignalEmitted).unwrap();
        assert_eq!(s, "\"signal_emitted\"");
        let s = serde_json::to_string(&RecordKindWire::OpenInterest).unwrap();
        assert_eq!(s, "\"open_interest\"");
    }

    #[test]
    fn ai_decision_carries_source_in_wire_form() {
        let k = RecordKindWire::AiDecision {
            source: AiSourceWire::Ranking,
        };
        let s = serde_json::to_string(&k).unwrap();
        // serde-tagged enums: { "ai_decision": { "source": "ranking" } }
        assert!(s.contains("ai_decision"), "wire form: {}", s);
        assert!(s.contains("ranking"), "wire form: {}", s);
    }

    #[test]
    fn record_wire_conversion_preserves_fields() {
        let r = ReplayRecord {
            session_id: 42,
            sequence_no: 7,
            monotonic_ns: 1_234_000,
            wallclock_utc: 1_700_000_000_000_000_000,
            kind: RecordKind::Fill,
            payload: vec![0x01, 0x02, 0x03],
        };
        let w: ReplayRecordWire = (&r).into();
        assert_eq!(w.session_id, 42);
        assert_eq!(w.sequence_no, 7);
        assert_eq!(w.monotonic_ns, 1_234_000);
        assert_eq!(w.wallclock_utc, 1_700_000_000_000_000_000);
        // Base64 of [0x01, 0x02, 0x03] is "AQID".
        assert_eq!(w.payload_b64, "AQID");
    }

    #[test]
    fn base64_encode_handles_padding_branches() {
        assert_eq!(base64_encode(&[]), "");
        assert_eq!(base64_encode(&[0x66]), "Zg==");
        assert_eq!(base64_encode(&[0x66, 0x6f]), "Zm8=");
        assert_eq!(base64_encode(&[0x66, 0x6f, 0x6f]), "Zm9v");
        assert_eq!(base64_encode(&[0x66, 0x6f, 0x6f, 0x62]), "Zm9vYg==");
    }
}
