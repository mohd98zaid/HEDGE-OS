//! Typed payload for the `ops.session.<phase>` subjects.
//!
//! The wire schema lives canonically in
//! `hedge-schemas/json_schemas/ops_session.schema.json` and is mirrored
//! here as a strongly-typed Rust struct with the same field names. This
//! struct is `serde::Serialize + Deserialize` so the typed
//! [`hedge_bus::JsonCodec`](hedge_bus::JsonCodec) bound by [`Subject<T>`]
//! produces wire bytes that round-trip through the JSON Schema validator.
//!
//! `SessionEvent` and [`crate::WarModeEvent`] are deliberately distinct
//! types — the schemas differ (the war-mode event additionally carries
//! `min_confidence` and `scan_multiplier` profile fields). Sharing a
//! single phase-tag enum across both events would invite drift the day
//! the canonical `ops_session.schema.json` adds a session-only field;
//! keeping them parallel is intentional.

use hedge_core::SessionId;
use serde::{Deserialize, Serialize};

/// `phase` discriminant for `ops.session.<phase>`.
///
/// The wire form is the lowercase string (`"start"` or `"end"`),
/// matching `ops_session.schema.json` and the trailing segment of the
/// NATS subject (`ops.session.start`, `ops.session.end`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    /// Trading_Session opening transition. Subject: `ops.session.start`.
    Start,
    /// Trading_Session closing transition. Subject: `ops.session.end`.
    End,
}

impl SessionPhase {
    /// The trailing segment of the NATS subject for this phase
    /// (`"start"` or `"end"`).
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::End => "end",
        }
    }
}

/// Typed payload for the `ops.session.<phase>` subjects.
///
/// Field order and names mirror
/// `hedge-schemas/json_schemas/ops_session.schema.json` exactly:
///
/// * `session_id` — Trading_Session identifier (`SessionId.raw()`).
/// * `phase` — `start` or `end`.
/// * `ts_ns` — monotonic process-startup timestamp in nanoseconds (the
///   same reference used by `Tick_v1.ts_recv_ns`); produced by
///   [`hedge_core::now_ns`].
///
/// `serde(deny_unknown_fields)` mirrors the schema's
/// `additionalProperties: false` so an accidentally renamed Rust field
/// surfaces immediately as a deserialize error rather than silently
/// dropping data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionEvent {
    /// Trading_Session identifier (matches `SessionId.raw()`).
    pub session_id: u64,
    /// Phase discriminant.
    pub phase: SessionPhase,
    /// Monotonic timestamp (nanoseconds since process startup) at which
    /// the transition was emitted. Produced by [`hedge_core::now_ns`].
    pub ts_ns: u64,
}

impl SessionEvent {
    /// Build a `start` event from a `SessionId` and a monotonic `ts_ns`.
    /// Pulled out as a constructor so call sites do not duplicate the
    /// phase-tag projection.
    #[inline]
    pub fn start(session_id: SessionId, ts_ns: u64) -> Self {
        Self {
            session_id: session_id.raw(),
            phase: SessionPhase::Start,
            ts_ns,
        }
    }

    /// Build an `end` event. See [`SessionEvent::start`] for the field
    /// derivation.
    #[inline]
    pub fn end(session_id: SessionId, ts_ns: u64) -> Self {
        Self {
            session_id: session_id.raw(),
            phase: SessionPhase::End,
            ts_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SessionPhase` serializes as the lowercase string the JSON schema
    /// declares.
    #[test]
    fn session_phase_serializes_to_snake_case_string() {
        let s = serde_json::to_string(&SessionPhase::Start).unwrap();
        let e = serde_json::to_string(&SessionPhase::End).unwrap();
        assert_eq!(s, "\"start\"");
        assert_eq!(e, "\"end\"");
    }

    /// `SessionPhase::as_str()` matches the trailing subject segment.
    #[test]
    fn session_phase_as_str_matches_subject_segment() {
        assert_eq!(SessionPhase::Start.as_str(), "start");
        assert_eq!(SessionPhase::End.as_str(), "end");
    }

    /// A `SessionEvent` built via the `start`/`end` constructors
    /// round-trips through `serde_json` losslessly.
    #[test]
    fn session_event_roundtrips_through_json() {
        let original = SessionEvent::start(SessionId::new(20_251_130), 123_456_789);
        let bytes = serde_json::to_vec(&original).unwrap();
        let decoded: SessionEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, original);

        let end = SessionEvent::end(SessionId::new(20_251_130), 987_654_321);
        let bytes = serde_json::to_vec(&end).unwrap();
        let decoded: SessionEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, end);
    }

    /// The on-wire JSON respects the schema's required-fields ordering
    /// and does not emit any extras.
    #[test]
    fn session_event_wire_form_contains_exact_fields() {
        let ev = SessionEvent::start(SessionId::new(42), 7);
        let v: serde_json::Value = serde_json::to_value(&ev).unwrap();
        let obj = v.as_object().unwrap();
        // Exactly three fields — same set as the JSON schema's
        // `properties` map (session_id, phase, ts_ns).
        assert_eq!(obj.len(), 3);
        assert!(obj.contains_key("session_id"));
        assert!(obj.contains_key("phase"));
        assert!(obj.contains_key("ts_ns"));
        assert_eq!(obj["session_id"], serde_json::json!(42));
        assert_eq!(obj["phase"], serde_json::json!("start"));
        assert_eq!(obj["ts_ns"], serde_json::json!(7));
    }

    /// Spot-check: the canonical JSON Schema is shipped by
    /// `hedge-schemas` and lists the same three required fields plus
    /// `additionalProperties: false`. We rely on
    /// `serde(deny_unknown_fields)` for the inverse direction; this
    /// test guards against accidental drift between the Rust struct's
    /// emitted field set and the canonical schema's `properties` map.
    #[test]
    fn session_event_field_set_matches_canonical_schema_properties() {
        let schema_str = hedge_schemas::json_schemas::OPS_SESSION_SCHEMA;
        let schema_value: serde_json::Value = serde_json::from_str(schema_str).unwrap();
        let props = schema_value["properties"]
            .as_object()
            .expect("properties must be an object");

        let ev = SessionEvent::start(SessionId::new(1), 1);
        let payload = serde_json::to_value(&ev).unwrap();
        let payload_obj = payload.as_object().unwrap();

        // Every field we emit is declared in the schema.
        for field in payload_obj.keys() {
            assert!(
                props.contains_key(field),
                "SessionEvent emits field `{}` that is not in ops_session.schema.json",
                field
            );
        }
        // Every required schema field is present in the payload.
        let required = schema_value["required"].as_array().expect("required");
        for r in required {
            let key = r.as_str().expect("required entry must be string");
            assert!(
                payload_obj.contains_key(key),
                "SessionEvent omits required schema field `{}`",
                key
            );
        }
    }
}
