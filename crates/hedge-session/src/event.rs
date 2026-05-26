//! Typed payloads for the `ops.warmode.<phase>` subjects.
//!
//! The wire schema lives canonically in
//! `hedge-schemas/json_schemas/ops_warmode.schema.json` and is mirrored here
//! as a strongly-typed Rust struct with the same field names. This struct is
//! `serde::Serialize + Deserialize` so the typed
//! [`hedge_bus::JsonCodec`](hedge_bus::JsonCodec) bound by [`Subject<T>`]
//! produces wire bytes that round-trip through the JSON Schema validator.
//!
//! A separate enum [`WarModePhase`] is used for the `phase` discriminant —
//! `serde(rename_all = "snake_case")` lines up with the schema's
//! `enum: ["start", "end"]` so the wire form is exactly `"start"` / `"end"`.

use hedge_core::SessionId;
use serde::{Deserialize, Serialize};

/// `phase` discriminant for `ops.warmode.<phase>`.
///
/// The wire form is the lowercase string (`"start"` or `"end"`), matching
/// `ops_warmode.schema.json` and the trailing segment of the NATS subject
/// (`ops.warmode.start`, `ops.warmode.end`).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarModePhase {
    /// War_Mode window opening transition. Subject: `ops.warmode.start`.
    Start,
    /// War_Mode window closing transition. Subject: `ops.warmode.end`.
    End,
}

impl WarModePhase {
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

/// Typed payload for the `ops.warmode.<phase>` subjects.
///
/// Field order and names mirror
/// `hedge-schemas/json_schemas/ops_warmode.schema.json` exactly:
///
/// * `session_id` — Trading_Session identifier (`SessionId.raw()`).
/// * `phase` — `start` or `end`.
/// * `min_confidence` — confidence floor applied while War_Mode is active.
///   Mirrors `WarModeConfig.min_confidence`. Only meaningful on the `start`
///   transition; included on `end` for symmetry so consumers can reset
///   their threshold without holding state.
/// * `scan_multiplier` — scan-frequency multiplier applied to Hot_Path
///   stages while War_Mode is active. Mirrors `WarModeConfig.scan_multiplier`.
///   Required for design § Operating Modes "increased orderflow sensitivity"
///   and "increased breakout detection sensitivity"; the `Hot_Path`
///   components downstream interpret the multiplier as a uniform sensitivity
///   factor across scan rate, orderflow sampling, and breakout
///   detection (R26.2).
/// * `ts_ns` — monotonic process-startup timestamp in nanoseconds (the same
///   reference used by `Tick_v1.ts_recv_ns`); produced by
///   [`hedge_core::now_ns`].
///
/// Both phases carry the full profile fields so a UI/Hot_Path consumer that
/// joins the bus mid-window can adopt the correct profile from the next
/// announcement without a round-trip to the config service.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WarModeEvent {
    /// Trading_Session identifier (matches `SessionId.raw()`).
    pub session_id: u64,
    /// Phase discriminant.
    pub phase: WarModePhase,
    /// Minimum signal confidence accepted while War_Mode is active.
    /// Applied by Risk_Engine and UI gateway as a hard floor. Mirrors
    /// [`hedge_config::WarModeConfig::min_confidence`].
    pub min_confidence: f32,
    /// Scan-frequency / sensitivity multiplier. Mirrors
    /// [`hedge_config::WarModeConfig::scan_multiplier`].
    pub scan_multiplier: f32,
    /// Monotonic timestamp (nanoseconds since process startup) at which the
    /// transition was emitted. Produced by [`hedge_core::now_ns`].
    pub ts_ns: u64,
}

impl WarModeEvent {
    /// Build a `start` event from a `SessionId`, a `WarModeConfig`, and a
    /// monotonic `ts_ns`. Pulled out as a constructor so call sites do not
    /// duplicate the `min_confidence` / `scan_multiplier` projection.
    #[inline]
    pub fn start(session_id: SessionId, min_confidence: f32, scan_multiplier: f32, ts_ns: u64) -> Self {
        Self {
            session_id: session_id.raw(),
            phase: WarModePhase::Start,
            min_confidence,
            scan_multiplier,
            ts_ns,
        }
    }

    /// Build an `end` event. See [`WarModeEvent::start`] for the field
    /// derivation.
    #[inline]
    pub fn end(session_id: SessionId, min_confidence: f32, scan_multiplier: f32, ts_ns: u64) -> Self {
        Self {
            session_id: session_id.raw(),
            phase: WarModePhase::End,
            min_confidence,
            scan_multiplier,
            ts_ns,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `WarModePhase` serializes as the lowercase string the JSON schema
    /// declares.
    #[test]
    fn warmode_phase_serializes_to_snake_case_string() {
        let s = serde_json::to_string(&WarModePhase::Start).unwrap();
        let e = serde_json::to_string(&WarModePhase::End).unwrap();
        assert_eq!(s, "\"start\"");
        assert_eq!(e, "\"end\"");
    }

    /// `WarModePhase::as_str()` matches the trailing subject segment.
    #[test]
    fn warmode_phase_as_str_matches_subject_segment() {
        assert_eq!(WarModePhase::Start.as_str(), "start");
        assert_eq!(WarModePhase::End.as_str(), "end");
    }

    /// A `WarModeEvent` built via the `start`/`end` constructors round-trips
    /// through `serde_json` losslessly.
    #[test]
    fn warmode_event_roundtrips_through_json() {
        let original =
            WarModeEvent::start(SessionId::new(20251130), 0.6, 2.0, 123_456_789);
        let bytes = serde_json::to_vec(&original).unwrap();
        let decoded: WarModeEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, original);

        let end = WarModeEvent::end(SessionId::new(20251130), 0.6, 2.0, 987_654_321);
        let bytes = serde_json::to_vec(&end).unwrap();
        let decoded: WarModeEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, end);
    }

    /// The on-wire JSON respects the schema's required-fields ordering and
    /// does not emit any extras (`serde(deny_unknown_fields)` would
    /// surface a typo on the deserialize side).
    #[test]
    fn warmode_event_wire_form_contains_exact_fields() {
        let ev = WarModeEvent::start(SessionId::new(42), 0.6, 2.0, 7);
        let v: serde_json::Value = serde_json::to_value(&ev).unwrap();
        let obj = v.as_object().unwrap();
        // Expect exactly five fields — same set as the JSON schema's
        // `properties` map (session_id, phase, min_confidence,
        // scan_multiplier, ts_ns).
        assert_eq!(obj.len(), 5);
        assert!(obj.contains_key("session_id"));
        assert!(obj.contains_key("phase"));
        assert!(obj.contains_key("min_confidence"));
        assert!(obj.contains_key("scan_multiplier"));
        assert!(obj.contains_key("ts_ns"));
        assert_eq!(obj["session_id"], serde_json::json!(42));
        assert_eq!(obj["phase"], serde_json::json!("start"));
    }

    /// Spot-check: the canonical JSON Schema is shipped by `hedge-schemas`
    /// and lists the same five required fields plus `additionalProperties:
    /// false`. We rely on `serde(deny_unknown_fields)` for the inverse
    /// direction; this test guards against accidental drift between the
    /// Rust struct's emitted field set and the canonical schema's
    /// `properties` map.
    #[test]
    fn warmode_event_field_set_matches_canonical_schema_properties() {
        let schema_str = hedge_schemas::json_schemas::OPS_WARMODE_SCHEMA;
        let schema_value: serde_json::Value = serde_json::from_str(schema_str).unwrap();
        let props = schema_value["properties"]
            .as_object()
            .expect("properties must be an object");

        let ev = WarModeEvent::start(SessionId::new(1), 0.6, 2.0, 1);
        let payload = serde_json::to_value(&ev).unwrap();
        let payload_obj = payload.as_object().unwrap();

        // Every field we emit is declared in the schema.
        for field in payload_obj.keys() {
            assert!(
                props.contains_key(field),
                "WarModeEvent emits field `{}` that is not in ops_warmode.schema.json",
                field
            );
        }
        // Every required schema field is present in the payload.
        let required = schema_value["required"].as_array().expect("required");
        for r in required {
            let key = r.as_str().expect("required entry must be string");
            assert!(
                payload_obj.contains_key(key),
                "WarModeEvent omits required schema field `{}`",
                key
            );
        }
    }
}
