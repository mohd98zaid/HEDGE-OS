//! [`ReplayRecord`] — the canonical wire format the recorder appends and
//! the player consumes.
//!
//! Layout matches the design's `Replay_Engine` block verbatim:
//!
//! ```text
//! pub struct ReplayRecord {
//!     pub session_id:    SessionId,
//!     pub sequence_no:   u64,             // strict monotonic, gap-free
//!     pub monotonic_ns:  u64,             // quanta::Instant nanos at record time
//!     pub wallclock_utc: i64,
//!     pub kind:          RecordKind,
//!     pub payload:       Bytes,           // rkyv-encoded typed payload
//! }
//! ```
//!
//! Each [`ReplayRecord`] is itself rkyv-encoded for the on-disk segment
//! and Redis-stream wire form. We use `rkyv 0.7` and depend on its
//! built-in archived primitives — `Vec<u8>` for the payload field is
//! supported out of the box. The encoding is **zero-copy on read** when
//! the segment file or Redis-stream entry is `mmap`'d / borrowed: the
//! archived `ArchivedReplayRecord` view aliases the wire bytes
//! directly.
//!
//! For the recorder we use the convenience [`encode_record`] / [`decode_record`]
//! helpers that round-trip through `rkyv::AlignedVec`. The
//! [`framed::write_framed`] / [`framed::read_framed`] helpers add a
//! `u32` length prefix so segment files can hold many records back-to-back.

use bytes::Bytes;
use rkyv::{
    ser::serializers::AllocSerializer, ser::Serializer, AlignedVec, Archive,
    Deserialize as RkyvDeserialize, Infallible, Serialize as RkyvSerialize,
};

/// Source of an `AIDecision` record. Mirrors the design's enum entry
/// `AIDecision { source: AISource }`.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
#[repr(u8)]
pub enum AISource {
    /// `ai.rank.<correlation_id>`.
    Ranking = 0,
    /// `ai.regime.changed`.
    Regime = 1,
    /// `ai.news.impact.<symbol>`.
    News = 2,
    /// `ai.psych.stability` / `ai.psych.intervention`.
    Psychology = 3,
    /// `ai.priority.changed.<symbol>`.
    Priority = 4,
    /// `ai.journal.entry`.
    Journal = 5,
    /// `ai.gov.action`.
    Governance = 6,
    /// `ai.ollama.degraded` and any other Warm_AI_Pipeline event not
    /// matching the categories above.
    Other = 255,
}

/// Discriminant of a recorded event kind.
///
/// Mirrors the design's `RecordKind` enum verbatim. The `AIDecision`
/// variant carries the originating Warm_AI_Pipeline source so a
/// downstream filter can rebuild AI-only views without re-parsing the
/// payload.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub enum RecordKind {
    /// `md.tick.<symbol>` — `Tick_v1` payload.
    Tick,
    /// `md.book.<symbol>` — `OrderBook_v1` payload.
    OrderBook,
    /// `md.oi.<symbol>` — `OpenInterest_v1` payload.
    OpenInterest,
    /// News_Intelligence_Engine input event.
    NewsEvent,
    /// `sig.emitted` — `Signal_v1` payload.
    SignalEmitted,
    /// `risk.decision.{approved,rejected}`.
    RiskDecision,
    /// `exec.order.submitted`.
    OrderSubmitted,
    /// Order modification on an existing working order.
    OrderModified,
    /// `exec.order.cancelled`.
    OrderCancelled,
    /// `exec.fill.<symbol>`.
    Fill,
    /// `trader.intent.*` — every UI-originated control intent.
    TraderAction,
    /// Any `ai.*` Warm_AI_Pipeline event, tagged with [`AISource`].
    AIDecision(AISource),
    /// Aggregate market-condition snapshot taken on a periodic cadence.
    MarketConditionSnapshot,
}

/// One recorded event.
///
/// `payload` carries the typed inner event already encoded in its
/// own wire form (FlatBuffers for Hot_Path payloads, JSON for `ai.*`).
/// The Replay_Engine treats it as opaque bytes — fan-out / decoding is
/// the consumer's job. This is what lets the same recorder handle
/// every event kind without growing a per-kind serializer table.
#[derive(Clone, Debug, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct ReplayRecord {
    /// Session this record belongs to.
    pub session_id: u64,
    /// Strict-monotonic, gap-free sequence number assigned by the
    /// [`Recorder`](crate::Recorder). The first record of a session
    /// is `0`; subsequent records are `+1`. This is the property
    /// Replay determinism (Property 12) hangs on.
    pub sequence_no: u64,
    /// `quanta`-derived monotonic ns at record time. The player uses
    /// successive `monotonic_ns` deltas to pace event release at
    /// `1x` / `10x`.
    pub monotonic_ns: u64,
    /// Wall-clock UTC timestamp in nanoseconds since the Unix epoch.
    /// Recorded for human-readable diffing only — replay timing
    /// derives from `monotonic_ns`, not from this field.
    pub wallclock_utc: i64,
    /// Event kind (and AI source where applicable).
    pub kind: RecordKind,
    /// Opaque per-kind payload (FlatBuffers / JSON / etc.).
    pub payload: Vec<u8>,
}

impl ReplayRecord {
    /// Number of bytes in the rkyv-encoded form, including the rkyv
    /// alignment padding. Returned by [`encode_record`] without
    /// re-encoding.
    #[inline]
    pub fn payload_bytes(&self) -> usize {
        self.payload.len()
    }
}

/// Encode `record` into a fresh `Bytes` buffer using rkyv's default
/// allocation-backed serializer.
///
/// The output is a self-contained rkyv archive: the consumer reads it
/// with [`archived_root::<ReplayRecord>`].
pub fn encode_record(record: &ReplayRecord) -> Bytes {
    // 1024-byte scratch is plenty for the typical Hot_Path payload sizes
    // we see in `hedge-schemas` (Tick, OrderBook, Signal, Fill — all
    // sub-512B). Larger payloads will simply spill into the heap-
    // allocated portion of the serializer.
    let mut serializer = AllocSerializer::<1024>::default();
    serializer
        .serialize_value(record)
        .expect("rkyv serialize_value should never fail for ReplayRecord");
    let aligned: AlignedVec = serializer.into_serializer().into_inner();
    Bytes::from(aligned.into_vec())
}

/// View `bytes` as an [`ArchivedReplayRecord`] without copying.
///
/// Returns `None` if `bytes` does not pass rkyv's check-bytes
/// validation (truncated, corrupted, or wrong type).
pub fn view_archived(bytes: &[u8]) -> Option<&ArchivedReplayRecord> {
    rkyv::check_archived_root::<ReplayRecord>(bytes).ok()
}

/// Decode `bytes` into an owned [`ReplayRecord`].
///
/// Convenience wrapper around [`view_archived`] +
/// `Deserialize<ReplayRecord, Infallible>`. Use [`view_archived`]
/// directly when zero-copy access is sufficient.
pub fn decode_record(bytes: &[u8]) -> Option<ReplayRecord> {
    let archived = view_archived(bytes)?;
    archived.deserialize(&mut Infallible).ok()
}

/// Length-prefix framing helpers.
///
/// Each segment file is a sequence of records framed as
/// `<u32 length, big-endian> <length bytes of rkyv archive>`. Big-endian
/// length is chosen for human-readable hex dumps; the payload itself
/// is rkyv (little-endian on the only platform we target — `x86_64`
/// Mumbai VPS).
pub mod framed {
    use std::io::{Read, Result, Write};

    /// Write one length-prefixed rkyv archive into `out`.
    pub fn write_framed<W: Write>(out: &mut W, archive: &[u8]) -> Result<()> {
        let len = archive.len() as u32;
        out.write_all(&len.to_be_bytes())?;
        out.write_all(archive)?;
        Ok(())
    }

    /// Read one length-prefixed rkyv archive from `inp`.
    ///
    /// Returns `Ok(None)` on clean EOF (no bytes available before the
    /// length prefix), `Ok(Some(_))` on success, and
    /// `Err(_)` on a partial frame or I/O error.
    pub fn read_framed<R: Read>(inp: &mut R) -> Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        match inp.read(&mut len_buf)? {
            0 => return Ok(None), // clean EOF
            4 => {}
            n => {
                // Read more bytes until we have the full length prefix
                // or hit EOF mid-prefix (treated as truncation).
                let mut filled = n;
                while filled < 4 {
                    match inp.read(&mut len_buf[filled..])? {
                        0 => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "truncated length prefix",
                            ));
                        }
                        more => filled += more,
                    }
                }
            }
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        inp.read_exact(&mut buf)?;
        Ok(Some(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(seq: u64) -> ReplayRecord {
        ReplayRecord {
            session_id: 20251130,
            sequence_no: seq,
            monotonic_ns: 1_000_000 + seq * 250_000,
            wallclock_utc: 1_700_000_000_000_000_000 + seq as i64,
            kind: RecordKind::Tick,
            payload: vec![0xAA, 0xBB, 0xCC, 0xDD],
        }
    }

    #[test]
    fn encode_round_trips_through_decode() {
        let r = sample_record(7);
        let bytes = encode_record(&r);
        let back = decode_record(&bytes).expect("valid archive");
        assert_eq!(back, r);
    }

    #[test]
    fn view_archived_returns_zero_copy_view() {
        let r = sample_record(11);
        let bytes = encode_record(&r);
        let view = view_archived(&bytes).expect("valid archive");
        assert_eq!(view.session_id, r.session_id);
        assert_eq!(view.sequence_no, r.sequence_no);
        assert_eq!(view.monotonic_ns, r.monotonic_ns);
        assert_eq!(view.wallclock_utc, r.wallclock_utc);
        assert_eq!(view.payload.as_slice(), r.payload.as_slice());
    }

    #[test]
    fn decode_rejects_truncated_archive() {
        let r = sample_record(1);
        let bytes = encode_record(&r);
        let truncated = &bytes[..bytes.len().saturating_sub(2)];
        // Truncated buffers should fail check_bytes — no panic.
        assert!(decode_record(truncated).is_none());
    }

    #[test]
    fn ai_source_round_trips_inside_record_kind() {
        let r = ReplayRecord {
            session_id: 1,
            sequence_no: 0,
            monotonic_ns: 0,
            wallclock_utc: 0,
            kind: RecordKind::AIDecision(AISource::Ranking),
            payload: Vec::new(),
        };
        let bytes = encode_record(&r);
        let back = decode_record(&bytes).unwrap();
        assert_eq!(back.kind, RecordKind::AIDecision(AISource::Ranking));
    }

    #[test]
    fn framed_round_trip_three_records() {
        // Write three records into a buffer, then read them back in
        // the same order. This is the basic invariant the segment
        // reader relies on.
        let mut buf: Vec<u8> = Vec::new();
        for i in 0..3u64 {
            let r = sample_record(i);
            let bytes = encode_record(&r);
            framed::write_framed(&mut buf, &bytes).unwrap();
        }
        let mut cursor = std::io::Cursor::new(buf);
        let mut got = Vec::new();
        while let Some(frame) = framed::read_framed(&mut cursor).unwrap() {
            let r = decode_record(&frame).unwrap();
            got.push(r.sequence_no);
        }
        assert_eq!(got, vec![0, 1, 2]);
    }

    #[test]
    fn framed_clean_eof_at_start() {
        // Reading from an empty buffer must return `None`, not an
        // unexpected-EOF error.
        let mut empty = std::io::Cursor::new(Vec::<u8>::new());
        let frame = framed::read_framed(&mut empty).unwrap();
        assert!(frame.is_none());
    }

    #[test]
    fn framed_truncated_length_prefix_errors() {
        // A buffer with 2 bytes is shorter than the 4-byte length
        // prefix and must surface as an `UnexpectedEof`.
        let mut cursor = std::io::Cursor::new(vec![0x00u8, 0x00]);
        let err = framed::read_framed(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
