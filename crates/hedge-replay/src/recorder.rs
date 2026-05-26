//! [`Recorder`] — append-only replay ledger writer.
//!
//! The recorder owns:
//!
//! 1. one [`SegmentWriter`] writing length-prefixed rkyv archives into
//!    `<segment_dir>/<session_id>/seg-NNNN.rkyv`, rolling on session
//!    boundary or when the active segment exceeds `max_segment_bytes`
//!    (default 1 GiB);
//! 2. an optional Redis Stream producer appending the same record to
//!    [`hedge_bus::STREAM_HOT_REPLAY_RECORD`] for live observers.
//!
//! Both sinks see the same record, in the same order, with the same
//! strict-monotonic gap-free `sequence_no` (R22.1 — design §
//! Replay_Engine § ReplayRecord). The disk sink is the system of
//! record; the Redis sink is an optional live stream that downstream
//! components may tail.
//!
//! ### Concurrency
//!
//! The recorder is meant to be owned by a single tokio task that funnels
//! every recordable event through it (see the design's "Replay and
//! Recording Flow" sequence diagram). It is therefore `&mut self` on
//! every method — no internal locking is needed and no mutex is held
//! across the disk write or the `XADD`. Callers that fan in from
//! multiple producers must serialise upstream (e.g. via an
//! `mpsc::channel` into the recorder task).
//!
//! ### Sequence numbering
//!
//! `record(kind, payload)` — the public API — assigns the
//! `sequence_no` itself, starting at 0 for each session. Callers may
//! also pass an explicit [`ReplayRecord`] via [`Recorder::record_raw`];
//! that path validates the sequence number against the recorder's
//! internal counter and returns
//! [`ReplayError::SequenceInvariant`](crate::ReplayError::SequenceInvariant)
//! on a gap or out-of-order write.

use std::path::PathBuf;

use bytes::Bytes;
use hedge_bus::{RedisStreamProducer, STREAM_HOT_REPLAY_RECORD};
use hedge_core::{now_ns, SessionId};

use crate::codec::ReplayRecordCodec;
use crate::error::ReplayError;
use crate::record::{ReplayRecord, RecordKind};
use crate::segments::{SegmentWriter, DEFAULT_MAX_SEGMENT_BYTES};

/// Wall-clock UTC nanoseconds since the Unix epoch.
///
/// `chrono::Utc::now()` is used here rather than `quanta::Instant` because
/// the wall-clock field is for human-readable diffing only — replay
/// timing derives from the `monotonic_ns` field, which uses
/// [`hedge_core::now_ns`].
#[inline]
fn wallclock_utc_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

/// Configuration for [`Recorder`].
#[derive(Clone, Debug)]
pub struct RecorderConfig {
    /// Root directory for segment files.
    pub segment_dir: PathBuf,
    /// Per-segment size budget. Default 1 GiB.
    pub max_segment_bytes: u64,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            segment_dir: PathBuf::from("./replay"),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        }
    }
}

/// Append-only replay-ledger writer.
pub struct Recorder {
    /// Trading_Session this recorder is scoped to.
    session_id: SessionId,
    /// Disk-segment writer. The recorder writes to disk first; Redis
    /// is best-effort and a Redis failure does not block the disk
    /// write.
    segment_writer: SegmentWriter,
    /// Optional Redis Stream producer. `None` when the recorder is
    /// running offline (tests, replay-only use, broken Redis).
    redis_producer: Option<RedisStreamProducer<ReplayRecord, ReplayRecordCodec>>,
    /// Strict-monotonic gap-free sequence counter. Starts at 0 and is
    /// incremented after every successful `record`.
    next_seq: u64,
    /// Total bytes written to disk to date. Used by tests and the
    /// observability dashboards.
    bytes_written: u64,
    /// Total records appended.
    records_appended: u64,
}

impl Recorder {
    /// Construct a recorder for `session_id` rooted at `segment_dir`.
    pub fn new(session_id: SessionId, cfg: RecorderConfig) -> Self {
        let writer = SegmentWriter::new(cfg.segment_dir, cfg.max_segment_bytes);
        Self {
            session_id,
            segment_writer: writer,
            redis_producer: None,
            next_seq: 0,
            bytes_written: 0,
            records_appended: 0,
        }
    }

    /// Construct a recorder that also publishes to a Redis Stream.
    pub fn with_redis(
        session_id: SessionId,
        cfg: RecorderConfig,
        redis_conn: redis::aio::ConnectionManager,
    ) -> Self {
        let mut r = Self::new(session_id, cfg);
        r.redis_producer = Some(RedisStreamProducer::new(
            redis_conn,
            STREAM_HOT_REPLAY_RECORD,
            ReplayRecordCodec,
        ));
        r
    }

    /// Enable Redis publishing on an existing recorder. Used by
    /// downstream wiring code that constructs the recorder before the
    /// Redis connection is ready.
    pub fn attach_redis(&mut self, redis_conn: redis::aio::ConnectionManager) {
        self.redis_producer = Some(RedisStreamProducer::new(
            redis_conn,
            STREAM_HOT_REPLAY_RECORD,
            ReplayRecordCodec,
        ));
    }

    /// Session this recorder is scoped to.
    #[inline]
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Sequence number that will be assigned to the **next** record.
    /// Equal to the count of records appended so far.
    #[inline]
    pub fn next_sequence_no(&self) -> u64 {
        self.next_seq
    }

    /// Total bytes written to disk so far.
    #[inline]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Total records appended so far.
    #[inline]
    pub fn records_appended(&self) -> u64 {
        self.records_appended
    }

    /// Whether a Redis sink is attached.
    #[inline]
    pub fn has_redis_sink(&self) -> bool {
        self.redis_producer.is_some()
    }

    /// Flush the disk segment. The Redis sink is line-oriented and does
    /// not require an explicit flush.
    pub fn flush(&mut self) -> Result<(), ReplayError> {
        self.segment_writer
            .flush()
            .map_err(|e| ReplayError::segment_io(self.segment_dir_for_error(), e))
    }

    /// Append one record built from a `kind` and a pre-encoded
    /// `payload`. The recorder stamps the current monotonic and
    /// wall-clock timestamps and assigns the next sequence number.
    pub async fn record(
        &mut self,
        kind: RecordKind,
        payload: impl Into<Vec<u8>>,
    ) -> Result<u64, ReplayError> {
        let record = ReplayRecord {
            session_id: self.session_id.raw(),
            sequence_no: self.next_seq,
            monotonic_ns: now_ns(),
            wallclock_utc: wallclock_utc_ns(),
            kind,
            payload: payload.into(),
        };
        self.record_raw(record).await
    }

    /// Append `record`, validating its sequence_no against the
    /// recorder's internal counter.
    ///
    /// Returns the sequence_no that was committed (which equals
    /// `record.sequence_no`).
    pub async fn record_raw(&mut self, record: ReplayRecord) -> Result<u64, ReplayError> {
        // Strict-monotonic, gap-free invariant. Caller error here is
        // a hard configuration bug — the recorder must NOT silently
        // accept gaps because Replay determinism (Property 12) hangs
        // on the property "i-th replayed event has sequence_no == i".
        if record.sequence_no != self.next_seq {
            return Err(ReplayError::SequenceInvariant {
                session: self.session_id.raw(),
                expected: self.next_seq,
                got: record.sequence_no,
            });
        }
        if record.session_id != self.session_id.raw() {
            return Err(ReplayError::Config(format!(
                "record.session_id ({}) does not match recorder.session_id ({})",
                record.session_id,
                self.session_id.raw()
            )));
        }

        // Disk first — the segment writer is the system of record.
        let path = self
            .segment_writer
            .append(&record)
            .map_err(|e| ReplayError::segment_io(self.segment_dir_for_error(), e))?;
        // Account against the size of the rkyv-encoded archive plus
        // the 4-byte length prefix written by `framed::write_framed`.
        let frame_size = 4u64 + crate::record::encode_record(&record).len() as u64;
        self.bytes_written = self.bytes_written.saturating_add(frame_size);

        // Redis sink is best-effort. A Redis outage must not lose
        // records — the disk segment is the durable home — so we
        // surface the Redis error to the caller without rolling back
        // the disk write.
        if let Some(producer) = self.redis_producer.as_mut() {
            // We pre-encode once so the disk path and the Redis path
            // share an identical payload. `xadd_bytes` accepts a
            // pre-encoded buffer.
            let archive = crate::record::encode_record(&record);
            producer
                .xadd_bytes(Bytes::from(archive.to_vec()))
                .await
                .map_err(ReplayError::from)?;
        }

        let committed = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.records_appended = self.records_appended.saturating_add(1);
        let _ = path; // path returned for tests via `last_segment_path`
        Ok(committed)
    }

    /// Path of the segment the most-recent append landed in. `None`
    /// when nothing has been written for this session yet.
    pub fn active_segment_path(&self) -> Option<PathBuf> {
        if self.segment_writer.active_segment_idx() == 0 {
            return None;
        }
        let session_id = self.segment_writer.active_session()?;
        Some(
            self.segment_writer
                .base_dir()
                .join(session_id.to_string())
                .join(format!(
                    "seg-{:04}.rkyv",
                    self.segment_writer.active_segment_idx()
                )),
        )
    }

    fn segment_dir_for_error(&self) -> PathBuf {
        self.segment_writer.base_dir().to_path_buf()
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        // Best-effort flush. The drop runs during shutdown and must
        // not panic.
        let _ = self.segment_writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordKind;
    use crate::segments::SegmentReader;
    use tempfile::TempDir;

    fn cfg(dir: &std::path::Path) -> RecorderConfig {
        RecorderConfig {
            segment_dir: dir.to_path_buf(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        }
    }

    #[tokio::test]
    async fn record_assigns_sequence_starting_at_zero() {
        let tmp = TempDir::new().unwrap();
        let mut rec = Recorder::new(SessionId::new(20251130), cfg(tmp.path()));
        let s0 = rec.record(RecordKind::Tick, vec![1u8, 2, 3]).await.unwrap();
        let s1 = rec.record(RecordKind::Tick, vec![4u8, 5, 6]).await.unwrap();
        let s2 = rec
            .record(RecordKind::SignalEmitted, vec![7u8])
            .await
            .unwrap();
        assert_eq!((s0, s1, s2), (0, 1, 2));
        assert_eq!(rec.records_appended(), 3);
        assert_eq!(rec.next_sequence_no(), 3);
        assert!(rec.bytes_written() > 0);
    }

    #[tokio::test]
    async fn record_raw_rejects_out_of_order_sequence() {
        let tmp = TempDir::new().unwrap();
        let session = SessionId::new(99);
        let mut rec = Recorder::new(session, cfg(tmp.path()));
        let bad = ReplayRecord {
            session_id: 99,
            sequence_no: 5, // gap — recorder expects 0
            monotonic_ns: 0,
            wallclock_utc: 0,
            kind: RecordKind::Tick,
            payload: vec![],
        };
        let err = rec.record_raw(bad).await.unwrap_err();
        match err {
            ReplayError::SequenceInvariant {
                session: 99,
                expected: 0,
                got: 5,
            } => {}
            other => panic!("wrong error: {other:?}"),
        }
        // The bad record must NOT have been written.
        assert_eq!(rec.records_appended(), 0);
    }

    #[tokio::test]
    async fn record_raw_rejects_session_id_mismatch() {
        let tmp = TempDir::new().unwrap();
        let mut rec = Recorder::new(SessionId::new(7), cfg(tmp.path()));
        let bad = ReplayRecord {
            session_id: 8, // wrong session
            sequence_no: 0,
            monotonic_ns: 0,
            wallclock_utc: 0,
            kind: RecordKind::Tick,
            payload: vec![],
        };
        let err = rec.record_raw(bad).await.unwrap_err();
        match err {
            ReplayError::Config(_) => {}
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_redis_sink_by_default() {
        let tmp = TempDir::new().unwrap();
        let rec = Recorder::new(SessionId::new(1), cfg(tmp.path()));
        assert!(!rec.has_redis_sink());
    }

    #[tokio::test]
    async fn round_trip_via_segment_reader_yields_same_records() {
        let tmp = TempDir::new().unwrap();
        let session = SessionId::new(42);
        let mut rec = Recorder::new(session, cfg(tmp.path()));
        let payloads: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; 4]).collect();
        for p in &payloads {
            rec.record(RecordKind::Tick, p.clone()).await.unwrap();
        }
        rec.flush().unwrap();
        drop(rec);

        let reader = SegmentReader::open_session(tmp.path(), 42).unwrap();
        let all = reader.read_all().unwrap();
        assert_eq!(all.len(), 5);
        for (i, r) in all.iter().enumerate() {
            assert_eq!(r.sequence_no, i as u64);
            assert_eq!(r.session_id, 42);
            assert_eq!(r.payload, payloads[i]);
            assert_eq!(r.kind, RecordKind::Tick);
        }
    }

    #[tokio::test]
    async fn rotation_at_size_threshold_preserves_sequence() {
        let tmp = TempDir::new().unwrap();
        let session = SessionId::new(1);
        // Tiny budget — every record forces rotation.
        let cfg = RecorderConfig {
            segment_dir: tmp.path().to_path_buf(),
            max_segment_bytes: 32,
        };
        let mut rec = Recorder::new(session, cfg);
        for i in 0..6u8 {
            rec.record(RecordKind::Tick, vec![i; 8]).await.unwrap();
        }
        rec.flush().unwrap();
        drop(rec);

        let reader = SegmentReader::open_session(tmp.path(), 1).unwrap();
        // Multiple segments — one per record under this budget.
        assert!(reader.segment_count() >= 2);
        let all = reader.read_all().unwrap();
        let seqs: Vec<u64> = all.iter().map(|r| r.sequence_no).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4, 5]);
    }
}
