//! Disk-segment writer and reader for the replay ledger.
//!
//! ### Layout
//!
//! The recorder lays out the directory tree as:
//!
//! ```text
//! <segment_dir>/
//!     <session_id>/
//!         seg-0001.rkyv
//!         seg-0002.rkyv
//!         ...
//! ```
//!
//! * One directory per `session_id` (R22.1 — recordings are
//!   session-scoped).
//! * Files are zero-padded four-digit segment indices so a `ls -1`
//!   listing reads in chronological order.
//! * Every segment file is a flat sequence of length-prefixed rkyv
//!   archives produced by [`crate::record::framed::write_framed`].
//!
//! ### Rotation
//!
//! [`SegmentWriter::append`] rolls a fresh segment when either:
//!
//! 1. The active segment's on-disk size + the next record's wire size
//!    would exceed `max_segment_bytes` (default 1 GiB).
//! 2. The session id of the next record differs from the active one —
//!    a session boundary forces a new directory.
//!
//! Rotation is synchronous: the previous file is `flush()`'d and closed
//! before the new one is opened. This is safe because the recorder
//! task already serialises calls to `append` (it's the single producer
//! into the segment file).
//!
//! ### Reader
//!
//! [`SegmentReader`] walks the segment files in sorted order and
//! yields owned [`ReplayRecord`]s on demand. The
//! [`SegmentReader::iter_session`] helper returns every record in a
//! single session as a flat stream.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Result as IoResult, Write};
use std::path::{Path, PathBuf};

use crate::record::{decode_record, encode_record, framed, ReplayRecord};

/// Default maximum segment size — 1 GiB, matching the spec brief
/// for task 40.1.
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 1_073_741_824;

/// Append-only segment writer.
///
/// Construct with [`SegmentWriter::new`] and call [`SegmentWriter::append`]
/// for each [`ReplayRecord`]. Rotation happens automatically.
pub struct SegmentWriter {
    /// Root directory for every session (e.g. `./replay`).
    base_dir: PathBuf,
    /// Maximum bytes per segment file before rotation.
    max_segment_bytes: u64,
    /// Active session id, `None` until the first `append` call.
    active_session: Option<u64>,
    /// Active segment index within the active session (1-based).
    active_segment_idx: u32,
    /// Active file handle, wrapped in `BufWriter` to amortise write
    /// syscalls over many records.
    active_file: Option<BufWriter<File>>,
    /// On-disk byte count of the active segment (records written so
    /// far). Used to drive size-based rotation.
    active_bytes: u64,
}

impl SegmentWriter {
    /// Construct a new writer rooted at `base_dir`.
    ///
    /// The directory is **not** created eagerly — it is created on
    /// the first call to [`SegmentWriter::append`] when the session
    /// directory is opened. This avoids leaving stub directories on
    /// disk for tests that build a writer but never write.
    pub fn new<P: Into<PathBuf>>(base_dir: P, max_segment_bytes: u64) -> Self {
        Self {
            base_dir: base_dir.into(),
            max_segment_bytes,
            active_session: None,
            active_segment_idx: 0,
            active_file: None,
            active_bytes: 0,
        }
    }

    /// Root directory.
    #[inline]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Active session id, if any.
    #[inline]
    pub fn active_session(&self) -> Option<u64> {
        self.active_session
    }

    /// Active segment index (1-based). `0` when nothing has been
    /// written yet for the current session.
    #[inline]
    pub fn active_segment_idx(&self) -> u32 {
        self.active_segment_idx
    }

    /// Append `record` to the active segment, rotating first if
    /// necessary.
    ///
    /// Returns the path of the segment file the record was written
    /// to. Used by tests and observability dashboards.
    pub fn append(&mut self, record: &ReplayRecord) -> IoResult<PathBuf> {
        let archive = encode_record(record);
        // 4 bytes of length prefix + the rkyv archive itself.
        let frame_size = 4u64 + archive.len() as u64;

        // 1. Session boundary triggers a brand-new directory + segment 1.
        let session_changed = self.active_session != Some(record.session_id);
        // 2. Size budget triggers a fresh segment within the same
        //    session. We compute against the *upcoming* frame so the
        //    decision is "would this record overflow?", not "did the
        //    previous record overflow?".
        let size_overflow = self
            .active_file
            .is_some()
            && self.active_bytes.saturating_add(frame_size) > self.max_segment_bytes;

        if session_changed {
            self.close_active()?;
            self.active_session = Some(record.session_id);
            self.active_segment_idx = 0;
            self.open_next_segment()?;
        } else if size_overflow {
            self.close_active()?;
            self.open_next_segment()?;
        } else if self.active_file.is_none() {
            // First append for this session.
            self.active_segment_idx = 0;
            self.open_next_segment()?;
        }

        let path = self.active_path();
        let writer = self
            .active_file
            .as_mut()
            .expect("open_next_segment leaves the file open");
        framed::write_framed(writer, &archive)?;
        self.active_bytes = self.active_bytes.saturating_add(frame_size);
        Ok(path)
    }

    /// Flush the active segment file. Idempotent.
    pub fn flush(&mut self) -> IoResult<()> {
        if let Some(w) = self.active_file.as_mut() {
            w.flush()?;
            w.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Close the active segment if any. Called automatically on
    /// rotation and on `Drop`.
    fn close_active(&mut self) -> IoResult<()> {
        if let Some(mut w) = self.active_file.take() {
            w.flush()?;
            w.get_ref().sync_data()?;
        }
        self.active_bytes = 0;
        Ok(())
    }

    /// Build the segment-directory path for the active session.
    fn session_dir(&self) -> PathBuf {
        let session = self
            .active_session
            .expect("session_dir called without an active session");
        self.base_dir.join(session.to_string())
    }

    /// Build the file path for the active segment index.
    fn active_path(&self) -> PathBuf {
        self.session_dir()
            .join(format!("seg-{:04}.rkyv", self.active_segment_idx))
    }

    /// Advance to the next segment index and open the file fresh.
    fn open_next_segment(&mut self) -> IoResult<()> {
        self.active_segment_idx = self.active_segment_idx.saturating_add(1);
        let dir = self.session_dir();
        fs::create_dir_all(dir)?;
        let path = self.active_path();
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;
        self.active_file = Some(BufWriter::new(file));
        self.active_bytes = 0;
        Ok(())
    }
}

impl Drop for SegmentWriter {
    fn drop(&mut self) {
        // Best-effort flush on drop. We swallow errors because the
        // recorder's drop path runs during shutdown and must not panic.
        let _ = self.close_active();
    }
}

/// Read every segment of one session in `sequence_no` order.
pub struct SegmentReader {
    /// Sorted segment paths.
    segments: Vec<PathBuf>,
    /// Active reader, lazily opened on first record.
    active: Option<BufReader<File>>,
    /// Index of the active segment within `segments`.
    active_idx: usize,
}

impl SegmentReader {
    /// Open every `seg-NNNN.rkyv` file for `session_id` under
    /// `base_dir`. Returns an empty reader if the directory does not
    /// exist (the player treats that as "no records").
    pub fn open_session<P: AsRef<Path>>(base_dir: P, session_id: u64) -> IoResult<Self> {
        let dir = base_dir.as_ref().join(session_id.to_string());
        let mut paths = Vec::new();
        if dir.exists() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("seg-") && n.ends_with(".rkyv"))
                    .unwrap_or(false)
                {
                    paths.push(path);
                }
            }
            paths.sort();
        }
        Ok(Self {
            segments: paths,
            active: None,
            active_idx: 0,
        })
    }

    /// Number of segment files for this session.
    #[inline]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Read the next record. Returns `Ok(None)` when every segment
    /// has been drained.
    pub fn next_record(&mut self) -> IoResult<Option<ReplayRecord>> {
        loop {
            if self.active.is_none() {
                if self.active_idx >= self.segments.len() {
                    return Ok(None);
                }
                let file = File::open(&self.segments[self.active_idx])?;
                self.active = Some(BufReader::new(file));
            }
            let r = self
                .active
                .as_mut()
                .expect("active reader open in next_record");
            match framed::read_framed(r)? {
                Some(bytes) => {
                    if let Some(rec) = decode_record(&bytes) {
                        return Ok(Some(rec));
                    } else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "corrupt rkyv frame in segment",
                        ));
                    }
                }
                None => {
                    // Segment exhausted — advance.
                    self.active = None;
                    self.active_idx += 1;
                }
            }
        }
    }

    /// Drain the entire session into a vector. Convenience helper
    /// for tests and the player's bootstrap step.
    pub fn read_all(mut self) -> IoResult<Vec<ReplayRecord>> {
        let mut out = Vec::new();
        while let Some(r) = self.next_record()? {
            out.push(r);
        }
        Ok(out)
    }
}

/// Discover every recorded `session_id` under `base_dir`.
///
/// Returns the list in ascending numeric order so the UI's
/// `trader.intent.replay.list` response is stable across calls.
pub fn list_sessions<P: AsRef<Path>>(base_dir: P) -> IoResult<Vec<u64>> {
    let dir = base_dir.as_ref();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(id) = name.parse::<u64>() {
                out.push(id);
            }
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{RecordKind, ReplayRecord};
    use tempfile::TempDir;

    fn rec(session: u64, seq: u64, payload_len: usize) -> ReplayRecord {
        ReplayRecord {
            session_id: session,
            sequence_no: seq,
            monotonic_ns: seq * 1_000_000,
            wallclock_utc: 1_700_000_000_000_000_000 + seq as i64,
            kind: RecordKind::Tick,
            payload: vec![0xAB; payload_len],
        }
    }

    #[test]
    fn writer_creates_session_dir_lazily() {
        let tmp = TempDir::new().unwrap();
        // Construct the writer but do not append anything.
        let w = SegmentWriter::new(tmp.path(), DEFAULT_MAX_SEGMENT_BYTES);
        let _ = w; // drop without writing
        // No subdirectory should have been created.
        let entries: Vec<_> = fs::read_dir(tmp.path()).unwrap().collect();
        assert!(entries.is_empty(), "writer should not create dirs eagerly");
    }

    #[test]
    fn append_creates_seg_0001_and_writes_record() {
        let tmp = TempDir::new().unwrap();
        let mut w = SegmentWriter::new(tmp.path(), DEFAULT_MAX_SEGMENT_BYTES);
        let r = rec(42, 0, 8);
        let path = w.append(&r).unwrap();
        w.flush().unwrap();
        assert!(path.ends_with("seg-0001.rkyv"));
        // Path layout matches the docstring: <base>/<session>/<seg>.
        let session_dir = tmp.path().join("42");
        assert!(session_dir.is_dir());
        assert!(session_dir.join("seg-0001.rkyv").is_file());
    }

    #[test]
    fn rotates_at_size_threshold() {
        let tmp = TempDir::new().unwrap();
        // Tiny budget — every record should land in its own segment.
        let mut w = SegmentWriter::new(tmp.path(), 32);
        for i in 0..3u64 {
            let r = rec(7, i, 16);
            let path = w.append(&r).unwrap();
            assert!(path.ends_with(&format!("seg-{:04}.rkyv", i + 1)));
        }
        w.flush().unwrap();
        let session_dir = tmp.path().join("7");
        let mut names: Vec<_> = fs::read_dir(&session_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "seg-0001.rkyv".to_string(),
                "seg-0002.rkyv".to_string(),
                "seg-0003.rkyv".to_string()
            ]
        );
    }

    #[test]
    fn session_change_starts_fresh_dir_and_segment() {
        let tmp = TempDir::new().unwrap();
        let mut w = SegmentWriter::new(tmp.path(), DEFAULT_MAX_SEGMENT_BYTES);
        let p1 = w.append(&rec(100, 0, 4)).unwrap();
        let p2 = w.append(&rec(101, 0, 4)).unwrap();
        w.flush().unwrap();
        assert_ne!(p1.parent(), p2.parent());
        assert!(p1.ends_with("seg-0001.rkyv"));
        assert!(p2.ends_with("seg-0001.rkyv"));
        // Both session dirs should exist.
        assert!(tmp.path().join("100").is_dir());
        assert!(tmp.path().join("101").is_dir());
    }

    #[test]
    fn reader_returns_records_in_order_across_segments() {
        let tmp = TempDir::new().unwrap();
        // Force every record into its own segment.
        let mut w = SegmentWriter::new(tmp.path(), 16);
        for i in 0..5u64 {
            w.append(&rec(9, i, 4)).unwrap();
        }
        w.flush().unwrap();

        let r = SegmentReader::open_session(tmp.path(), 9).unwrap();
        assert_eq!(r.segment_count(), 5);
        let all = r.read_all().unwrap();
        let seqs: Vec<u64> = all.iter().map(|r| r.sequence_no).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn reader_for_missing_session_yields_no_records() {
        let tmp = TempDir::new().unwrap();
        let r = SegmentReader::open_session(tmp.path(), 555).unwrap();
        assert_eq!(r.segment_count(), 0);
        let all = r.read_all().unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn list_sessions_returns_sorted_session_ids() {
        let tmp = TempDir::new().unwrap();
        let mut w = SegmentWriter::new(tmp.path(), DEFAULT_MAX_SEGMENT_BYTES);
        for s in [200u64, 100, 300] {
            w.append(&rec(s, 0, 4)).unwrap();
        }
        w.flush().unwrap();
        let sessions = list_sessions(tmp.path()).unwrap();
        assert_eq!(sessions, vec![100, 200, 300]);
    }

    #[test]
    fn list_sessions_empty_when_dir_missing() {
        let tmp = TempDir::new().unwrap();
        let nonexistent = tmp.path().join("nope");
        let s = list_sessions(&nonexistent).unwrap();
        assert!(s.is_empty());
    }
}
