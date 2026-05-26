//! Unified error type for the Replay_Engine.
//!
//! The recorder, the player, and the `/replay` command plane all funnel
//! through [`ReplayError`] so call sites only have to deal with one type.
//! Like [`hedge_bus::BusError`], the variants carry enough context to
//! drive structured `obs.error.*` payloads without re-stringifying the
//! whole error chain.

use std::path::PathBuf;

use thiserror::Error;

/// Top-level error returned by every Replay_Engine entry point.
#[derive(Debug, Error)]
pub enum ReplayError {
    /// I/O failure on a segment file (open, write, flush, read, ...).
    #[error("replay segment I/O failed at {path:?}: {source}")]
    SegmentIo {
        /// Path of the segment file the operation targeted.
        path: PathBuf,
        /// Wrapped `std::io::Error`.
        #[source]
        source: std::io::Error,
    },

    /// rkyv decoding of a segment frame or a Redis Stream entry failed.
    #[error("replay rkyv decode failed: {0}")]
    Decode(String),

    /// Underlying Redis Stream error surfaced by `hedge-bus`.
    #[error("replay Redis stream error: {0}")]
    Bus(#[from] hedge_bus::BusError),

    /// Strict-monotonic-gap-free invariant violated by the caller. The
    /// recorder validates `record(record)` against its internal counter
    /// before any disk or Redis write.
    #[error(
        "replay sequence invariant broken: expected sequence_no = {expected}, got {got} \
         (session_id = {session})"
    )]
    SequenceInvariant {
        /// Session in which the violation occurred.
        session: u64,
        /// Sequence number the recorder expected next.
        expected: u64,
        /// Sequence number the caller passed.
        got: u64,
    },

    /// Player was asked to seek to a sequence number that is not
    /// present in the session.
    #[error("replay seek target not found: sequence_no = {target}")]
    SeekNotFound {
        /// The sequence_no the caller asked to seek to.
        target: u64,
    },

    /// `/replay` command plane received a malformed request.
    #[error("replay command malformed: {0}")]
    BadCommand(String),

    /// Configuration error at recorder/player construction time.
    #[error("replay configuration error: {0}")]
    Config(String),
}

impl ReplayError {
    /// Helper for wrapping a `std::io::Error` together with the segment path.
    #[inline]
    pub fn segment_io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::SegmentIo {
            path: path.into(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_io_carries_path_and_source() {
        let e = ReplayError::segment_io(
            "/tmp/seg-0001.rkyv",
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
        );
        let s = format!("{}", e);
        assert!(s.contains("seg-0001.rkyv"));
        assert!(s.contains("denied"));
    }

    #[test]
    fn sequence_invariant_renders_three_pieces() {
        let e = ReplayError::SequenceInvariant {
            session: 20251130,
            expected: 7,
            got: 9,
        };
        let s = format!("{}", e);
        assert!(s.contains("20251130"));
        assert!(s.contains("expected sequence_no = 7"));
        assert!(s.contains("got 9"));
    }

    #[test]
    fn seek_not_found_carries_target() {
        let e = ReplayError::SeekNotFound { target: 42 };
        assert!(format!("{}", e).contains("42"));
    }
}
