//! [`Player`] — single-threaded deterministic replay scheduler.
//!
//! ### What the Player does
//!
//! Given a session id and a recorder-produced segment tree under
//! `<segment_dir>/<session_id>/seg-NNNN.rkyv`, the player:
//!
//! 1. Loads every record into memory in `sequence_no` order. The
//!    [`SegmentReader`] already returns records in segment-then-frame
//!    order, which equals strict-monotonic sequence_no order (R22.1).
//! 2. Holds a position cursor and a [`rand_chacha::ChaCha20Rng`]
//!    seeded with the configured `rng_seed`. The RNG is exposed to
//!    consumers via [`Player::rng_mut`] so any stochastic component
//!    that needs randomness can pull from the same deterministic
//!    stream — re-running the player twice with the same seed
//!    produces the same RNG outputs (R22.2).
//! 3. Releases events one at a time:
//!    - [`Player::step`] returns the next record without pacing.
//!    - [`Player::play`] returns a [`futures::Stream`] that paces
//!      releases according to the configured [`ReplaySpeed`]
//!      (`X1`, `X10`, or `Max` — uncapped).
//!    - [`Player::seek`] sets the cursor to a specific sequence_no.
//!
//! ### Single-threaded by design
//!
//! The Player is `!Sync` in spirit — it is a single-task driver
//! returning each record in turn. The `play` stream is built on the
//! current tokio runtime; tests exercise it on a `current_thread`
//! runtime. This satisfies the design's "single-threaded scheduler
//! that releases events in `sequence_no` order" wording.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::Stream;
use hedge_config::ReplaySpeed;
use hedge_core::SessionId;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use tokio::time::Sleep;

use crate::error::ReplayError;
use crate::record::ReplayRecord;
use crate::segments::SegmentReader;

/// Configuration for the [`Player`].
#[derive(Clone, Debug)]
pub struct PlayerConfig {
    /// Root directory containing `<session_id>/seg-NNNN.rkyv`.
    pub segment_dir: PathBuf,
    /// Default speed when [`Player::play`] is called without an
    /// explicit override.
    pub default_speed: ReplaySpeed,
    /// Seed for the deterministic RNG (R22.2).
    pub rng_seed: u64,
}

impl Default for PlayerConfig {
    fn default() -> Self {
        Self {
            segment_dir: PathBuf::from("./replay"),
            default_speed: ReplaySpeed::X1,
            rng_seed: 0,
        }
    }
}

/// Single-threaded deterministic replay scheduler.
pub struct Player {
    /// Session being replayed.
    session_id: SessionId,
    /// All records, in sequence_no order.
    records: Vec<ReplayRecord>,
    /// Position cursor — index of the next record to release.
    cursor: usize,
    /// Default pacing speed.
    default_speed: ReplaySpeed,
    /// Seeded ChaCha20 RNG for any stochastic component.
    rng: ChaCha20Rng,
}

impl std::fmt::Debug for Player {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // ChaCha20Rng is not Debug, so we summarise the player without
        // dumping its internal state.
        f.debug_struct("Player")
            .field("session_id", &self.session_id.raw())
            .field("records", &self.records.len())
            .field("cursor", &self.cursor)
            .field("default_speed", &self.default_speed)
            .finish()
    }
}

impl Player {
    /// Open the session at `cfg.segment_dir / session_id` and load
    /// every record into memory.
    pub fn open(session_id: SessionId, cfg: PlayerConfig) -> Result<Self, ReplayError> {
        let reader = SegmentReader::open_session(&cfg.segment_dir, session_id.raw())
            .map_err(|e| ReplayError::segment_io(cfg.segment_dir.clone(), e))?;
        let records = reader
            .read_all()
            .map_err(|e| ReplayError::segment_io(cfg.segment_dir.clone(), e))?;
        // The SegmentReader walks files in sorted order and frames in
        // append order, which yields strict-monotonic sequence_no
        // order. We assert that here so a corrupt ledger fails loudly
        // rather than silently producing a non-deterministic replay.
        for (i, r) in records.iter().enumerate() {
            if r.sequence_no != i as u64 {
                return Err(ReplayError::SequenceInvariant {
                    session: session_id.raw(),
                    expected: i as u64,
                    got: r.sequence_no,
                });
            }
        }
        Ok(Self {
            session_id,
            records,
            cursor: 0,
            default_speed: cfg.default_speed,
            rng: ChaCha20Rng::seed_from_u64(cfg.rng_seed),
        })
    }

    /// Construct a player from an in-memory record vector. Used by
    /// tests and by integration code that wants to drive replays
    /// without round-tripping through disk.
    pub fn from_records(
        session_id: SessionId,
        records: Vec<ReplayRecord>,
        default_speed: ReplaySpeed,
        rng_seed: u64,
    ) -> Result<Self, ReplayError> {
        for (i, r) in records.iter().enumerate() {
            if r.sequence_no != i as u64 {
                return Err(ReplayError::SequenceInvariant {
                    session: session_id.raw(),
                    expected: i as u64,
                    got: r.sequence_no,
                });
            }
        }
        Ok(Self {
            session_id,
            records,
            cursor: 0,
            default_speed,
            rng: ChaCha20Rng::seed_from_u64(rng_seed),
        })
    }

    /// Session being replayed.
    #[inline]
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Total number of records in the session.
    #[inline]
    pub fn total_records(&self) -> usize {
        self.records.len()
    }

    /// Position of the next record to release.
    #[inline]
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Whether the cursor has consumed every record.
    #[inline]
    pub fn at_end(&self) -> bool {
        self.cursor >= self.records.len()
    }

    /// Borrow the seeded RNG so a stochastic component can pull
    /// reproducible randomness.
    #[inline]
    pub fn rng_mut(&mut self) -> &mut ChaCha20Rng {
        &mut self.rng
    }

    /// Advance the cursor by one record and return it. Returns `None`
    /// at end-of-session.
    pub fn step(&mut self) -> Option<ReplayRecord> {
        if self.cursor >= self.records.len() {
            return None;
        }
        let r = self.records[self.cursor].clone();
        self.cursor += 1;
        Some(r)
    }

    /// Move the cursor to the record whose `sequence_no == target`.
    ///
    /// Returns [`ReplayError::SeekNotFound`] when the target is
    /// outside the recorded range.
    pub fn seek(&mut self, target: u64) -> Result<(), ReplayError> {
        if target as usize > self.records.len() {
            return Err(ReplayError::SeekNotFound { target });
        }
        // Binary search by sequence_no — guaranteed monotonic by `open`.
        match self
            .records
            .binary_search_by_key(&target, |r| r.sequence_no)
        {
            Ok(idx) => {
                self.cursor = idx;
                Ok(())
            }
            Err(_) if target as usize == self.records.len() => {
                // Seeking to one-past-end is allowed and means "park
                // at end".
                self.cursor = self.records.len();
                Ok(())
            }
            Err(_) => Err(ReplayError::SeekNotFound { target }),
        }
    }

    /// Move the cursor to the start of the session.
    pub fn rewind(&mut self) {
        self.cursor = 0;
    }

    /// Return a [`Stream`] that releases records from the current
    /// cursor position onwards, paced according to `speed`.
    pub fn play(&mut self, speed: ReplaySpeed) -> PacedReplay<'_> {
        PacedReplay {
            player: self,
            speed,
            sleep: None,
            last_monotonic_ns: None,
        }
    }

    /// [`Player::play`] with the [`PlayerConfig::default_speed`].
    pub fn play_default(&mut self) -> PacedReplay<'_> {
        let speed = self.default_speed;
        self.play(speed)
    }
}

/// Stream returned by [`Player::play`].
///
/// Yields each [`ReplayRecord`] from the cursor to the end, with the
/// inter-record delay derived from the recorded `monotonic_ns` deltas
/// divided by [`ReplaySpeed::divisor`]. `ReplaySpeed::Max` releases
/// records back-to-back with no pacing.
pub struct PacedReplay<'a> {
    player: &'a mut Player,
    speed: ReplaySpeed,
    sleep: Option<Pin<Box<Sleep>>>,
    last_monotonic_ns: Option<u64>,
}

impl<'a> Stream for PacedReplay<'a> {
    type Item = ReplayRecord;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // If we're parked on a sleep, drain it first.
        if let Some(s) = self.sleep.as_mut() {
            match s.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.sleep = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Read the next record from the player.
        let next = match self.player.step() {
            Some(r) => r,
            None => return Poll::Ready(None),
        };

        // Compute the pacing delay relative to the previous record.
        let delay = match (self.speed.divisor(), self.last_monotonic_ns) {
            (None, _) => Duration::ZERO, // Max: no pacing
            (Some(_), None) => Duration::ZERO, // first record fires immediately
            (Some(div), Some(prev)) if next.monotonic_ns > prev => {
                // Saturating-protected division. `div` is a u32 and
                // safe to cast to u64.
                let delta_ns = next.monotonic_ns - prev;
                Duration::from_nanos(delta_ns / div as u64)
            }
            (Some(_), Some(_)) => Duration::ZERO,
        };
        self.last_monotonic_ns = Some(next.monotonic_ns);

        if delay.is_zero() {
            return Poll::Ready(Some(next));
        }

        // Schedule the sleep but return the record now: tests and
        // downstream consumers want each `poll_next` to make forward
        // progress; the pacing applies *between* releases. We park on
        // the sleep on the next poll instead. This is achieved by
        // creating the sleep, immediately returning the record, and
        // letting the next call to `poll_next` drain it before
        // pulling the next record.
        //
        // Note: this is the standard tokio-stream interleave pattern.
        // Releasing the i-th record then sleeping before the (i+1)-th
        // release matches the "release events at speed" wording in
        // the design.
        let sleep = Box::pin(tokio::time::sleep(delay));
        self.sleep = Some(sleep);
        Poll::Ready(Some(next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{RecordKind, ReplayRecord};
    use futures::StreamExt;
    use rand::RngCore;

    fn rec_at(seq: u64, ns: u64) -> ReplayRecord {
        ReplayRecord {
            session_id: 1,
            sequence_no: seq,
            monotonic_ns: ns,
            wallclock_utc: 0,
            kind: RecordKind::Tick,
            payload: vec![seq as u8],
        }
    }

    fn records(n: u64) -> Vec<ReplayRecord> {
        (0..n).map(|i| rec_at(i, i * 1_000_000)).collect()
    }

    #[tokio::test]
    async fn step_returns_records_in_sequence_order() {
        let mut p =
            Player::from_records(SessionId::new(1), records(3), ReplaySpeed::Max, 0).unwrap();
        assert_eq!(p.step().unwrap().sequence_no, 0);
        assert_eq!(p.step().unwrap().sequence_no, 1);
        assert_eq!(p.step().unwrap().sequence_no, 2);
        assert!(p.step().is_none());
        assert!(p.at_end());
    }

    #[tokio::test]
    async fn seek_moves_cursor_to_sequence_no() {
        let mut p =
            Player::from_records(SessionId::new(1), records(5), ReplaySpeed::Max, 0).unwrap();
        p.seek(3).unwrap();
        assert_eq!(p.step().unwrap().sequence_no, 3);
    }

    #[tokio::test]
    async fn seek_to_end_parks_cursor() {
        let mut p =
            Player::from_records(SessionId::new(1), records(3), ReplaySpeed::Max, 0).unwrap();
        p.seek(3).unwrap();
        assert!(p.at_end());
    }

    #[tokio::test]
    async fn seek_out_of_range_errors() {
        let mut p =
            Player::from_records(SessionId::new(1), records(2), ReplaySpeed::Max, 0).unwrap();
        let err = p.seek(99).unwrap_err();
        match err {
            ReplayError::SeekNotFound { target: 99 } => {}
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn rewind_resets_cursor() {
        let mut p =
            Player::from_records(SessionId::new(1), records(3), ReplaySpeed::Max, 0).unwrap();
        p.step();
        p.step();
        assert_eq!(p.cursor(), 2);
        p.rewind();
        assert_eq!(p.cursor(), 0);
    }

    #[tokio::test]
    async fn open_rejects_non_monotonic_records() {
        let bad = vec![
            rec_at(0, 0),
            rec_at(2, 0), // gap
        ];
        let res = Player::from_records(SessionId::new(1), bad, ReplaySpeed::Max, 0);
        match res {
            Err(ReplayError::SequenceInvariant {
                expected: 1,
                got: 2,
                ..
            }) => {}
            Err(other) => panic!("wrong error: {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn play_stream_drains_all_records_under_max() {
        let mut p =
            Player::from_records(SessionId::new(1), records(4), ReplaySpeed::Max, 0).unwrap();
        let stream = p.play(ReplaySpeed::Max);
        let collected: Vec<_> = stream.collect().await;
        let seqs: Vec<u64> = collected.iter().map(|r| r.sequence_no).collect();
        assert_eq!(seqs, vec![0, 1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn play_stream_x1_paces_by_monotonic_delta() {
        // Build records 1ms apart in monotonic time. Under X1, the
        // stream should take ~3ms total to deliver four records.
        let recs: Vec<_> = (0..4u64)
            .map(|i| rec_at(i, i * 1_000_000))
            .collect();
        let mut p =
            Player::from_records(SessionId::new(1), recs, ReplaySpeed::X1, 0).unwrap();

        let start = tokio::time::Instant::now();
        let mut stream = p.play(ReplaySpeed::X1);
        let mut count = 0;
        while let Some(_r) = stream.next().await {
            count += 1;
        }
        assert_eq!(count, 4);
        let elapsed = tokio::time::Instant::now() - start;
        // With paused time + auto-advance the elapsed is at least the
        // sum of three 1-ms gaps. The exact value is not
        // deterministic, but >= 3 ms is.
        assert!(
            elapsed >= Duration::from_millis(3),
            "elapsed = {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn rng_seed_is_deterministic_across_runs() {
        // Property 12: replay determinism. Two players seeded with the
        // same value must produce the same RNG stream.
        let mut p1 =
            Player::from_records(SessionId::new(1), records(0), ReplaySpeed::Max, 0xCAFE).unwrap();
        let mut p2 =
            Player::from_records(SessionId::new(1), records(0), ReplaySpeed::Max, 0xCAFE).unwrap();
        for _ in 0..32 {
            assert_eq!(p1.rng_mut().next_u64(), p2.rng_mut().next_u64());
        }
    }

    #[tokio::test]
    async fn rng_with_different_seeds_diverges() {
        let mut p1 =
            Player::from_records(SessionId::new(1), records(0), ReplaySpeed::Max, 1).unwrap();
        let mut p2 =
            Player::from_records(SessionId::new(1), records(0), ReplaySpeed::Max, 2).unwrap();
        // ChaCha20 is well-mixed: at least one of the first 16 outputs
        // must differ.
        let differs = (0..16).any(|_| p1.rng_mut().next_u64() != p2.rng_mut().next_u64());
        assert!(differs, "RNG streams should diverge across seeds");
    }
}
