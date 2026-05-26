//! `hedge-replay` — Replay_Engine.
//!
//! Implements task **40.1** of the implementation plan: a deterministic
//! recorder + player + UI control plane that backs Property 12 (Replay
//! Determinism, Recording Completeness, and Simulated-Broker Routing).
//!
//! ### Architecture
//!
//! Three orthogonal pieces, all owned by this crate:
//!
//! | Piece           | Type                                | Live?      | Replay?    |
//! |-----------------|-------------------------------------|------------|------------|
//! | [`Recorder`]    | Append-only ledger writer           | Always-on  | No         |
//! | [`Player`]      | Single-threaded scheduler           | No         | Always-on  |
//! | Command plane   | `replay.command.*` NATS subjects    | No         | UI-driven  |
//!
//! And a fourth piece — the [`ReplayMode`] flag — that ties the two
//! together. When set to [`ReplayMode::On`], the Execution_Engine binds
//! to [`hedge_broker_simulated::SimulatedBroker`] rather than a live
//! broker. The Replay_Engine deliberately does NOT link to the broker
//! crate: the wiring is config-driven and lives at the
//! Execution_Engine's startup builder. See [`mode`] for the full
//! contract.
//!
//! ### Disk layout
//!
//! ```text
//! <segment_dir>/
//!     <session_id>/
//!         seg-0001.rkyv
//!         seg-0002.rkyv
//!         ...
//! ```
//!
//! Every segment file is a flat sequence of length-prefixed rkyv
//! archives. Segments roll on session boundary or when the active
//! file's on-disk size + the next record's wire size would exceed
//! [`segments::DEFAULT_MAX_SEGMENT_BYTES`] (1 GiB by default; the
//! threshold is configurable).
//!
//! ### Determinism contract (Property 12)
//!
//! 1. Every recorded event has a strict-monotonic gap-free
//!    `sequence_no` per session ([`Recorder::record`]).
//! 2. The player loads records in `sequence_no` order and validates
//!    monotonicity at open time ([`Player::open`]).
//! 3. Any stochastic component pulls from a single
//!    [`rand_chacha::ChaCha20Rng`] seeded with the configured
//!    `rng_seed`. Two players seeded identically produce identical
//!    RNG streams.
//! 4. The Execution_Engine routes every approval to
//!    `SimulatedBroker` while [`ReplayMode::On`] is set.
//!
//! ### Hot_Path purity (R30)
//!
//! `forbid::FORBIDDEN_DEPENDENCIES` enumerates the prohibited
//! dependency closure (`pyo3`, `numpy`, `pandas`,
//! `reqwest::blocking`, every cloud LLM SDK). The full transitive
//! check ships in CI as `scripts/check-forbidden-deps.sh`; the
//! defensive in-crate `build.rs` aborts compilation if a prohibited
//! Cargo feature is ever turned on.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
pub mod command;
pub mod error;
pub mod forbid;
pub mod mode;
pub mod player;
pub mod record;
pub mod recorder;
pub mod segments;

pub use codec::ReplayRecordCodec;
pub use command::{
    AckResponse, AiSourceWire, CursorResponse, ListSessionsRequest, ListSessionsResponse,
    OpenSessionRequest, OpenSessionResponse, PlayRequest, RecordKindWire, ReplayRecordWire,
    ScrubRequest, StatusResponse, REPLAY_COMMAND_LIST, REPLAY_COMMAND_OPEN, REPLAY_COMMAND_PLAY,
    REPLAY_COMMAND_PREFIX, REPLAY_COMMAND_SCRUB, REPLAY_COMMAND_STATUS, REPLAY_COMMAND_STEP,
};
pub use error::ReplayError;
pub use forbid::FORBIDDEN_DEPENDENCIES;
pub use mode::ReplayMode;
pub use player::{PacedReplay, Player, PlayerConfig};
pub use record::{
    decode_record, encode_record, view_archived, AISource, ArchivedReplayRecord, RecordKind,
    ReplayRecord,
};
pub use recorder::{Recorder, RecorderConfig};
pub use segments::{list_sessions, SegmentReader, SegmentWriter, DEFAULT_MAX_SEGMENT_BYTES};
