//! `hedge-bus` — typed transport wrappers for PROJECT HEDGE.
//!
//! This crate is task **3.1** of the implementation plan. It wraps the two
//! transport substrates the Hot_Path uses:
//!
//! * **NATS** (`async_nats` 0.x) — the *primary* inter-service bus
//!   (R29.2). Every subject domain in the design's [NATS Subject Naming
//!   Convention](../../.kiro/specs/project-hedge/design.md) is exposed as a
//!   typed [`Subject<T>`] newtype so a `Subject<Tick>` cannot be accidentally
//!   passed where a `Subject<RiskApproval>` is expected.
//! * **Redis Streams** (`redis::aio::ConnectionManager`) — used where
//!   ordered, persistent intra-Hot_Path delivery is required (R29.3). The
//!   four streams `hedge.hot.signals`, `hedge.hot.approvals`, `hedge.hot.fills`,
//!   and `hedge.hot.replay_record` are exposed via [`RedisStreamProducer`] and
//!   [`RedisStreamConsumer`] with consumer-group ack semantics.
//!
//! ### Zero-copy receive path
//!
//! NATS payloads are delivered as [`bytes::Bytes`], which is a refcounted
//! handle to the wire buffer. Calling [`NatsSubscriber::recv_bytes`] returns
//! that `Bytes` directly — FlatBuffers verifiers and accessors take
//! `&[u8]` and read in place without an intermediate `Vec` allocation
//! (R1.5). The typed [`NatsSubscriber::recv`] entry point then funnels the
//! same `Bytes` through a [`Codec`] without copying when the codec is
//! [`FlatBuffersCodec`].
//!
//! ### Forbidden modules (R30.6, R30.7, R30.8)
//!
//! The Hot_Path is prohibited from depending on `pyo3`, `numpy`, `pandas`,
//! `reqwest::blocking`, or any cloud LLM SDK. The full transitive-closure
//! check is implemented as a CI gate in task 8.1; this crate ships:
//!
//! 1. [`crate::forbid::FORBIDDEN_DEPENDENCIES`] — the canonical list the CI
//!    script reads.
//! 2. A `build.rs` that aborts compilation if a prohibited Cargo feature
//!    flag is ever turned on (defensive local check).
//!
//! ### Tracing
//!
//! Every publish/subscribe entry point is annotated with
//! `#[tracing::instrument]`. Spans surface the subject/stream name and the
//! payload byte length so structured logs flow into Loki via task 5.1.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
pub mod error;
pub mod forbid;
pub mod nats;
pub mod redis_stream;
pub mod subject;
pub mod symbol_table;

// ---- Public re-exports --------------------------------------------------

pub use codec::{Codec, FlatBuffersCodec, JsonCodec, RawBytes};
pub use error::BusError;
pub use forbid::FORBIDDEN_DEPENDENCIES;
pub use nats::{NatsClient, NatsPublisher, NatsSubscriber};
pub use symbol_table::{symbol_for_id, symbol_id_for};
pub use redis_stream::{
    RedisStreamConsumer, RedisStreamProducer, StreamEntry, STREAM_HOT_APPROVALS,
    STREAM_HOT_FILLS, STREAM_HOT_REPLAY_RECORD, STREAM_HOT_SIGNALS,
};
pub use subject::{
    subjects, Subject, SubjectDomain, AI_GOV_ACTION, AI_JOURNAL_ENTRY, AI_NEWS_IMPACT,
    AI_OLLAMA_DEGRADED, AI_PRIORITY_CHANGED, AI_PSYCH_INTERVENTION, AI_PSYCH_STABILITY, AI_RANK,
    AI_REGIME_CHANGED, EXEC_BROKER_FAILOVER, EXEC_FILL, EXEC_ORDER, EXEC_TRADE_CLOSED,
    FEAT_UPDATE, MD_BOOK, MD_BREADTH_SECTOR, MD_BREADTH_VOL, MD_CONNECTION, MD_OI, MD_TICK,
    MEM_PREV_DAY, OBS_BUDGET_BREACH, OBS_ERROR, OBS_LATENCY, OF_EVENT, OF_HEATMAP, OPS_ACTION,
    OPS_SESSION_END, OPS_SESSION_START, OPS_WARMODE_END, OPS_WARMODE_START, POS_RISK_STATE,
    POS_UPDATE, RISK_COOLDOWN, RISK_DECISION_APPROVED, RISK_DECISION_REJECTED,
    RISK_KILLSWITCH_ACTIVATED, RISK_TARGET_REACHED, SIG_EMITTED, TRADER_INTENT_KILLSWITCH,
    TRADER_INTENT_ORDER, TRADER_INTENT_PRIORITY, TRADER_INTENT_STRATEGY_TOGGLE,
};
