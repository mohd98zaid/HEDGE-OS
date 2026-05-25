//! Typed Redis Streams producer/consumer over `redis::aio::ConnectionManager`.
//!
//! Implements the four hot streams from the design's
//! **Redis_Streams Usage** table:
//!
//! | Stream | Producer | Consumer |
//! |--------|----------|----------|
//! | [`STREAM_HOT_SIGNALS`]        | Signal_Engine     | Risk_Engine        |
//! | [`STREAM_HOT_APPROVALS`]      | Risk_Engine       | Execution_Engine   |
//! | [`STREAM_HOT_FILLS`]          | Execution_Engine  | Position_Engine    |
//! | [`STREAM_HOT_REPLAY_RECORD`]  | Replay_Recorder   | (sink, no consumer) |
//!
//! Each typed pair is built around three Redis commands:
//!
//! * `XADD <stream> * payload <bytes>` — append a payload entry.
//! * `XREADGROUP GROUP <group> <consumer> COUNT <n> BLOCK <ms> STREAMS <stream> >`
//!   — read new entries assigned to this consumer.
//! * `XACK <stream> <group> <entry_id>` — acknowledge a processed entry.
//!
//! The single field name on every entry is `payload`. We keep a single field
//! deliberately: the design's intent is "carry one FlatBuffers / JSON
//! payload per entry", and a fixed schema simplifies the consumer.

use bytes::Bytes;
use redis::aio::ConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisResult};
use tracing::instrument;

use crate::codec::Codec;
use crate::error::BusError;

// ---- Stream key constants ----------------------------------------------

/// `hedge.hot.signals` — Signal_Engine → Risk_Engine ordered queue.
pub const STREAM_HOT_SIGNALS: &str = "hedge.hot.signals";

/// `hedge.hot.approvals` — Risk_Engine → Execution_Engine ordered approvals.
pub const STREAM_HOT_APPROVALS: &str = "hedge.hot.approvals";

/// `hedge.hot.fills` — Execution_Engine → Position_Engine ordered fills.
pub const STREAM_HOT_FILLS: &str = "hedge.hot.fills";

/// `hedge.hot.replay_record` — append-only ledger backing the replay log.
pub const STREAM_HOT_REPLAY_RECORD: &str = "hedge.hot.replay_record";

/// The fixed field name carrying the encoded payload bytes on every entry.
///
/// Exposed for callers that need to interact with the raw stream
/// (e.g. observability dashboards) but the typed wrappers below reference it
/// internally.
pub const PAYLOAD_FIELD: &str = "payload";

// ---- StreamEntry -------------------------------------------------------

/// A single entry read from a Redis Stream, paired with its decoded payload.
///
/// `id` is the Redis-assigned entry id (`<unix_ms>-<seq>`) and is the same id
/// the caller passes to [`RedisStreamConsumer::ack`] once the entry has been
/// processed. Holding the id explicitly is what makes the at-least-once /
/// exactly-once-after-ack guarantee operable on the Risk_Engine and
/// Execution_Engine paths.
#[derive(Clone, Debug)]
pub struct StreamEntry<T> {
    /// Redis-assigned entry id.
    pub id: String,
    /// Decoded payload.
    pub payload: T,
}

// ---- RedisStreamProducer<T> --------------------------------------------

/// Typed producer for a Redis Stream.
///
/// Holds a clone of the shared `ConnectionManager` (refcounted, cheap),
/// the stream key, and the [`Codec<T>`] used to encode each payload before
/// `XADD`.
pub struct RedisStreamProducer<T, C>
where
    C: Codec<T>,
{
    conn: ConnectionManager,
    stream: &'static str,
    codec: C,
    _marker: std::marker::PhantomData<fn() -> T>,
}

// Manual `Clone` so the bound on `T` is none (only `C: Codec<T> + Clone`).
impl<T, C> Clone for RedisStreamProducer<T, C>
where
    C: Codec<T> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            stream: self.stream,
            codec: self.codec.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, C> RedisStreamProducer<T, C>
where
    C: Codec<T>,
{
    /// Construct a producer for `stream` (one of the `STREAM_HOT_*` constants).
    #[inline]
    pub fn new(conn: ConnectionManager, stream: &'static str, codec: C) -> Self {
        Self {
            conn,
            stream,
            codec,
            _marker: std::marker::PhantomData,
        }
    }

    /// The stream key this producer writes to.
    #[inline]
    pub fn stream(&self) -> &'static str {
        self.stream
    }

    /// `XADD <stream> * payload <encoded>` — append one entry.
    ///
    /// Returns the Redis-assigned entry id on success.
    #[instrument(
        level = "trace",
        skip(self, value),
        fields(redis.stream = self.stream, payload.bytes)
    )]
    pub async fn xadd(&mut self, value: &T) -> Result<String, BusError> {
        let payload = self.codec.encode(value)?;
        tracing::Span::current().record("payload.bytes", payload.len() as u64);
        self.xadd_bytes(payload).await
    }

    /// `XADD <stream> * payload <bytes>` — append a pre-encoded payload.
    #[instrument(
        level = "trace",
        skip(self, payload),
        fields(redis.stream = self.stream, payload.bytes = payload.len() as u64)
    )]
    pub async fn xadd_bytes(&mut self, payload: Bytes) -> Result<String, BusError> {
        let id: RedisResult<String> = self
            .conn
            .xadd(self.stream, "*", &[(PAYLOAD_FIELD, payload.as_ref())])
            .await;
        id.map_err(|e| BusError::redis(self.stream, e))
    }
}

// ---- RedisStreamConsumer<T> --------------------------------------------

/// Typed consumer-group reader for a Redis Stream.
///
/// `XREADGROUP GROUP <group> <consumer> COUNT <max> BLOCK <ms> STREAMS <stream> >`
/// is the workhorse: each call to [`next_batch`](Self::next_batch) reads
/// up to `max_count` entries and returns them with their ids preserved.
/// Callers ack each id individually via [`ack`](Self::ack), giving the
/// design's "consumer-group ack so a Risk_Engine restart does not drop
/// in-flight signals" semantic.
pub struct RedisStreamConsumer<T, C>
where
    C: Codec<T>,
{
    conn: ConnectionManager,
    stream: &'static str,
    group: String,
    consumer: String,
    block_ms: usize,
    codec: C,
    _marker: std::marker::PhantomData<fn() -> T>,
}

// Manual `Clone` so the bound on `T` is none (only `C: Codec<T> + Clone`).
impl<T, C> Clone for RedisStreamConsumer<T, C>
where
    C: Codec<T> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            stream: self.stream,
            group: self.group.clone(),
            consumer: self.consumer.clone(),
            block_ms: self.block_ms,
            codec: self.codec.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, C> RedisStreamConsumer<T, C>
where
    C: Codec<T>,
{
    /// Construct a consumer.
    ///
    /// * `stream` — one of the `STREAM_HOT_*` constants.
    /// * `group` — the consumer-group name (e.g. `"risk_engine"`).
    /// * `consumer` — a unique consumer id within the group (typically
    ///   `format!("{}-{}", hostname, pid)`).
    /// * `block_ms` — how long `XREADGROUP` blocks waiting for new entries
    ///   on each call. `0` means block indefinitely.
    pub fn new(
        conn: ConnectionManager,
        stream: &'static str,
        group: impl Into<String>,
        consumer: impl Into<String>,
        block_ms: usize,
        codec: C,
    ) -> Self {
        Self {
            conn,
            stream,
            group: group.into(),
            consumer: consumer.into(),
            block_ms,
            codec,
            _marker: std::marker::PhantomData,
        }
    }

    /// Ensure the consumer group exists.
    ///
    /// Calls `XGROUP CREATE <stream> <group> $ MKSTREAM`. If the group
    /// already exists, the resulting `BUSYGROUP` error is swallowed so the
    /// helper is idempotent.
    #[instrument(
        level = "info",
        skip(self),
        fields(redis.stream = self.stream, redis.group = %self.group)
    )]
    pub async fn ensure_group(&mut self) -> Result<(), BusError> {
        // `xgroup_create_mkstream` is the redis-rs convenience that issues
        // `XGROUP CREATE ... MKSTREAM`. If the group already exists the
        // server replies with `BUSYGROUP`, which we treat as success.
        let res: RedisResult<()> = self
            .conn
            .xgroup_create_mkstream(self.stream, self.group.as_str(), "$")
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("BUSYGROUP") {
                    Ok(())
                } else {
                    Err(BusError::redis(self.stream, e))
                }
            }
        }
    }

    /// `XREADGROUP ... COUNT max_count BLOCK <block_ms> STREAMS <stream> >`
    ///
    /// Returns up to `max_count` newly delivered entries (those not yet
    /// delivered to any consumer in the group). Each [`StreamEntry`]
    /// carries its Redis id; the caller acks via [`ack`](Self::ack) once
    /// processing is complete.
    #[instrument(
        level = "trace",
        skip(self),
        fields(
            redis.stream = self.stream,
            redis.group = %self.group,
            redis.consumer = %self.consumer,
            max_count
        )
    )]
    pub async fn next_batch(
        &mut self,
        max_count: usize,
    ) -> Result<Vec<StreamEntry<T>>, BusError> {
        let opts = StreamReadOptions::default()
            .count(max_count)
            .block(self.block_ms)
            .group(self.group.as_str(), self.consumer.as_str());

        let reply: RedisResult<StreamReadReply> = self
            .conn
            .xread_options(&[self.stream], &[">"], &opts)
            .await;

        let reply = reply.map_err(|e| BusError::redis(self.stream, e))?;

        let mut out = Vec::with_capacity(max_count.min(64));
        for stream_key in reply.keys {
            for entry in stream_key.ids {
                // Use `from_redis_value::<Vec<u8>>` so we're insulated from
                // `redis::Value` variant-name changes between minor releases
                // (e.g. `Data` → `BulkString` between 0.23 and 0.24). The
                // `redis::Value` impl of `FromRedisValue<Vec<u8>>` accepts
                // both bulk-string and simple-string forms.
                let raw = entry.map.get(PAYLOAD_FIELD).ok_or_else(|| {
                    BusError::MalformedEntry {
                        id: entry.id.clone(),
                        reason: "missing `payload` field",
                    }
                })?;
                let payload_bytes: Vec<u8> = redis::from_redis_value(raw).map_err(|_| {
                    BusError::MalformedEntry {
                        id: entry.id.clone(),
                        reason: "non-binary `payload` field",
                    }
                })?;
                let payload = self.codec.decode(&payload_bytes)?;
                out.push(StreamEntry {
                    id: entry.id,
                    payload,
                });
            }
        }
        Ok(out)
    }

    /// `XACK <stream> <group> <entry_id>` — acknowledge an entry.
    ///
    /// `XACK` returns the count of acked entries; we ignore the count
    /// because acking an already-acked or never-pending id is a no-op
    /// rather than an error per Redis semantics.
    #[instrument(
        level = "trace",
        skip(self),
        fields(
            redis.stream = self.stream,
            redis.group = %self.group,
            redis.entry_id = entry_id
        )
    )]
    pub async fn ack(&mut self, entry_id: &str) -> Result<(), BusError> {
        let res: RedisResult<i64> = self
            .conn
            .xack(self.stream, self.group.as_str(), &[entry_id])
            .await;
        res.map(|_count| ())
            .map_err(|e| BusError::redis(self.stream, e))
    }

    /// The stream key this consumer reads from.
    #[inline]
    pub fn stream(&self) -> &'static str {
        self.stream
    }

    /// The consumer-group name.
    #[inline]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The consumer id within the group.
    #[inline]
    pub fn consumer(&self) -> &str {
        &self.consumer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_constants_match_design_table() {
        // Lock the on-wire stream keys to the design's table verbatim.
        assert_eq!(STREAM_HOT_SIGNALS, "hedge.hot.signals");
        assert_eq!(STREAM_HOT_APPROVALS, "hedge.hot.approvals");
        assert_eq!(STREAM_HOT_FILLS, "hedge.hot.fills");
        assert_eq!(STREAM_HOT_REPLAY_RECORD, "hedge.hot.replay_record");
    }

    #[test]
    fn payload_field_constant_is_payload() {
        assert_eq!(PAYLOAD_FIELD, "payload");
    }

    #[test]
    fn stream_entry_carries_id_and_payload() {
        let e = StreamEntry {
            id: "1700000000000-0".to_string(),
            payload: 42u32,
        };
        assert_eq!(e.id, "1700000000000-0");
        assert_eq!(e.payload, 42);
    }
}
