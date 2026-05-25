//! Market_Data_Engine orchestrator.
//!
//! Composes the adapter, normalizer, distributor, breadth aggregator, and
//! NATS publisher into the single per-source ingest loop described in
//! design § Components § Market_Data_Engine.
//!
//! ### Per-tick flow
//!
//! ```text
//! WebSocket frame
//!   └─► WsAdapter::next_message()       ┐
//!         └─► RawTick                   │
//!               └─► TickNormalizer.normalize() → Tick (FlatBuffers POD)
//!                     ├─► NATS publish on md.tick.<symbol_id> (RawBytes via FlatBuffersCodec)
//!                     ├─► Distributor.broadcast(&tick)
//!                     └─► BreadthAggregator.on_tick(&tick)
//!                           └─► (on batch boundary) NATS publish on
//!                               md.breadth.sector / md.breadth.volatility
//!                                                                       ┘
//!     The whole bracket above is timed under a 2 ms p99 budget (R28.1)
//!     and emitted as one `obs.latency.TickIngest` record per tick.
//! ```
//!
//! Each adapter runs in its own Tokio task; failures are isolated. The
//! engine owns no shared mutable state across adapters except the
//! `Arc<SymbolInterner>` and the `Arc<Distributor>`.
//!
//! ### Why we measure manually instead of `LatencyTracer::start`
//!
//! The `hedge_obs::LatencyTracer` is intentionally `!Send` (`PhantomData<*const ()>`)
//! to forbid Hot_Path stages from accidentally holding a tracer guard
//! across an `.await` boundary. The Market_Data_Engine's per-tick path
//! IS async (NATS publish is async by construction), so the equivalent
//! timing semantics are realised here by computing `now_ns()` deltas
//! and calling [`LatencyEmitter::emit_record`] / [`LatencyEmitter::emit_breach`]
//! directly. The same fields are populated, the same Prometheus
//! histogram (`hedge_tick_ingest_ns`) and breach counter
//! (`hedge_budget_breach_total{stage="TickIngest"}`) are updated, and
//! `correlation_id` is preserved end-to-end (R27.4).

use std::sync::Arc;

use hedge_bus::{subjects, FlatBuffersCodec, JsonCodec, NatsClient, RawBytes, Subject};
use hedge_core::{now_ns, CorrelationId, SymbolId};
use hedge_obs::metrics;
use hedge_obs::tracer::LatencyEmitter;
use hedge_schemas::stage::Stage;
use hedge_schemas::{LatencyRecord, Tick};
use tokio::task::JoinHandle;
use tracing::instrument;

use crate::adapter::LiveWsAdapter;
use crate::breadth::{BreadthAggregator, SectorBreadth, VolatilityBreadth};
use crate::distributor::Distributor;
use crate::error::MarketDataError;
use crate::interner::SymbolInterner;
use crate::normalizer::{Exchange, TickNormalizer};
use crate::protocol::MarketDataProtocol;

/// p99 budget for the tick-ingest stage in nanoseconds (R28.1, design §
/// Latency Budget Allocation: tick ingest 2 ms).
pub const TICK_INGEST_BUDGET_NS: u64 = 2_000_000;

/// Engine handle — holds shared state and exposes per-adapter task spawning.
///
/// The engine is cloneable (everything inside is `Arc`) so adapter spawn
/// closures can capture a cheap snapshot. We implement [`Clone`] manually
/// because `Arc<E>` is `Clone` for any `E` but `#[derive(Clone)]` would
/// add a spurious `E: Clone` bound.
pub struct MarketDataEngine<E: LatencyEmitter + 'static> {
    nats: NatsClient,
    interner: Arc<SymbolInterner>,
    distributor: Arc<Distributor>,
    normalizer: TickNormalizer,
    latency_emitter: Arc<E>,
}

impl<E: LatencyEmitter + 'static> Clone for MarketDataEngine<E> {
    fn clone(&self) -> Self {
        Self {
            nats: self.nats.clone(),
            interner: Arc::clone(&self.interner),
            distributor: Arc::clone(&self.distributor),
            normalizer: self.normalizer.clone(),
            latency_emitter: Arc::clone(&self.latency_emitter),
        }
    }
}

impl<E: LatencyEmitter + 'static> MarketDataEngine<E> {
    /// Construct an engine.
    pub fn new(
        nats: NatsClient,
        interner: Arc<SymbolInterner>,
        distributor: Arc<Distributor>,
        latency_emitter: Arc<E>,
    ) -> Self {
        let normalizer = TickNormalizer::new(Arc::clone(&interner));
        Self {
            nats,
            interner,
            distributor,
            normalizer,
            latency_emitter,
        }
    }

    /// Borrow the shared symbol interner.
    #[inline]
    pub fn interner(&self) -> &Arc<SymbolInterner> {
        &self.interner
    }

    /// Borrow the shared distributor.
    #[inline]
    pub fn distributor(&self) -> &Arc<Distributor> {
        &self.distributor
    }

    /// Borrow the underlying NATS client. Useful when the engine binary
    /// wants to spawn additional publishers (e.g. degraded events) on
    /// the same connection.
    #[inline]
    pub fn nats(&self) -> &NatsClient {
        &self.nats
    }

    /// Process a single normalized tick: publish to NATS, broadcast on
    /// the per-symbol channel, and feed the breadth aggregator.
    ///
    /// Returns the optional sector and volatility breadth payloads when
    /// a batch boundary fires. The caller — typically the per-adapter
    /// task spawned by [`Self::spawn_adapter`] — publishes those
    /// payloads on `md.breadth.sector` / `md.breadth.volatility`.
    #[instrument(level = "trace", skip_all, fields(symbol = tick.symbol))]
    pub async fn ingest_tick(
        &self,
        tick: &Tick,
        breadth: &mut BreadthAggregator,
    ) -> Result<BreadthByproduct, MarketDataError> {
        // 1. Publish on md.tick.<symbol_id> via FlatBuffersCodec + RawBytes.
        let payload = tick_to_raw_bytes(tick);
        let subject: Subject<RawTickPayload> = subjects::md_tick(SymbolId::new(tick.symbol));
        let publisher = self
            .nats
            .publisher::<RawTickPayload, _>(subject, FlatBuffersCodecBridge::default());
        publisher.publish(&payload).await?;

        // 2. Broadcast on the per-symbol channel.
        self.distributor.broadcast(tick);

        // 3. Feed breadth aggregator.
        let snap = breadth.on_tick(tick);
        Ok(BreadthByproduct {
            sector: snap.sector,
            volatility: snap.volatility,
        })
    }

    /// Publish a sector-breadth payload on `md.breadth.sector`.
    pub async fn publish_sector_breadth(
        &self,
        payload: &SectorBreadth,
    ) -> Result<(), MarketDataError> {
        let subject: Subject<SectorBreadth> = Subject::new(hedge_bus::MD_BREADTH_SECTOR);
        let publisher = self
            .nats
            .publisher(subject, JsonCodec::<SectorBreadth>::new());
        publisher.publish(payload).await?;
        Ok(())
    }

    /// Publish a volatility-breadth payload on `md.breadth.volatility`.
    pub async fn publish_volatility_breadth(
        &self,
        payload: &VolatilityBreadth,
    ) -> Result<(), MarketDataError> {
        let subject: Subject<VolatilityBreadth> = Subject::new(hedge_bus::MD_BREADTH_VOL);
        let publisher = self
            .nats
            .publisher(subject, JsonCodec::<VolatilityBreadth>::new());
        publisher.publish(payload).await?;
        Ok(())
    }

    /// Emit one `LatencyRecord` for the tick-ingest stage.
    ///
    /// Updates the `hedge_tick_ingest_ns` Prometheus histogram, fires the
    /// corresponding `obs.latency.TickIngest` event through the configured
    /// [`LatencyEmitter`], and on budget breach also fires
    /// `obs.budget.breach.TickIngest` and increments
    /// `hedge_budget_breach_total{stage="TickIngest"}` (R27.1, R27.4, R28.6).
    fn emit_tick_ingest_latency(&self, cid: CorrelationId, elapsed_ns: u64) {
        let m = metrics();
        m.tick_ingest_ns.observe(elapsed_ns as f64);

        let breach = elapsed_ns > TICK_INGEST_BUDGET_NS;
        let mut cid_bytes = [0u8; 16];
        cid_bytes.copy_from_slice(&cid.as_u128().to_be_bytes());
        let record = LatencyRecord {
            correlation_id: cid_bytes,
            stage: Stage::TickIngest.as_u8(),
            nanos: elapsed_ns,
            budget_nanos: TICK_INGEST_BUDGET_NS,
            breach,
        };
        self.latency_emitter
            .emit_record(Stage::TickIngest, &record);
        if breach {
            m.budget_breach_total
                .with_label_values(&[Stage::TickIngest.as_str()])
                .inc();
            self.latency_emitter
                .emit_breach(Stage::TickIngest, &record);
        }
    }

    /// Spawn a long-running task that drives `adapter` through its receive
    /// loop, normalizing every payload and routing it via [`Self::ingest_tick`].
    ///
    /// Returns the [`JoinHandle`]. Failures inside the adapter loop are
    /// isolated: a transport error triggers `adapter.reconnect()` under the
    /// documented backoff schedule and the task continues.
    pub fn spawn_adapter<P>(
        &self,
        mut adapter: LiveWsAdapter<P>,
        exchange: Exchange,
        breadth: BreadthAggregator,
    ) -> JoinHandle<()>
    where
        P: MarketDataProtocol + 'static,
    {
        let engine = self.clone();
        tokio::spawn(async move {
            engine.run_adapter_loop(&mut adapter, exchange, breadth).await;
        })
    }

    /// Drive `adapter` through its receive loop until the surrounding
    /// task is cancelled. Public for tests that want to feed a custom
    /// adapter implementation through the engine.
    #[instrument(level = "info", skip_all, fields(source = %adapter.source(), exchange = ?exchange))]
    pub async fn run_adapter_loop<P>(
        &self,
        adapter: &mut LiveWsAdapter<P>,
        exchange: Exchange,
        mut breadth: BreadthAggregator,
    ) where
        P: MarketDataProtocol + 'static,
    {
        loop {
            match adapter.next_message().await {
                Ok(raw) => {
                    // Mint correlation_id once so the latency record and the
                    // normalized Tick share the same id (R27.4).
                    let cid = CorrelationId::new();
                    let started_ns = now_ns();

                    let tick = self
                        .normalizer
                        .normalize_with_correlation(&raw, exchange, cid);

                    match self.ingest_tick(&tick, &mut breadth).await {
                        Ok(by) => {
                            if let Some(s) = by.sector {
                                if let Err(err) = self.publish_sector_breadth(&s).await {
                                    tracing::warn!(error = %err, "publish sector breadth failed");
                                }
                            }
                            if let Some(v) = by.volatility {
                                if let Err(err) = self.publish_volatility_breadth(&v).await {
                                    tracing::warn!(error = %err, "publish volatility breadth failed");
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "ingest_tick failed");
                        }
                    }

                    let elapsed_ns = now_ns().saturating_sub(started_ns);
                    self.emit_tick_ingest_latency(cid, elapsed_ns);
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        source = %adapter.source(),
                        "adapter next_message failed; reconnecting",
                    );
                    if let Err(rerr) = adapter.reconnect().await {
                        // Reconnect itself failed (e.g. dial refused). We
                        // loop and retry; the next next_message call will
                        // observe the still-broken stream and trigger
                        // another reconnect, exercising the schedule
                        // progression.
                        tracing::warn!(
                            error = %rerr,
                            source = %adapter.source(),
                            attempt = adapter.attempt_count(),
                            "reconnect failed; retrying",
                        );
                    }
                }
            }
        }
    }
}

/// Optional breadth payloads produced by [`MarketDataEngine::ingest_tick`].
#[derive(Debug, Default, Clone)]
pub struct BreadthByproduct {
    /// Sector-breadth payload to be published on `md.breadth.sector`.
    pub sector: Option<SectorBreadth>,
    /// Volatility-breadth payload to be published on `md.breadth.volatility`.
    pub volatility: Option<VolatilityBreadth>,
}

// ---- FlatBuffers raw-bytes bridge --------------------------------------
//
// The hedge-schemas crate does not yet ship a typed
// `FlatBuffersCodec<Tick>` (task 4.2). Until then we serialize a `Tick`
// (POD struct) as a fixed-layout byte string of explicit little-endian
// field values via the workspace's `FlatBuffersCodec` + `RawBytes` codec.
// Encoding fields explicitly (rather than transmuting the struct) avoids
// reading any padding bytes the compiler may insert between the `exchange`
// byte and the following `i64` fields and keeps the wire form
// reproducible across platforms.

/// Newtype wrapping the raw bytes of one `Tick`. Used as the codec payload
/// type on `md.tick.<sym>` until the typed FlatBuffers codec lands in
/// task 4.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTickPayload(pub RawBytes);

/// Codec bridge that adapts the workspace's [`FlatBuffersCodec`] (which
/// works over [`RawBytes`]) onto a `Subject<RawTickPayload>`.
#[derive(Default, Copy, Clone)]
pub struct FlatBuffersCodecBridge(FlatBuffersCodec);

impl hedge_bus::Codec<RawTickPayload> for FlatBuffersCodecBridge {
    fn encode(&self, value: &RawTickPayload) -> Result<bytes::Bytes, hedge_bus::BusError> {
        self.0.encode(&value.0)
    }
    fn decode(&self, bytes: &[u8]) -> Result<RawTickPayload, hedge_bus::BusError> {
        Ok(RawTickPayload(self.0.decode(bytes)?))
    }
}

/// Fixed wire size of the [`RawTickPayload`] body, in bytes:
///
/// `16 (correlation_id) + 4 (symbol) + 1 (exchange) + 7×8 (paise/qty/timestamp i64/u64 fields)`
///   = `16 + 4 + 1 + 56` = `77` bytes.
pub const TICK_WIRE_SIZE: usize = 16 + 4 + 1 + 8 * 8;

/// Encode a [`Tick`] (POD) into the wire `RawTickPayload`.
///
/// Field order on the wire matches `Tick_v1` declaration order in
/// `schemas/tick.fbs`. Multi-byte fields are emitted in little-endian so
/// the wire form is reproducible across hosts. Once `hedge-schemas` ships
/// the typed `FlatBuffersCodec<Tick>` (task 4.2) this helper is replaced
/// by `flatbuffers::FlatBufferBuilder::finished_data`.
pub fn tick_to_raw_bytes(tick: &Tick) -> RawTickPayload {
    let mut buf = Vec::with_capacity(TICK_WIRE_SIZE);
    buf.extend_from_slice(&tick.correlation_id);
    buf.extend_from_slice(&tick.symbol.to_le_bytes());
    buf.push(tick.exchange as u8);
    buf.extend_from_slice(&tick.ltp_paise.to_le_bytes());
    buf.extend_from_slice(&tick.bid_paise.to_le_bytes());
    buf.extend_from_slice(&tick.ask_paise.to_le_bytes());
    buf.extend_from_slice(&tick.ltq.to_le_bytes());
    buf.extend_from_slice(&tick.total_buy_qty.to_le_bytes());
    buf.extend_from_slice(&tick.total_sell_qty.to_le_bytes());
    buf.extend_from_slice(&tick.ts_exchange_ns.to_le_bytes());
    buf.extend_from_slice(&tick.ts_recv_ns.to_le_bytes());
    debug_assert_eq!(buf.len(), TICK_WIRE_SIZE);
    RawTickPayload(RawBytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tick(symbol: u32) -> Tick {
        Tick {
            correlation_id: [0u8; 16],
            symbol,
            exchange: 0,
            ltp_paise: 100,
            bid_paise: 99,
            ask_paise: 101,
            ltq: 1,
            total_buy_qty: 0,
            total_sell_qty: 0,
            ts_exchange_ns: 0,
            ts_recv_ns: 0,
        }
    }

    #[test]
    fn tick_to_raw_bytes_emits_documented_wire_size() {
        let t = make_tick(7);
        let raw = tick_to_raw_bytes(&t);
        assert_eq!(raw.0.len(), TICK_WIRE_SIZE);
    }

    #[test]
    fn tick_to_raw_bytes_distinct_for_distinct_ticks() {
        let a = tick_to_raw_bytes(&make_tick(1));
        let b = tick_to_raw_bytes(&make_tick(2));
        assert_ne!(a.0.as_slice(), b.0.as_slice());
    }

    #[test]
    fn tick_to_raw_bytes_uses_little_endian_for_symbol_id() {
        let t = make_tick(0x01020304);
        let raw = tick_to_raw_bytes(&t);
        // After the 16-byte correlation id, the next 4 bytes are the
        // little-endian symbol u32: 04 03 02 01.
        let symbol_bytes = &raw.0.as_slice()[16..20];
        assert_eq!(symbol_bytes, &[0x04, 0x03, 0x02, 0x01]);
    }

    #[test]
    fn tick_ingest_budget_matches_design() {
        // R28.1: tick ingest p99 ≤ 2 ms.
        assert_eq!(TICK_INGEST_BUDGET_NS, 2_000_000);
    }

    #[test]
    fn flatbuffers_codec_bridge_round_trips_through_codec() {
        use hedge_bus::Codec;
        let bridge = FlatBuffersCodecBridge::default();
        let original = tick_to_raw_bytes(&make_tick(1234));
        let bytes = bridge.encode(&original).expect("encode");
        let decoded = bridge.decode(&bytes).expect("decode");
        assert_eq!(decoded, original);
    }

    #[test]
    fn raw_tick_payload_clones_cheaply() {
        // RawBytes is refcounted; cloning the payload must not copy the
        // underlying buffer.
        let a = tick_to_raw_bytes(&make_tick(1));
        let b = a.clone();
        assert_eq!(a.0.as_slice().as_ptr(), b.0.as_slice().as_ptr());
    }
}
