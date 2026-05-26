//! `FeatureExtractionEngine` — per-symbol incremental compute orchestrator.
//!
//! The engine owns one `parking_lot::Mutex<FeatureState>` per symbol
//! inside a `DashMap`. On each `Tick` the engine:
//!
//! 1. Locks the per-symbol state (per-symbol mutex, not a global lock).
//! 2. Runs every incremental indicator's `update` in a fixed order.
//! 3. Builds a [`FeatureSnapshot`] (the FlatBuffers POD struct from
//!    `hedge-schemas`) with every indicator's `compute` result.
//! 4. Encodes the snapshot to a [`RawBytes`] payload via [`encode`].
//! 5. Publishes on `feat.update.<sym>` (non-blocking — the actual NATS
//!    publish is awaited by the engine binary, not the hot loop).
//! 6. Records a `LatencyTracer`-equivalent record on
//!    `obs.latency.FeatureExtraction` with the 3 ms budget (R28.2).
//!
//! ## Why manual latency instrumentation
//!
//! `hedge_obs::LatencyTracer` is intentionally `!Send` so it cannot
//! cross an `.await`. The Hot_Path async publish path crosses one (the
//! NATS `publish_bytes` call), so we measure the synchronous compute
//! window manually — same Prometheus histogram, same per-stage breach
//! event, but without trapping a tracer guard across the await boundary.

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use hedge_bus::{subjects, BusError, Codec, FlatBuffersCodec, NatsClient, RawBytes, Subject};
use hedge_core::{now_ns, CorrelationId, SymbolId};
use hedge_obs::metrics;
use hedge_obs::tracer::LatencyEmitter;
use hedge_schemas::stage::Stage;
use hedge_schemas::{FeatureSnapshot, LatencyRecord, Tick};
use parking_lot::Mutex;
use tracing::{instrument, warn};

use crate::incremental::{
    atr, breakout, candle, compression, ema, liquidity, momentum, rolling_delta, sweep,
    volatility, vwap,
};
use crate::state::FeatureState;
use crate::war_mode::WarModeProfile;

/// p99 budget for the feature-extraction stage in nanoseconds (R28.2).
pub const FEATURE_EXTRACTION_BUDGET_NS: u64 = 3_000_000;

/// Wire size of the encoded `FeatureSnapshot`. Mirrors `tick_to_raw_bytes`
/// in `hedge-market-data`: each field is emitted in declaration order, all
/// multi-byte values little-endian, without struct padding.
///
/// `16 (cid) + 4 (symbol) + 4*8 (vwap/atr/ema_fast/ema_slow i64) + 3*4 (slope/vol/momentum f32)
///  + 8 (rolling_delta i64) + 2*4 (liq_imbalance/of_strength f32) + 1 (candle u8)
///  + 3*4 (breakout/compression/sweep f32) + 8 (ts_ns u64)`
///   = `16 + 4 + 32 + 12 + 8 + 8 + 1 + 12 + 8` = `101` bytes.
pub const FEATURE_WIRE_SIZE: usize = 16 + 4 + 4 * 8 + 3 * 4 + 8 + 2 * 4 + 1 + 3 * 4 + 8;

/// Encode a [`FeatureSnapshot`] (POD) into a wire [`RawBytes`].
///
/// Field order matches `schemas/features.fbs`. Once the typed
/// `FlatBuffersCodec<FeatureSnapshot>` lands in task 4.2, this helper
/// is replaced by the generated builder.
pub fn encode(snap: &FeatureSnapshot) -> RawBytes {
    let mut buf = Vec::with_capacity(FEATURE_WIRE_SIZE);
    buf.extend_from_slice(&snap.correlation_id);
    buf.extend_from_slice(&snap.symbol.to_le_bytes());
    buf.extend_from_slice(&snap.vwap.to_le_bytes());
    buf.extend_from_slice(&snap.atr.to_le_bytes());
    buf.extend_from_slice(&snap.ema_fast.to_le_bytes());
    buf.extend_from_slice(&snap.ema_slow.to_le_bytes());
    buf.extend_from_slice(&snap.ema_slope.to_le_bytes());
    buf.extend_from_slice(&snap.realized_vol.to_le_bytes());
    buf.extend_from_slice(&snap.momentum.to_le_bytes());
    buf.extend_from_slice(&snap.rolling_delta.to_le_bytes());
    buf.extend_from_slice(&snap.liquidity_imbalance.to_le_bytes());
    buf.extend_from_slice(&snap.orderflow_strength.to_le_bytes());
    buf.push(snap.candle_structure);
    buf.extend_from_slice(&snap.breakout_pressure.to_le_bytes());
    buf.extend_from_slice(&snap.compression_zone.to_le_bytes());
    buf.extend_from_slice(&snap.liquidity_sweep.to_le_bytes());
    buf.extend_from_slice(&snap.ts_ns.to_le_bytes());
    debug_assert_eq!(buf.len(), FEATURE_WIRE_SIZE);
    RawBytes::from(buf)
}

/// Codec bridge that adapts the workspace's [`FlatBuffersCodec`] (which
/// works over [`RawBytes`]) onto a `Subject<RawFeaturePayload>`.
#[derive(Default, Copy, Clone)]
pub struct FlatBuffersCodecBridge(FlatBuffersCodec);

/// Newtype wrapping the raw bytes of one [`FeatureSnapshot`]. Used as the
/// codec payload type on `feat.update.<sym>` until the typed FlatBuffers
/// codec ships in task 4.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFeaturePayload(pub RawBytes);

impl Codec<RawFeaturePayload> for FlatBuffersCodecBridge {
    fn encode(&self, value: &RawFeaturePayload) -> Result<Bytes, BusError> {
        self.0.encode(&value.0)
    }
    fn decode(&self, bytes: &[u8]) -> Result<RawFeaturePayload, BusError> {
        Ok(RawFeaturePayload(self.0.decode(bytes)?))
    }
}

/// Run every indicator's `update` and return the resulting snapshot,
/// directly on a borrowed `&mut FeatureState`.
///
/// This is the allocation-free hot loop body. The
/// [`FeatureExtractionEngine::process_tick`] entry point is a thin
/// wrapper that locates the per-symbol cell in the `DashMap` and forwards
/// to this function. Tests that exercise `assert_no_alloc` over the
/// hot path call this function directly so they do not have to spin up
/// a `NatsClient`.
pub fn process_tick_into_state(state: &mut FeatureState, tick: &Tick) -> FeatureSnapshot {
    // Indicator order: state-mutating updates first (so cross-module
    // reads see the new last_book / log_returns / etc.), then candle
    // (which sets session high/low), then sweep (which reads the
    // post-update session high/low).
    //
    // Rolling delta and volatility both read `state.last_ltp_paise` as
    // the **previous** tick's LTP, so we run them BEFORE the bookkeeping
    // step that rolls `last_ltp_paise → prev_ltp_paise`.
    rolling_delta::update(state, tick);
    liquidity::update(state, tick);
    vwap::update(state, tick);
    atr::update(state, tick);
    ema::update(state, tick);
    volatility::update(state, tick);
    momentum::update(state, tick);
    candle::update(state, tick);
    sweep::update(state, tick);
    // Breakout and compression are pure-read indicators — `update` is
    // a no-op but we call them for symmetry / future-proofing.
    breakout::update(state, tick);
    compression::update(state, tick);

    // Bookkeeping: advance the per-state pointers AFTER every indicator
    // has read the previous-tick LTP it needs.
    state.tick_count = state.tick_count.saturating_add(1);
    state.prev_ltp_paise = state.last_ltp_paise;
    state.last_ltp_paise = tick.ltp_paise;
    state.last_ts_ns = tick.ts_recv_ns;

    FeatureSnapshot {
        correlation_id: tick.correlation_id,
        symbol: tick.symbol,
        vwap: vwap::compute_paise(state),
        atr: atr::compute_paise(state),
        ema_fast: ema::compute_fast_paise(state),
        ema_slow: ema::compute_slow_paise(state),
        ema_slope: ema::compute_slope(state),
        realized_vol: volatility::compute(state),
        momentum: momentum::compute(state),
        rolling_delta: rolling_delta::compute_paise(state),
        liquidity_imbalance: liquidity::compute_imbalance(state),
        orderflow_strength: liquidity::compute_orderflow_strength(state),
        candle_structure: candle::classify(state).as_u8(),
        breakout_pressure: breakout::compute(state),
        compression_zone: compression::compute(state),
        liquidity_sweep: sweep::compute(state),
        ts_ns: tick.ts_recv_ns,
    }
}

/// `FeatureExtractionEngine` — owns the per-symbol state map and the
/// shared NATS publisher / latency emitter.
pub struct FeatureExtractionEngine<E: LatencyEmitter + 'static> {
    nats: NatsClient,
    states: Arc<DashMap<SymbolId, Mutex<FeatureState>>>,
    latency_emitter: Arc<E>,
    /// Runtime War_Mode profile. Updated by the engine binary's
    /// `ops.warmode.*` subscriber and surfaced to schedulers / priority
    /// engines via [`Self::war_mode`] (R26.2).
    war_mode: Arc<WarModeProfile>,
}

impl<E: LatencyEmitter + 'static> Clone for FeatureExtractionEngine<E> {
    fn clone(&self) -> Self {
        Self {
            nats: self.nats.clone(),
            states: Arc::clone(&self.states),
            latency_emitter: Arc::clone(&self.latency_emitter),
            war_mode: Arc::clone(&self.war_mode),
        }
    }
}

impl<E: LatencyEmitter + 'static> FeatureExtractionEngine<E> {
    /// Construct a fresh engine.
    pub fn new(nats: NatsClient, latency_emitter: Arc<E>) -> Self {
        Self {
            nats,
            states: Arc::new(DashMap::new()),
            latency_emitter,
            war_mode: Arc::new(WarModeProfile::inactive()),
        }
    }

    /// Borrow the underlying NATS client (used by the binary for shared
    /// connection wiring).
    #[inline]
    pub fn nats(&self) -> &NatsClient {
        &self.nats
    }

    /// Borrow the per-symbol state map (read-only access for tests).
    #[inline]
    pub fn states(&self) -> &Arc<DashMap<SymbolId, Mutex<FeatureState>>> {
        &self.states
    }

    /// Shared handle to the runtime War_Mode profile. The engine binary
    /// drives the [`WarModeProfile`] from its `ops.warmode.*` subscriber
    /// (`hedge-session::WarModeController` is the producer); this
    /// accessor lets schedulers and priority engines read the current
    /// scan multiplier without re-resolving an Arc on every tick.
    #[inline]
    pub fn war_mode(&self) -> &Arc<WarModeProfile> {
        &self.war_mode
    }

    /// Synchronous compute step. Locks the per-symbol mutex, runs every
    /// indicator's `update`, and returns the resulting [`FeatureSnapshot`].
    ///
    /// **Allocation-free** in steady state: the only mutable state lives
    /// in the (preallocated) `RingWindow` buffers inside `FeatureState`,
    /// the `DashMap` already holds an entry for the symbol after the
    /// first tick, and the snapshot is a POD struct returned by value.
    pub fn process_tick(&self, tick: &Tick) -> FeatureSnapshot {
        let sym = SymbolId::new(tick.symbol);
        let cell = self
            .states
            .entry(sym)
            .or_insert_with(|| Mutex::new(FeatureState::default()));
        let mut state = cell.lock();
        process_tick_into_state(&mut state, tick)
    }

    /// Reset the cumulative state for one symbol (e.g. on session start).
    pub fn reset_symbol(&self, sym: SymbolId) {
        if let Some(cell) = self.states.get(&sym) {
            cell.lock().clear_session();
        }
    }

    /// Reset the cumulative state for every tracked symbol.
    pub fn reset_all(&self) {
        for entry in self.states.iter() {
            entry.value().lock().clear_session();
        }
    }

    /// Inject an Orderflow_Engine `liquidity_pressure` value for `sym`.
    ///
    /// The engine binary owns the `of.event.<sym>` subscription; on each
    /// payload it calls this method with the new value. The cached value
    /// is then surfaced as `orderflow_strength` on the next
    /// `feat.update.<sym>` snapshot.
    pub fn ingest_orderflow_pressure(&self, sym: SymbolId, value: f32) {
        let cell = self
            .states
            .entry(sym)
            .or_insert_with(|| Mutex::new(FeatureState::default()));
        liquidity::update_orderflow(&mut cell.lock(), value);
    }

    /// Async wrapper: run `process_tick`, publish the encoded snapshot
    /// on `feat.update.<sym>`, and emit the per-stage latency record.
    #[instrument(level = "trace", skip_all, fields(symbol = tick.symbol))]
    pub async fn ingest_tick(&self, tick: &Tick) -> Result<FeatureSnapshot, BusError> {
        let started_ns = now_ns();
        let snap = self.process_tick(tick);
        let elapsed_compute_ns = now_ns().saturating_sub(started_ns);

        // Publish on feat.update.<sym>.
        let payload = RawFeaturePayload(encode(&snap));
        let subject: Subject<RawFeaturePayload> =
            subjects::feat_update(SymbolId::new(snap.symbol));
        let publisher = self
            .nats
            .publisher::<RawFeaturePayload, _>(subject, FlatBuffersCodecBridge::default());
        if let Err(err) = publisher.publish(&payload).await {
            warn!(error = %err, "feat.update publish failed");
        }

        // Emit latency record on the synchronous compute window. We use
        // the compute-only delta so a slow NATS broker does not pollute
        // the feature-extraction histogram (R28.2 measures compute-only).
        let cid = restore_correlation_id(&tick.correlation_id);
        self.emit_feature_extract_latency(cid, elapsed_compute_ns);

        Ok(snap)
    }

    /// Emit one `LatencyRecord` for the feature-extraction stage.
    fn emit_feature_extract_latency(&self, cid: CorrelationId, elapsed_ns: u64) {
        let m = metrics();
        m.feature_extract_ns.observe(elapsed_ns as f64);

        let breach = elapsed_ns > FEATURE_EXTRACTION_BUDGET_NS;
        let mut cid_bytes = [0u8; 16];
        cid_bytes.copy_from_slice(&cid.as_u128().to_be_bytes());
        let record = LatencyRecord {
            correlation_id: cid_bytes,
            stage: Stage::FeatureExtraction.as_u8(),
            nanos: elapsed_ns,
            budget_nanos: FEATURE_EXTRACTION_BUDGET_NS,
            breach,
        };
        self.latency_emitter
            .emit_record(Stage::FeatureExtraction, &record);
        if breach {
            m.budget_breach_total
                .with_label_values(&[Stage::FeatureExtraction.as_str()])
                .inc();
            self.latency_emitter
                .emit_breach(Stage::FeatureExtraction, &record);
        }
    }
}

/// Reconstruct a `CorrelationId` from the wire-form 16-byte field.
///
/// `CorrelationId` is a `#[repr(transparent)]` newtype around `u128`, so the
/// big-endian → host conversion below is the canonical inverse of the
/// `cid.as_u128().to_be_bytes()` we used when encoding the snapshot.
#[inline]
fn restore_correlation_id(bytes: &[u8; 16]) -> CorrelationId {
    CorrelationId(u128::from_be_bytes(*bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::test_helpers::tick;

    /// Direct integration: ensures the encoded snapshot has the
    /// documented wire size.
    #[test]
    fn encode_emits_documented_wire_size() {
        let snap = FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap: 100_00,
            atr: 50,
            ema_fast: 100_00,
            ema_slow: 99_50,
            ema_slope: 0.5,
            realized_vol: 0.001,
            momentum: 0.01,
            rolling_delta: 5,
            liquidity_imbalance: 0.2,
            orderflow_strength: 0.3,
            candle_structure: 1,
            breakout_pressure: 0.7,
            compression_zone: 0.6,
            liquidity_sweep: 0.0,
            ts_ns: 12345,
        };
        let raw = encode(&snap);
        assert_eq!(raw.len(), FEATURE_WIRE_SIZE);
    }

    #[test]
    fn feature_extraction_budget_matches_design() {
        // R28.2: feature extraction p99 ≤ 3 ms.
        assert_eq!(FEATURE_EXTRACTION_BUDGET_NS, 3_000_000);
    }

    #[test]
    fn flatbuffers_codec_bridge_round_trips_through_codec() {
        let bridge = FlatBuffersCodecBridge::default();
        let snap = FeatureSnapshot {
            correlation_id: [1u8; 16],
            symbol: 7,
            vwap: 0,
            atr: 0,
            ema_fast: 0,
            ema_slow: 0,
            ema_slope: 0.0,
            realized_vol: 0.0,
            momentum: 0.0,
            rolling_delta: 0,
            liquidity_imbalance: 0.0,
            orderflow_strength: 0.0,
            candle_structure: 0,
            breakout_pressure: 0.0,
            compression_zone: 0.0,
            liquidity_sweep: 0.0,
            ts_ns: 0,
        };
        let payload = RawFeaturePayload(encode(&snap));
        let bytes = bridge.encode(&payload).unwrap();
        let decoded = bridge.decode(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }

    /// Lightweight in-process check: directly drive `FeatureState`
    /// through every indicator's `update` exactly the way the engine
    /// would, and assert that warm-up gates resolve correctly. This
    /// avoids needing a live NATS broker for unit tests.
    #[test]
    fn warm_up_gates_resolve_in_lockstep() {
        let mut s = FeatureState::default();
        // Before any ticks every is_ready returns false.
        assert!(!vwap::is_ready(&s));
        assert!(!atr::is_ready(&s));
        assert!(!ema::is_ready(&s));
        assert!(!volatility::is_ready(&s));
        assert!(!momentum::is_ready(&s));
        assert!(!compression::is_ready(&s));
        assert!(!sweep::is_ready(&s));

        // 32 ticks — large enough to fill every window.
        for i in 0..32u64 {
            let t = tick(100_00 + (i as i64) * 10, 5);
            // Mutator order MUST mirror engine.process_tick.
            rolling_delta::update(&mut s, &t);
            liquidity::update(&mut s, &t);
            vwap::update(&mut s, &t);
            atr::update(&mut s, &t);
            ema::update(&mut s, &t);
            volatility::update(&mut s, &t);
            momentum::update(&mut s, &t);
            candle::update(&mut s, &t);
            sweep::update(&mut s, &t);
            s.tick_count = s.tick_count.saturating_add(1);
            s.prev_ltp_paise = s.last_ltp_paise;
            s.last_ltp_paise = t.ltp_paise;
        }

        // After 32 ticks every gate should be true.
        assert!(vwap::is_ready(&s));
        assert!(atr::is_ready(&s));
        assert!(ema::is_ready(&s));
        assert!(volatility::is_ready(&s));
        assert!(momentum::is_ready(&s));
        assert!(compression::is_ready(&s));
    }
}
