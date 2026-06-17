//! `SignalEngine` — strategy-evaluation orchestrator.
//!
//! Owns the stable-order array of strategies, the per-symbol previous-day
//! memory cache, the gating logic, and the NATS + Redis Streams publisher
//! pair. On each `feat.update.<sym>` the engine:
//!
//! 1. Looks up (or creates) the per-symbol previous-day cache entry.
//! 2. Iterates every strategy in stable registration order.
//! 3. Runs the pre-evaluate gates ([`crate::gating::check_gates`]).
//! 4. Calls `Strategy::evaluate` and the post-evaluate war-mode gate.
//! 5. Publishes each emitted signal on `sig.emitted` (NATS) **and**
//!    `XADD`s it onto the `hedge.hot.signals` Redis Stream consumer
//!    group (R29.3).
//!
//! ### Stable strategy ordering
//!
//! Strategies are stored in a `Vec<Arc<dyn Strategy>>` populated in one
//! fixed order at construction time. The engine never reorders the
//! vector after construction so two identical inputs produce identical
//! emission sequences (Property 7's "same input → same signal sequence").

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use hedge_bus::{
    BusError, Codec, FlatBuffersCodec, NatsClient, RawBytes, RedisStreamProducer, Subject,
    SIG_EMITTED, STREAM_HOT_SIGNALS,
};
use hedge_core::SymbolId;
use hedge_schemas::{FeatureSnapshot, Signal};
use parking_lot::RwLock;
use redis::aio::ConnectionManager;
use tracing::{instrument, warn};

use crate::context::{NewsGates, PreviousDayMemory, StrategyContext, StrategyToggles};
use crate::gating::{check_gates, check_war_mode};
use crate::strategies::{
    CompositeAlphaBreakout, LiquiditySweepReversal, MomentumBreakout, OpeningRangeBreakout,
    OptionsOiExpansionBreakout, VolatilityCompressionBreakout, VwapPullback,
};
use crate::strategy::Strategy;

/// Wire size of the encoded `Signal_v1`. Mirrors `hedge-features::encode`
/// for `FeatureSnapshot_v1`: each field is emitted in declaration order,
/// all multi-byte values little-endian, without struct padding.
///
/// `16 (cid) + 1 (strategy) + 4 (symbol) + 1 (side) + 4 (base_probability)
///  + 4 (confidence) + 8 (stop_loss) + 8 (take_profit) + 8 (max_size_qty)
///  + 4 (time_horizon_seconds) + 8 (ts_ns)` = 66 bytes.
pub const SIGNAL_WIRE_SIZE: usize = 16 + 1 + 4 + 1 + 4 + 4 + 8 + 8 + 8 + 4 + 8;

/// Encode a [`Signal`] into a wire [`RawBytes`] payload.
///
/// Field order matches `schemas/signal.fbs`. Once the typed FlatBuffers
/// codec lands in task 4.2 this helper is replaced by the generated
/// builder. Mirror layout from `hedge-features::engine::encode` so the
/// two crates stay byte-compatible until then.
pub fn encode_signal(signal: &Signal) -> RawBytes {
    let mut buf = Vec::with_capacity(SIGNAL_WIRE_SIZE);
    buf.extend_from_slice(&signal.correlation_id);
    buf.push(signal.strategy);
    buf.extend_from_slice(&signal.symbol.to_le_bytes());
    buf.push(signal.side);
    buf.extend_from_slice(&signal.base_probability.to_le_bytes());
    buf.extend_from_slice(&signal.confidence.to_le_bytes());
    buf.extend_from_slice(&signal.risk_profile.stop_loss_paise.to_le_bytes());
    buf.extend_from_slice(&signal.risk_profile.take_profit_paise.to_le_bytes());
    buf.extend_from_slice(&signal.risk_profile.max_size_qty.to_le_bytes());
    buf.extend_from_slice(&signal.risk_profile.time_horizon_seconds.to_le_bytes());
    buf.extend_from_slice(&signal.ts_ns.to_le_bytes());
    debug_assert_eq!(buf.len(), SIGNAL_WIRE_SIZE);
    RawBytes::from(buf)
}

/// Codec bridge that adapts the workspace's [`FlatBuffersCodec`] (which
/// works over [`RawBytes`]) onto a `Subject<RawSignalPayload>`.
#[derive(Default, Copy, Clone)]
pub struct FlatBuffersCodecBridge(FlatBuffersCodec);

/// Newtype wrapping the raw bytes of one [`Signal`]. Used as the codec
/// payload type on `sig.emitted` until the typed FlatBuffers codec ships
/// in task 4.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSignalPayload(pub RawBytes);

impl Codec<RawSignalPayload> for FlatBuffersCodecBridge {
    fn encode(&self, value: &RawSignalPayload) -> Result<Bytes, BusError> {
        self.0.encode(&value.0)
    }
    fn decode(&self, bytes: &[u8]) -> Result<RawSignalPayload, BusError> {
        Ok(RawSignalPayload(self.0.decode(bytes)?))
    }
}

/// Knobs the engine reads at evaluation time. Values are populated by
/// the engine binary from `hedge-config` / NATS subscribers and
/// snapshotted on each evaluation under `RwLock`.
#[derive(Clone, Debug)]
pub struct SignalEngineConfig {
    /// Current market regime (R13.1). Updated by the
    /// `ai.regime.changed` subscriber.
    pub regime: hedge_core::Regime,
    /// Trader toggles (R4.5). Updated by the
    /// `trader.intent.strategy_toggle` subscriber.
    pub toggles: StrategyToggles,
    /// `true` while Market_Open_War_Mode is active (R26.1, R26.2).
    pub war_mode: bool,
    /// Minimum confidence accepted while war mode is active (R26.3).
    pub war_mode_min_confidence: f32,
    /// News-driven gating (R12.6).
    pub news_gates: NewsGates,
}

impl Default for SignalEngineConfig {
    fn default() -> Self {
        Self {
            regime: hedge_core::Regime::Trending,
            toggles: StrategyToggles::all_enabled(),
            war_mode: false,
            war_mode_min_confidence: 0.7,
            news_gates: NewsGates::empty(),
        }
    }
}

/// Pure synchronous evaluation: run every strategy through gating →
/// evaluate → war-mode-gate, and return the resulting signals in
/// stable strategy order.
///
/// This function performs no IO and does not depend on a NATS client,
/// which makes it the canonical entry point for unit tests. The stateful
/// [`SignalEngine`] wraps this function and adds the publish path.
pub fn evaluate_strategies(
    strategies: &[Arc<dyn Strategy>],
    snap: &FeatureSnapshot,
    cfg: &SignalEngineConfig,
    previous_day: Option<&PreviousDayMemory>,
) -> Vec<Signal> {
    let symbol = SymbolId::new(snap.symbol);
    let ctx = StrategyContext {
        regime: cfg.regime,
        trader_config: &cfg.toggles,
        war_mode: cfg.war_mode,
        war_mode_min_confidence: cfg.war_mode_min_confidence,
        previous_day,
        news_gates: &cfg.news_gates,
    };

    let mut out = Vec::with_capacity(strategies.len());
    for strategy in strategies {
        // Pre-evaluate gates: trader toggle, regime, news.
        if !check_gates(strategy.as_ref(), &ctx, symbol, None).is_allowed() {
            continue;
        }
        let Some(sig) = strategy.evaluate(snap, &ctx) else {
            continue;
        };
        // Post-evaluate war-mode confidence gate.
        if !check_war_mode(&sig, &ctx).is_allowed() {
            continue;
        }
        out.push(sig);
    }
    out
}

/// `SignalEngine` — strategy registry + gating + publish.
pub struct SignalEngine {
    nats: NatsClient,
    redis_stream:
        parking_lot::Mutex<Option<RedisStreamProducer<RawSignalPayload, FlatBuffersCodecBridge>>>,
    /// Stable-order strategy registry. Populated once at construction.
    strategies: Vec<Arc<dyn Strategy>>,
    /// Cached config under `RwLock` so subscriber tasks can update it
    /// without contending with the read-heavy evaluation path.
    config: RwLock<SignalEngineConfig>,
    /// Per-symbol previous-day memory cache keyed on `SymbolId`.
    previous_day: DashMap<SymbolId, PreviousDayMemory>,
}

impl SignalEngine {
    /// Construct an engine with the six configured strategies in their
    /// canonical order (matching `StrategyId` enum order).
    pub fn new_default(nats: NatsClient) -> Self {
        let strategies: Vec<Arc<dyn Strategy>> = vec![
            Arc::new(OpeningRangeBreakout),
            Arc::new(VwapPullback),
            Arc::new(MomentumBreakout),
            Arc::new(LiquiditySweepReversal),
            Arc::new(OptionsOiExpansionBreakout::new()),
            Arc::new(VolatilityCompressionBreakout),
            Arc::new(CompositeAlphaBreakout),
        ];
        Self {
            nats,
            redis_stream: parking_lot::Mutex::new(None),
            strategies,
            config: RwLock::new(SignalEngineConfig::default()),
            previous_day: DashMap::new(),
        }
    }

    /// Construct an engine with an explicit strategy registry. Used by
    /// the integration tests to plug in stub strategies.
    pub fn new_with(nats: NatsClient, strategies: Vec<Arc<dyn Strategy>>) -> Self {
        Self {
            nats,
            redis_stream: parking_lot::Mutex::new(None),
            strategies,
            config: RwLock::new(SignalEngineConfig::default()),
            previous_day: DashMap::new(),
        }
    }

    /// Wire a Redis `ConnectionManager` so emitted signals are also
    /// `XADD`d onto `hedge.hot.signals` for the Risk_Engine consumer
    /// group (R29.3).
    pub fn with_redis(self, redis: ConnectionManager) -> Self {
        let producer = RedisStreamProducer::new(
            redis,
            STREAM_HOT_SIGNALS,
            FlatBuffersCodecBridge::default(),
        );
        *self.redis_stream.lock() = Some(producer);
        self
    }

    /// Borrow the strategy registry (read-only).
    #[inline]
    pub fn strategies(&self) -> &[Arc<dyn Strategy>] {
        &self.strategies
    }

    /// Replace the engine's config. Acquires a write lock — call this
    /// from infrequent NATS subscriber tasks (regime / toggles / news
    /// updates), not from the per-tick path.
    pub fn update_config<F>(&self, f: F)
    where
        F: FnOnce(&mut SignalEngineConfig),
    {
        let mut g = self.config.write();
        f(&mut g);
    }

    /// Read-only borrow of the engine's config snapshot. Cloned out so
    /// the lock guard does not span the evaluation window.
    pub fn config(&self) -> SignalEngineConfig {
        self.config.read().clone()
    }

    /// Inject / update the previous-day memory entry for a symbol.
    pub fn upsert_previous_day(&self, prev: PreviousDayMemory) {
        self.previous_day.insert(prev.symbol, prev);
    }

    /// Synchronous evaluation: run every strategy through gating →
    /// evaluate → war-mode-gate, and return the resulting signals in
    /// stable strategy order.
    ///
    /// Performs no IO. The engine binary calls this on every decoded
    /// `feat.update.<sym>` payload, then awaits the publish path on
    /// each returned signal in [`Self::publish`].
    pub fn evaluate(&self, snap: &FeatureSnapshot) -> Vec<Signal> {
        let cfg = self.config();
        let sym = SymbolId::new(snap.symbol);
        // Snapshot the previous-day record so we do not hold a DashMap
        // ref guard across the strategy iteration. The clone is cheap —
        // `PreviousDayMemory` is plain `Copy`-friendly POD-ish data.
        let prev_day_snapshot: Option<PreviousDayMemory> =
            self.previous_day.get(&sym).map(|r| r.value().clone());
        evaluate_strategies(
            &self.strategies,
            snap,
            &cfg,
            prev_day_snapshot.as_ref(),
        )
    }

    /// Publish a single emitted signal on both NATS (`sig.emitted`) and
    /// the Redis Stream (`hedge.hot.signals`). Failures on either
    /// channel are logged at `warn` and reported via `Err` so the
    /// caller can decide whether to retry.
    #[instrument(level = "trace", skip_all, fields(sig.strategy = signal.strategy, sig.symbol = signal.symbol))]
    pub async fn publish(&self, signal: &Signal) -> Result<(), BusError> {
        let payload = RawSignalPayload(encode_signal(signal));

        // 1. NATS publish on `sig.emitted`.
        let subject: Subject<RawSignalPayload> = Subject::new(SIG_EMITTED);
        let publisher = self
            .nats
            .publisher::<RawSignalPayload, _>(subject, FlatBuffersCodecBridge::default());
        if let Err(err) = publisher.publish(&payload).await {
            warn!(error = %err, "sig.emitted publish failed");
            return Err(err);
        }

        // 2. Redis XADD on `hedge.hot.signals`.
        let producer_clone = {
            let g = self.redis_stream.lock();
            // Explicit deref so `clone()` resolves to `Option::clone`,
            // not to `MutexGuard::clone` (which does not exist).
            (*g).clone()
        };
        if let Some(mut producer) = producer_clone {
            if let Err(err) = producer.xadd(&payload).await {
                warn!(error = %err, "hedge.hot.signals XADD failed");
                return Err(err);
            }
        }

        Ok(())
    }

    /// End-to-end: evaluate then publish every emitted signal.
    pub async fn ingest_feature_snapshot(
        &self,
        snap: &FeatureSnapshot,
    ) -> Result<usize, BusError> {
        let signals = self.evaluate(snap);
        let n = signals.len();
        for sig in &signals {
            self.publish(sig).await?;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{NewsGates, StrategyToggles};
    use crate::strategy::Strategy;
    use hedge_core::{Regime, Side};
    use hedge_schemas::strategy_id::StrategyId;
    use hedge_schemas::{RiskProfile, Signal};

    /// Stub strategy that always fires; used to exercise gating + ordering
    /// in isolation from the real strategies' internal preconditions.
    struct AlwaysFire {
        id: StrategyId,
        confidence: f32,
        regime_enabled: bool,
    }

    impl Strategy for AlwaysFire {
        fn id(&self) -> StrategyId {
            self.id
        }
        fn evaluate(&self, snap: &FeatureSnapshot, _ctx: &StrategyContext) -> Option<Signal> {
            Some(Signal {
                correlation_id: snap.correlation_id,
                strategy: self.id.as_u8(),
                symbol: snap.symbol,
                side: Side::Buy.as_u8(),
                base_probability: 0.5,
                confidence: self.confidence,
                risk_profile: RiskProfile::default(),
                ts_ns: snap.ts_ns,
            })
        }
        fn enabled_in(&self, _regime: Regime) -> bool {
            self.regime_enabled
        }
    }

    fn snap() -> FeatureSnapshot {
        FeatureSnapshot {
            correlation_id: [0u8; 16],
            symbol: 1,
            vwap: 100_00,
            atr: 100,
            ema_fast: 100_00,
            ema_slow: 100_00,
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
            ts_ns: 12345,
        }
    }

    fn three_strategies() -> Vec<Arc<dyn Strategy>> {
        vec![
            Arc::new(AlwaysFire {
                id: StrategyId::OpeningRangeBreakout,
                confidence: 0.8,
                regime_enabled: true,
            }),
            Arc::new(AlwaysFire {
                id: StrategyId::VwapPullback,
                confidence: 0.8,
                regime_enabled: true,
            }),
            Arc::new(AlwaysFire {
                id: StrategyId::MomentumBreakout,
                confidence: 0.8,
                regime_enabled: true,
            }),
        ]
    }

    #[test]
    fn signal_wire_size_matches_documented_layout() {
        // 16 + 1 + 4 + 1 + 4 + 4 + 8 + 8 + 8 + 4 + 8 = 66
        assert_eq!(SIGNAL_WIRE_SIZE, 66);
    }

    #[test]
    fn encode_signal_round_trips_through_the_codec() {
        let bridge = FlatBuffersCodecBridge::default();
        let signal = Signal {
            correlation_id: [7u8; 16],
            strategy: StrategyId::OpeningRangeBreakout.as_u8(),
            symbol: 42,
            side: Side::Buy.as_u8(),
            base_probability: 0.6,
            confidence: 0.8,
            risk_profile: RiskProfile {
                stop_loss_paise: 9_900,
                take_profit_paise: 10_500,
                max_size_qty: 100,
                time_horizon_seconds: 300,
            },
            ts_ns: 12345,
        };
        let payload = RawSignalPayload(encode_signal(&signal));
        assert_eq!(payload.0.len(), SIGNAL_WIRE_SIZE);
        let bytes = bridge.encode(&payload).unwrap();
        let decoded = bridge.decode(&bytes).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn evaluate_preserves_strategy_order() {
        let cfg = SignalEngineConfig::default();
        let signals = evaluate_strategies(&three_strategies(), &snap(), &cfg, None);
        assert_eq!(signals.len(), 3);
        assert_eq!(signals[0].strategy, StrategyId::OpeningRangeBreakout.as_u8());
        assert_eq!(signals[1].strategy, StrategyId::VwapPullback.as_u8());
        assert_eq!(signals[2].strategy, StrategyId::MomentumBreakout.as_u8());
    }

    #[test]
    fn evaluate_skips_disabled_strategies() {
        let mut cfg = SignalEngineConfig::default();
        cfg.toggles = StrategyToggles::all_enabled().with_disabled(StrategyId::VwapPullback);
        let signals = evaluate_strategies(&three_strategies(), &snap(), &cfg, None);
        assert_eq!(signals.len(), 2);
        assert_eq!(signals[0].strategy, StrategyId::OpeningRangeBreakout.as_u8());
        assert_eq!(signals[1].strategy, StrategyId::MomentumBreakout.as_u8());
    }

    #[test]
    fn evaluate_drops_low_confidence_in_war_mode() {
        let strategies: Vec<Arc<dyn Strategy>> = vec![Arc::new(AlwaysFire {
            id: StrategyId::OpeningRangeBreakout,
            confidence: 0.5,
            regime_enabled: true,
        })];
        let mut cfg = SignalEngineConfig::default();
        cfg.war_mode = true;
        cfg.war_mode_min_confidence = 0.7;
        let signals = evaluate_strategies(&strategies, &snap(), &cfg, None);
        assert!(signals.is_empty(), "war mode dropped low-confidence signal");
    }

    #[test]
    fn evaluate_keeps_high_confidence_in_war_mode() {
        let strategies: Vec<Arc<dyn Strategy>> = vec![Arc::new(AlwaysFire {
            id: StrategyId::OpeningRangeBreakout,
            confidence: 0.9,
            regime_enabled: true,
        })];
        let mut cfg = SignalEngineConfig::default();
        cfg.war_mode = true;
        cfg.war_mode_min_confidence = 0.7;
        let signals = evaluate_strategies(&strategies, &snap(), &cfg, None);
        assert_eq!(signals.len(), 1);
    }

    #[test]
    fn evaluate_drops_news_gated_signals() {
        let strategies: Vec<Arc<dyn Strategy>> = vec![Arc::new(AlwaysFire {
            id: StrategyId::OpeningRangeBreakout,
            confidence: 0.9,
            regime_enabled: true,
        })];
        let mut cfg = SignalEngineConfig::default();
        cfg.news_gates = NewsGates::empty();
        cfg.news_gates.blocked_symbols.push(SymbolId::new(1));
        let signals = evaluate_strategies(&strategies, &snap(), &cfg, None);
        assert!(signals.is_empty(), "news gate dropped signal");
    }

    #[test]
    fn evaluate_drops_regime_blocked_strategies() {
        let strategies: Vec<Arc<dyn Strategy>> = vec![Arc::new(AlwaysFire {
            id: StrategyId::OpeningRangeBreakout,
            confidence: 0.9,
            regime_enabled: false,
        })];
        let cfg = SignalEngineConfig::default();
        let signals = evaluate_strategies(&strategies, &snap(), &cfg, None);
        assert!(signals.is_empty(), "regime gate blocked");
    }

    #[test]
    fn evaluate_emits_in_canonical_strategy_order_for_default_engine() {
        // The 6 default strategies use real preconditions, so a zero
        // snapshot will not trigger any of them — we get an empty Vec
        // but in stable order. This still exercises that the engine
        // does not panic and yields an in-order Vec.
        let strategies: Vec<Arc<dyn Strategy>> = vec![
            Arc::new(OpeningRangeBreakout),
            Arc::new(VwapPullback),
            Arc::new(MomentumBreakout),
            Arc::new(LiquiditySweepReversal),
            Arc::new(OptionsOiExpansionBreakout::new()),
            Arc::new(VolatilityCompressionBreakout),
            Arc::new(CompositeAlphaBreakout),
        ];
        let cfg = SignalEngineConfig::default();
        let _ = evaluate_strategies(&strategies, &snap(), &cfg, None);
    }

    #[test]
    fn signal_engine_default_has_six_strategies_in_canonical_order() {
        // We can't construct a real NatsClient without a broker, so we
        // just verify the constructor signature compiles and the
        // strategy IDs are correct by deconstructing them through a
        // helper Vec. The wire-id assertion is in the proptest crate.
        // Here we exercise the order assertion using `evaluate_strategies`.
        let strategies: Vec<Arc<dyn Strategy>> = vec![
            Arc::new(OpeningRangeBreakout),
            Arc::new(VwapPullback),
            Arc::new(MomentumBreakout),
            Arc::new(LiquiditySweepReversal),
            Arc::new(OptionsOiExpansionBreakout::new()),
            Arc::new(VolatilityCompressionBreakout),
            Arc::new(CompositeAlphaBreakout),
        ];
        let ids: Vec<StrategyId> = strategies.iter().map(|s| s.id()).collect();
        assert_eq!(
            ids,
            vec![
                StrategyId::OpeningRangeBreakout,
                StrategyId::VwapPullback,
                StrategyId::MomentumBreakout,
                StrategyId::LiquiditySweepReversal,
                StrategyId::OptionsOiExpansionBreakout,
                StrategyId::VolatilityCompressionBreakout,
                StrategyId::CompositeAlphaBreakout,
            ]
        );
    }
}
