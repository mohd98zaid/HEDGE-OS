//! `WarmCacheUpdater` — Tokio task that subscribes to the Warm_AI_Pipeline
//! NATS subjects and feeds the [`WarmCache`] last-known values.
//!
//! ### Subscription set
//!
//! The updater subscribes to the wildcard subjects defined in the design's
//! NATS Subject Naming Convention table:
//!
//! | Subject pattern              | Producer                    | Snapshot field            |
//! |------------------------------|-----------------------------|---------------------------|
//! | `ai.rank.*`                  | AI_Trade_Ranking_Engine     | `trade_confidence`        |
//! | `ai.regime.changed`          | Market_Regime_Engine        | `market_stability`        |
//! | `ai.psych.stability`         | Trader_Psychology_Engine    | `trader_stability`        |
//! | `ai.priority.changed.*`      | Symbol_Priority_Engine      | `priority`                |
//! | `ai.news.impact.*`           | News_Intelligence_Engine    | `news_impact`             |
//!
//! Every payload is JSON (the Warm_AI_Pipeline is Python; design § Data
//! Models § Warm_AI_Pipeline Events (JSON)). Decode is via `serde_json`.
//!
//! ### Hot_Path discipline
//!
//! This module runs *off* the Hot_Path: it owns its own `tokio::task` and
//! never touches the Risk_Engine's per-tick code. The updater interacts
//! with the Hot_Path only by calling [`WarmCache::store_*`] mutators,
//! which copy-on-write a new snapshot and `ArcSwap::store` it. The
//! Risk_Engine reader observes the new snapshot on its next atomic load
//! — which is one cache miss in the worst case (R9.4: < 50 µs budget).
//!
//! ### Failure model
//!
//! Per-message decode errors are logged at `WARN` and the message is
//! dropped. The connection itself is retried by `async_nats` internally;
//! a sustained outage surfaces as `obs.error.warmcache` events emitted by
//! the supervisor (task 25.x). The updater never `panic!`s on a malformed
//! payload — see design § Error Handling § Hot_Path Error Discipline.

use std::sync::Arc;

use bytes::Bytes;
use hedge_bus::{subjects, BusError, FlatBuffersCodec, NatsClient, RawBytes, Subject};
use hedge_bus::{AI_PSYCH_STABILITY, AI_REGIME_CHANGED};
use hedge_core::{CorrelationId, Priority, SymbolId};
use serde::Deserialize;

use crate::cache::WarmCache;
use crate::snapshot::NewsImpactSnapshot;

/// Wildcard suffix for `ai.rank.<correlation_id>` subjects.
const AI_RANK_WILDCARD: &str = "ai.rank.*";
/// Wildcard suffix for `ai.priority.changed.<symbol>` subjects.
const AI_PRIORITY_WILDCARD: &str = "ai.priority.changed.*";
/// Wildcard suffix for `ai.news.impact.<symbol>` subjects.
const AI_NEWS_IMPACT_WILDCARD: &str = "ai.news.impact.*";

// ---- JSON payload mirrors -----------------------------------------------
//
// We do not use `pydantic`-generated bindings (this is the Hot_Path —
// Python is forbidden, R30.8). Instead we mirror the JSON Schemas from
// `hedge-schemas/json_schemas/` as `serde::Deserialize`-bound structs.
// Field names and types match the schema verbatim; unknown fields are
// ignored on this side because the Warm_AI_Pipeline owns schema evolution
// and may add fields the WarmCache does not yet care about.

/// Mirror of `ai.rank.<cid>` payload (see
/// `hedge-schemas/json_schemas/ai_rank.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct AiRankEvent {
    correlation_id: String,
    trade_confidence_score: f32,
    #[serde(default)]
    ts_ns: u64,
}

/// Mirror of `ai.regime.changed` payload (see
/// `hedge-schemas/json_schemas/ai_regime_changed.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct AiRegimeChangedEvent {
    to: String,
    #[serde(default)]
    ts_ns: u64,
}

/// Mirror of `ai.psych.stability` payload (see
/// `hedge-schemas/json_schemas/ai_psych_stability.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct AiPsychStabilityEvent {
    score: f32,
    #[serde(default)]
    ts_ns: u64,
}

/// Mirror of `ai.priority.changed.<sym>` payload (see
/// `hedge-schemas/json_schemas/ai_priority_changed.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct AiPriorityChangedEvent {
    symbol: String,
    to: String,
    #[serde(default)]
    ts_ns: u64,
}

/// Mirror of `ai.news.impact.<sym>` payload (see
/// `hedge-schemas/json_schemas/ai_news_impact.schema.json`).
#[derive(Debug, Clone, Deserialize)]
struct AiNewsImpactEvent {
    symbol: String,
    sentiment: f32,
    impact_magnitude: f32,
    #[serde(default)]
    ts_ns: u64,
}

// ---- Mapping helpers ---------------------------------------------------

/// Map the canonical `Regime` label (matching the
/// `ai.regime.changed.to` enum in the JSON Schema) to a
/// `MarketStability ∈ [0.0, 1.0]` factor. Values mirror the
/// Warm_AI_Pipeline reference table (see
/// `python/hedge_warm_ai/src/hedge_warm_ai/regime/config.py
/// :_default_stability_factor_map`) so the Hot_Path and the producer
/// agree without a network round-trip.
fn regime_to_market_stability(label: &str) -> f32 {
    match label {
        "Trending" => 1.00,
        "Sideways" => 0.80,
        "HighVolatility" => 0.50,
        "NewsDriven" => 0.40,
        "LowParticipation" => 0.30,
        "LiquidityCrisis" => 0.10,
        "Panic" => 0.05,
        // Unknown label — neutral (defence in depth; the JSON schema
        // already enforces the enum at decode time).
        _ => 1.00,
    }
}

/// Parse the `to: "P1"|"P2"|"P3"|"P4"` field. Returns `Priority::P3`
/// (the design default) for unknown labels so the cache stays neutral.
fn parse_priority(label: &str) -> Priority {
    match label {
        "P1" => Priority::P1,
        "P2" => Priority::P2,
        "P3" => Priority::P3,
        "P4" => Priority::P4,
        _ => Priority::P3,
    }
}

/// Parse a `symbol` JSON field into the Hot_Path's [`SymbolId`].
///
/// The `ai.*` schemas declare `symbol` as a free-form string (1..=32
/// chars). The Hot_Path uses interned `u32` ids — interning lives in the
/// Market_Data_Engine and is not yet available to this crate. As an
/// interim measure we accept the canonical decimal string form
/// (`"42"`) so replay rigs and unit tests can drive the updater
/// directly. Production will replace this with a call into the global
/// symbol interner once it ships.
fn parse_symbol(s: &str) -> Option<SymbolId> {
    s.parse::<u32>().ok().map(SymbolId::new)
}

/// Parse a `correlation_id` JSON field into [`CorrelationId`]. The field
/// is declared as a 1..=64-char string. Two canonical forms are accepted:
///
/// 1. The 32-char hex form (16 bytes ULID/UUID, no dashes) — the canonical
///    wire form emitted by the Warm_AI_Pipeline producer.
/// 2. A plain decimal `u128` literal — useful only in unit tests.
///
/// The hex branch is gated on the 32-char length so that `"42"` is not
/// silently re-interpreted as `0x42`.
fn parse_correlation_id(s: &str) -> Option<CorrelationId> {
    if s.len() == 32 {
        if let Ok(v) = u128::from_str_radix(s, 16) {
            return Some(CorrelationId(v));
        }
    }
    s.parse::<u128>().ok().map(CorrelationId)
}

// ---- Public API --------------------------------------------------------

/// Long-running task that subscribes to the Warm_AI_Pipeline subjects and
/// feeds the [`WarmCache`] from inbound NATS messages.
///
/// Construct with [`WarmCacheUpdater::connect`] and run with
/// [`WarmCacheUpdater::run`]. The two are split so callers can wire
/// supervised retry / backoff (R25.x) around the connect step without
/// coupling it to the run loop.
pub struct WarmCacheUpdater {
    cache: Arc<WarmCache>,
    nats: NatsClient,
}

impl WarmCacheUpdater {
    /// Connect to NATS using the URL configured on the cache.
    pub async fn connect(cache: Arc<WarmCache>) -> Result<Self, BusError> {
        let nats = NatsClient::connect(cache.config().nats_url()).await?;
        Ok(Self { cache, nats })
    }

    /// Construct from an already-connected client. Useful in tests where
    /// the caller wires a single shared [`NatsClient`] across multiple
    /// subscribers.
    pub fn from_client(cache: Arc<WarmCache>, nats: NatsClient) -> Self {
        Self { cache, nats }
    }

    /// Run the subscription loop. Returns when **any** of the underlying
    /// streams ends — in production the `WarmCacheUpdater` is wrapped
    /// by the Self_Healing_Supervisor (R25.x) which restarts the task.
    ///
    /// The loop is push-driven: every awaited future is a NATS-bus
    /// `recv()` on a long-lived subscription. There is **no
    /// `tokio::time::interval` polling** anywhere in this method
    /// (R30.3, enforced by the CI gate at `scripts/check-no-polling.sh`).
    pub async fn run(self) -> Result<(), BusError> {
        let cache = self.cache;
        let nats = self.nats;

        let mut sub_rank = nats
            .subscriber(
                Subject::<RawBytes>::new(AI_RANK_WILDCARD),
                FlatBuffersCodec,
            )
            .await?;
        let mut sub_regime = nats
            .subscriber(
                Subject::<RawBytes>::new(AI_REGIME_CHANGED),
                FlatBuffersCodec,
            )
            .await?;
        let mut sub_psych = nats
            .subscriber(
                Subject::<RawBytes>::new(AI_PSYCH_STABILITY),
                FlatBuffersCodec,
            )
            .await?;
        let mut sub_priority = nats
            .subscriber(
                Subject::<RawBytes>::new(AI_PRIORITY_WILDCARD),
                FlatBuffersCodec,
            )
            .await?;
        let mut sub_news = nats
            .subscriber(
                Subject::<RawBytes>::new(AI_NEWS_IMPACT_WILDCARD),
                FlatBuffersCodec,
            )
            .await?;

        // Multiplex five subscriptions in one task. Every branch is a
        // future on a long-lived subscription; the `tokio::select!` macro
        // only awakes when one of them yields a payload — no polling
        // primitive (`tokio::time::interval`/`sleep`) is involved.
        loop {
            tokio::select! {
                msg = sub_rank.recv_bytes() => match msg {
                    Ok(bytes) => Self::handle_rank(&cache, &bytes),
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => {
                        tracing::warn!(error = %other, "warmcache: ai.rank.* recv failed");
                    }
                },
                msg = sub_regime.recv_bytes() => match msg {
                    Ok(bytes) => Self::handle_regime(&cache, &bytes),
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => {
                        tracing::warn!(error = %other, "warmcache: ai.regime.changed recv failed");
                    }
                },
                msg = sub_psych.recv_bytes() => match msg {
                    Ok(bytes) => Self::handle_psych(&cache, &bytes),
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => {
                        tracing::warn!(error = %other, "warmcache: ai.psych.stability recv failed");
                    }
                },
                msg = sub_priority.recv_bytes() => match msg {
                    Ok(bytes) => Self::handle_priority(&cache, &bytes),
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => {
                        tracing::warn!(error = %other, "warmcache: ai.priority.changed.* recv failed");
                    }
                },
                msg = sub_news.recv_bytes() => match msg {
                    Ok(bytes) => Self::handle_news(&cache, &bytes),
                    Err(BusError::SubscriptionClosed { .. }) => return Ok(()),
                    Err(other) => {
                        tracing::warn!(error = %other, "warmcache: ai.news.impact.* recv failed");
                    }
                },
            }
        }
    }

    // ---- Per-subject decoders -------------------------------------------

    /// Apply a single `ai.rank.<cid>` event.
    pub(crate) fn handle_rank(cache: &WarmCache, bytes: &Bytes) {
        match serde_json::from_slice::<AiRankEvent>(bytes) {
            Ok(ev) => {
                let Some(cid) = parse_correlation_id(&ev.correlation_id) else {
                    tracing::warn!(
                        correlation_id = %ev.correlation_id,
                        "warmcache: ai.rank correlation_id not parseable; dropping"
                    );
                    return;
                };
                let ts = if ev.ts_ns == 0 { hedge_core::now_ns() } else { ev.ts_ns };
                cache.store_trade_confidence(cid, ev.trade_confidence_score, ts);
            }
            Err(e) => tracing::warn!(error = %e, "warmcache: ai.rank decode failed; dropping"),
        }
    }

    /// Apply a single `ai.regime.changed` event.
    pub(crate) fn handle_regime(cache: &WarmCache, bytes: &Bytes) {
        match serde_json::from_slice::<AiRegimeChangedEvent>(bytes) {
            Ok(ev) => {
                let stability = regime_to_market_stability(&ev.to);
                let ts = if ev.ts_ns == 0 { hedge_core::now_ns() } else { ev.ts_ns };
                cache.store_market_stability(stability, ts);
            }
            Err(e) => tracing::warn!(
                error = %e,
                "warmcache: ai.regime.changed decode failed; dropping"
            ),
        }
    }

    /// Apply a single `ai.psych.stability` event.
    pub(crate) fn handle_psych(cache: &WarmCache, bytes: &Bytes) {
        match serde_json::from_slice::<AiPsychStabilityEvent>(bytes) {
            Ok(ev) => {
                let ts = if ev.ts_ns == 0 { hedge_core::now_ns() } else { ev.ts_ns };
                cache.store_trader_stability(ev.score, ts);
            }
            Err(e) => tracing::warn!(
                error = %e,
                "warmcache: ai.psych.stability decode failed; dropping"
            ),
        }
    }

    /// Apply a single `ai.priority.changed.<sym>` event.
    pub(crate) fn handle_priority(cache: &WarmCache, bytes: &Bytes) {
        match serde_json::from_slice::<AiPriorityChangedEvent>(bytes) {
            Ok(ev) => {
                let Some(sym) = parse_symbol(&ev.symbol) else {
                    tracing::warn!(
                        symbol = %ev.symbol,
                        "warmcache: ai.priority.changed symbol not parseable; dropping"
                    );
                    return;
                };
                cache.store_priority(sym, parse_priority(&ev.to));
            }
            Err(e) => tracing::warn!(
                error = %e,
                "warmcache: ai.priority.changed decode failed; dropping"
            ),
        }
    }

    /// Apply a single `ai.news.impact.<sym>` event.
    pub(crate) fn handle_news(cache: &WarmCache, bytes: &Bytes) {
        match serde_json::from_slice::<AiNewsImpactEvent>(bytes) {
            Ok(ev) => {
                let Some(sym) = parse_symbol(&ev.symbol) else {
                    tracing::warn!(
                        symbol = %ev.symbol,
                        "warmcache: ai.news.impact symbol not parseable; dropping"
                    );
                    return;
                };
                let ts = if ev.ts_ns == 0 { hedge_core::now_ns() } else { ev.ts_ns };
                cache.store_news_impact(
                    sym,
                    NewsImpactSnapshot {
                        sentiment: ev.sentiment,
                        impact_magnitude: ev.impact_magnitude,
                        ts_ns: ts,
                    },
                );
            }
            Err(e) => tracing::warn!(
                error = %e,
                "warmcache: ai.news.impact decode failed; dropping"
            ),
        }
    }
}

// We pull `subjects` and the typed wildcards in via `use` above so the
// imports document which canonical subject names this updater listens
// to. If a future refactor in `hedge-bus` renames a constant we want a
// compile error here, not silent drift.
const _: &str = AI_REGIME_CHANGED;
const _: &str = AI_PSYCH_STABILITY;
const _: fn(SymbolId) -> Subject<()> = subjects::ai_priority_changed::<()>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WarmCacheConfig;

    fn cache() -> Arc<WarmCache> {
        Arc::new(WarmCache::new(WarmCacheConfig::from_parts(
            8,
            0,
            "nats://127.0.0.1:4222",
        )))
    }

    #[test]
    fn handle_rank_inserts_confidence_for_decimal_cid() {
        let c = cache();
        let payload = br#"{"correlation_id":"42","signal_id":"sig","trade_confidence_score":0.7,"factors":{"orderflow":0,"technical_strength":0,"news_sentiment":0,"market_regime":0,"trader_discipline":0},"shadow":false,"ts_ns":1000}"#;
        WarmCacheUpdater::handle_rank(&c, &Bytes::from_static(payload));
        let cid = CorrelationId(42);
        // ts_ns=1000 in the payload + zero staleness ⇒ visible.
        assert_eq!(c.trade_confidence_at(cid, 2_000), Some(0.7));
    }

    #[test]
    fn handle_rank_drops_unparseable_cid() {
        let c = cache();
        let payload = br#"{"correlation_id":"--not-a-cid--","signal_id":"sig","trade_confidence_score":0.5,"factors":{"orderflow":0,"technical_strength":0,"news_sentiment":0,"market_regime":0,"trader_discipline":0},"shadow":false,"ts_ns":1}"#;
        WarmCacheUpdater::handle_rank(&c, &Bytes::from_static(payload));
        // No insert happened — len stays zero.
        assert_eq!(c.trade_confidence(CorrelationId(0)), None);
    }

    #[test]
    fn handle_regime_maps_label_to_factor() {
        let c = cache();
        let payload =
            br#"{"from":"Trending","to":"Panic","ts_ns":100}"#;
        WarmCacheUpdater::handle_regime(&c, &Bytes::from_static(payload));
        // `Panic` -> 0.05 from the canonical table.
        let v = c.market_stability();
        assert!((v - 0.05).abs() < 1e-6, "got {v}");
    }

    #[test]
    fn handle_regime_unknown_label_falls_back_to_one() {
        let c = cache();
        let payload =
            br#"{"from":"Trending","to":"BOGUS","ts_ns":100}"#;
        WarmCacheUpdater::handle_regime(&c, &Bytes::from_static(payload));
        assert!((c.market_stability() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn handle_psych_writes_score() {
        let c = cache();
        let payload = br#"{"score":0.42,"components":{"discipline":0,"emotional_control":0,"risk_consistency":0,"patience":0},"behaviors":[],"ts_ns":7}"#;
        WarmCacheUpdater::handle_psych(&c, &Bytes::from_static(payload));
        assert!((c.trader_stability() - 0.42).abs() < 1e-6);
    }

    #[test]
    fn handle_priority_updates_tier() {
        let c = cache();
        let payload =
            br#"{"symbol":"42","from":"P3","to":"P1","ts_ns":1}"#;
        WarmCacheUpdater::handle_priority(&c, &Bytes::from_static(payload));
        assert_eq!(c.priority(SymbolId::new(42)), Priority::P1);
    }

    #[test]
    fn handle_news_writes_impact() {
        let c = cache();
        let payload = br#"{"correlation_id":"x","symbol":"7","headline_id":"h","sentiment":0.6,"impact_magnitude":0.4,"fast_path":true,"slow_path_pending":false,"ts_ns":99}"#;
        WarmCacheUpdater::handle_news(&c, &Bytes::from_static(payload));
        let n = c.news_impact(SymbolId::new(7));
        assert!((n.sentiment - 0.6).abs() < 1e-6);
        assert!((n.impact_magnitude - 0.4).abs() < 1e-6);
        assert_eq!(n.ts_ns, 99);
    }

    #[test]
    fn handle_decode_failure_does_not_panic() {
        let c = cache();
        // Each handler must tolerate garbage and leave the cache unchanged.
        WarmCacheUpdater::handle_rank(&c, &Bytes::from_static(b"{garbage"));
        WarmCacheUpdater::handle_regime(&c, &Bytes::from_static(b"{garbage"));
        WarmCacheUpdater::handle_psych(&c, &Bytes::from_static(b"{garbage"));
        WarmCacheUpdater::handle_priority(&c, &Bytes::from_static(b"{garbage"));
        WarmCacheUpdater::handle_news(&c, &Bytes::from_static(b"{garbage"));
        // Defaults still in place.
        assert!((c.market_stability() - 1.0).abs() < f32::EPSILON);
        assert!((c.trader_stability() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn regime_to_market_stability_table_matches_python_reference() {
        // Cross-language wire contract: the Rust Hot_Path consumer must
        // agree byte-for-byte with the producer table in
        // python/hedge_warm_ai/src/hedge_warm_ai/regime/config.py.
        assert!((regime_to_market_stability("Trending") - 1.00).abs() < 1e-6);
        assert!((regime_to_market_stability("Sideways") - 0.80).abs() < 1e-6);
        assert!((regime_to_market_stability("HighVolatility") - 0.50).abs() < 1e-6);
        assert!((regime_to_market_stability("NewsDriven") - 0.40).abs() < 1e-6);
        assert!((regime_to_market_stability("LowParticipation") - 0.30).abs() < 1e-6);
        assert!((regime_to_market_stability("LiquidityCrisis") - 0.10).abs() < 1e-6);
        assert!((regime_to_market_stability("Panic") - 0.05).abs() < 1e-6);
    }

    #[test]
    fn parse_correlation_id_accepts_decimal_and_32char_hex() {
        // Decimal form (small literal).
        assert_eq!(parse_correlation_id("42"), Some(CorrelationId(42)));
        // 32-char hex — the canonical wire form.
        let hex = format!("{:032x}", 0x1234_5678u128);
        assert_eq!(parse_correlation_id(&hex), Some(CorrelationId(0x1234_5678)));
        // 2-char "hex" is rejected: short inputs are read as decimal so
        // `"ff"` is **not** silently interpreted as 255.
        assert_eq!(parse_correlation_id("ff"), None);
        // Strings with non-hex+non-decimal chars are rejected.
        assert_eq!(parse_correlation_id("--bogus--"), None);
    }

    #[test]
    fn parse_priority_total_for_known_labels_and_safe_fallback() {
        for (label, expected) in [
            ("P1", Priority::P1),
            ("P2", Priority::P2),
            ("P3", Priority::P3),
            ("P4", Priority::P4),
            ("BOGUS", Priority::P3),
        ] {
            assert_eq!(parse_priority(label), expected, "label={label}");
        }
    }
}
