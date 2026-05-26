//! Typed configuration structs mirroring the design's YAML surface.
//!
//! Every struct uses `#[serde(deny_unknown_fields)]` so a typo in
//! `/etc/hedge/config.yaml` is caught at load time rather than silently
//! ignored. Defaults match the design's
//! _Data Models § Configuration Surface and Defaults_ section verbatim
//! and live in [`crate::defaults`].

use chrono::NaiveTime;
use hedge_core::BrokerId;
use serde::{Deserialize, Serialize};

use crate::defaults;
use crate::error::ConfigError;

// ---------------------------------------------------------------------------
// Top-level configuration ---------------------------------------------------
// ---------------------------------------------------------------------------

/// Root configuration loaded from `/etc/hedge/config.yaml`.
///
/// Maps 1:1 to the YAML in design § Data Models § Configuration Surface and
/// Defaults (R32). Sub-structs are themselves `deny_unknown_fields`, so any
/// unrecognised key — at any depth — fails loading with
/// `ConfigError::SchemaViolation`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HedgeConfig {
    /// Capital base and daily-profit-target band (R32.1, R32.2, R32.3).
    pub capital: CapitalConfig,
    /// Risk_Engine limits and gates (R5, R31, R32.4).
    pub risk: RiskConfig,
    /// Trading session window in IST (R26.1).
    pub session: SessionConfig,
    /// Market_Open_War_Mode window and confidence floor (R26.2, R26.3).
    pub war_mode: WarModeConfig,
    /// UI-side thresholds.
    pub ui: UiConfig,
    /// AI governance, ranking, and shadow-mode surface (R10, R17, R23).
    pub ai: AiConfig,
    /// Trader_Psychology threshold ladder (R16).
    pub trader_psychology: TraderPsychologyConfig,
    /// Broker primary/backup and failover thresholds (R6.5, R7.1).
    pub brokers: BrokerConfig,
    /// Ollama_Infrastructure model registry (R10).
    pub ollama: OllamaConfig,
    /// Observability retention and degraded-mode policy (R27, R28).
    pub observability: ObservabilityConfig,
    /// Warm_AI_Pipeline last-known-value cache surface read by the
    /// Risk_Engine on the Hot_Path (R9.4, R9.5, R17.4, R19.7; design
    /// § Architecture § Hot_Path Architecture (WarmCache)).
    pub warm_cache: WarmCacheConfig,
    /// Replay_Engine recorder + player surface (R22; design §
    /// Components § Replay_Engine).
    pub replay: ReplayConfig,
}

// ---------------------------------------------------------------------------
// Capital -------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Post-target policy applied once `daily_profit_target_max_inr` is hit
/// (R31.4, R32.3). YAML uses snake_case strings.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostTargetPolicy {
    /// Cap new entries at zero size; existing positions are managed normally.
    ReduceSizeToZero,
    /// Halt all signal admission for the remainder of the session.
    StopForSession,
    /// Halve the per-trade size on subsequent entries.
    HalveSize,
    /// Continue trading without modification (explicitly opt-in).
    Continue,
}

/// Capital base and daily-profit-target band (R32.1–R32.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapitalConfig {
    /// Default ₹20,000 (R32.1).
    pub base_inr: u32,
    /// Daily-profit-target lower bound; default ₹300 (R32.2).
    pub daily_profit_target_min_inr: u32,
    /// Daily-profit-target upper bound; default ₹1,000 (R32.2).
    pub daily_profit_target_max_inr: u32,
    /// Behaviour after upper bound is reached (R32.3).
    pub post_target_policy: PostTargetPolicy,
}

impl Default for CapitalConfig {
    fn default() -> Self {
        defaults::capital()
    }
}

// ---------------------------------------------------------------------------
// Risk ----------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Risk_Engine limits exactly mirroring design YAML (R5, R31, R32.4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    /// Max realised+unrealised loss for the session (₹).
    pub max_daily_loss_inr: u32,
    /// Max absolute position size (units) per symbol.
    pub max_position_per_symbol: u32,
    /// Max absolute position size summed across symbols.
    pub max_position_portfolio: u32,
    /// Max leverage permitted on any single symbol.
    pub max_leverage_per_symbol: f32,
    /// Max leverage permitted across the entire account.
    pub max_leverage_account: f32,
    /// Max peak-to-trough drawdown (₹) before kill-switch.
    pub max_drawdown_inr: u32,
    /// Max trade entries per rolling minute.
    pub max_trades_per_minute: u32,
    /// Max trade entries per rolling hour.
    pub max_trades_per_hour: u32,
    /// Max trade entries per session.
    pub max_trades_per_session: u32,
    /// Max notional exposure per symbol (₹).
    pub max_exposure_per_symbol_inr: u32,
    /// Max notional exposure per sector (₹).
    pub max_exposure_per_sector_inr: u32,
    /// Slippage cooldown trigger (basis points).
    pub slippage_threshold_bps: u32,
    /// Cooldown duration after a slippage breach (ms).
    pub slippage_cooldown_ms: u32,
    /// Realised volatility above which new entries are blocked.
    pub volatility_block_threshold: f32,
    /// Broker round-trip latency above which new entries are blocked (ms).
    pub broker_latency_block_ms: u32,
    /// Base risk per trade in rupees, scaled by `Adaptive_Risk` (R5.13).
    pub base_risk_per_trade_inr: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        defaults::risk()
    }
}

// ---------------------------------------------------------------------------
// Session and War_Mode ------------------------------------------------------
// ---------------------------------------------------------------------------

/// Regular trading session window in IST (R26.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    /// Session open. Default `09:15:00`.
    #[serde(with = "time_ist")]
    pub start_ist: NaiveTime,
    /// Session close. Default `15:30:00`.
    #[serde(with = "time_ist")]
    pub end_ist: NaiveTime,
}

impl Default for SessionConfig {
    fn default() -> Self {
        defaults::session()
    }
}

/// Market_Open_War_Mode parameters (R26.2, R26.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WarModeConfig {
    /// War-mode start; default `09:15:00`.
    #[serde(with = "time_ist")]
    pub start_ist: NaiveTime,
    /// War-mode end; default `09:45:00`.
    #[serde(with = "time_ist")]
    pub end_ist: NaiveTime,
    /// Minimum signal confidence accepted while war mode is active.
    pub min_confidence: f32,
    /// Scan-frequency multiplier applied during war mode.
    pub scan_multiplier: f32,
}

impl Default for WarModeConfig {
    fn default() -> Self {
        defaults::war_mode()
    }
}

// ---------------------------------------------------------------------------
// UI ------------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// UI-side thresholds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiConfig {
    /// Realised-volatility threshold above which the UI enters high-vol mode.
    pub high_vol_threshold: f32,
}

impl Default for UiConfig {
    fn default() -> Self {
        defaults::ui()
    }
}

// ---------------------------------------------------------------------------
// AI ------------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Drift bands governing AI suspend/critical actions (R23).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernanceConfig {
    /// Drift level at which a `warn` is published.
    pub drift_warn: f32,
    /// Drift level at which the offending component is auto-suspended.
    pub drift_critical: f32,
}

/// Per-factor weights used by the AI ranking engine (R17.1).
///
/// Surface only — formula constants are fixed in R17.1.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankingFactorsConfig {
    #[doc = "Surface only — formula constants are fixed in R17.1"]
    pub orderflow: f32,
    #[doc = "Surface only — formula constants are fixed in R17.1"]
    pub technical_strength: f32,
    #[doc = "Surface only — formula constants are fixed in R17.1"]
    pub news_sentiment: f32,
    #[doc = "Surface only — formula constants are fixed in R17.1"]
    pub market_regime: f32,
    #[doc = "Surface only — formula constants are fixed in R17.1"]
    pub trader_discipline: f32,
}

/// AI governance, ranking, and shadow-mode surface (R10, R17, R23).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AiConfig {
    /// AI components currently running in shadow mode.
    pub shadow_components: Vec<String>,
    /// Drift thresholds for AI governance (R23).
    pub governance: GovernanceConfig,
    /// p95 budget for the ranking call (ms).
    pub rank_p95_budget_ms: u64,
    /// Fixed factor weights for the ranking formula (R17.1).
    pub ranking_factors: RankingFactorsConfig,
}

impl Default for AiConfig {
    fn default() -> Self {
        defaults::ai()
    }
}

// ---------------------------------------------------------------------------
// Trader Psychology ---------------------------------------------------------
// ---------------------------------------------------------------------------

/// Threshold ladder for the Trader_Stability_Score (R16).
///
/// Invariants enforced by [`PsychologyThresholds::validated`]:
/// `critical < suppression < cooldown < warning`.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PsychologyThresholds {
    /// Above this score, trading proceeds normally; UI shows a warning.
    pub warning: f32,
    /// Score at which a brief cooldown is enforced.
    pub cooldown: f32,
    /// Score at which new signals are suppressed.
    pub suppression: f32,
    /// Score at which the kill-switch is engaged.
    pub critical: f32,
}

impl PsychologyThresholds {
    /// Construct and validate the ordering invariant
    /// `critical < suppression < cooldown < warning`.
    pub fn validated(
        warning: f32,
        cooldown: f32,
        suppression: f32,
        critical: f32,
    ) -> Result<Self, ConfigError> {
        let raw = Self { warning, cooldown, suppression, critical };
        raw.check_ordering()?;
        Ok(raw)
    }

    /// Returns `Ok(())` when the ordering invariant holds.
    pub fn check_ordering(&self) -> Result<(), ConfigError> {
        if !(self.critical < self.suppression
            && self.suppression < self.cooldown
            && self.cooldown < self.warning)
        {
            return Err(ConfigError::InvariantViolation(format!(
                "trader_psychology.thresholds must satisfy critical < suppression < cooldown < warning; got critical={}, suppression={}, cooldown={}, warning={}",
                self.critical, self.suppression, self.cooldown, self.warning
            )));
        }
        Ok(())
    }
}

/// Trader_Psychology config (R16).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraderPsychologyConfig {
    /// Threshold ladder; validated for monotonicity.
    pub thresholds: PsychologyThresholds,
}

impl Default for TraderPsychologyConfig {
    fn default() -> Self {
        defaults::trader_psychology()
    }
}

// ---------------------------------------------------------------------------
// Brokers -------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Broker primary/backup selection and failover thresholds (R6.5, R7.1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfig {
    /// Primary broker.
    #[serde(with = "broker_id_serde")]
    pub primary: BrokerId,
    /// Backup broker for failover (R6.5).
    #[serde(with = "broker_id_serde")]
    pub backup: BrokerId,
    /// Sliding-window error rate above which we fail over.
    pub failover_error_rate: f32,
    /// Sliding-window p99 latency above which we fail over (ms).
    pub failover_latency_ms: u32,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        defaults::brokers()
    }
}

// ---------------------------------------------------------------------------
// Ollama --------------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Role assigned to an Ollama model in the routing table (R10).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OllamaRole {
    /// High-quality model used by default for narrative tasks.
    Primary,
    /// Smaller, lower-latency model.
    Fast,
    /// Reasoning-heavy model used for trade post-mortems.
    Deep,
    /// Lightweight model used for low-stakes utility tasks.
    Lightweight,
}

/// One row of the Ollama model registry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OllamaModelConfig {
    /// Model name, e.g. `qwen2.5:14b`.
    pub name: String,
    /// Role this model fulfils.
    pub role: OllamaRole,
    /// Quantisation tag, e.g. `q4_k_m`.
    pub quant: String,
}

/// Ollama_Infrastructure model registry (R10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OllamaConfig {
    /// Configured models in priority order.
    pub models: Vec<OllamaModelConfig>,
}

impl Default for OllamaConfig {
    fn default() -> Self {
        defaults::ollama()
    }
}

// ---------------------------------------------------------------------------
// Observability -------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Retention windows in days for observability stores.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    /// Prometheus retention.
    pub metrics_days: u32,
    /// Loki retention.
    pub logs_days: u32,
    /// Jaeger retention.
    pub traces_days: u32,
}

/// Degraded-mode policy when observability backends misbehave (R28.6).
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DegradedModeConfig {
    /// When `true`, drop low-severity logs while Loki is unavailable.
    pub drop_low_severity_logs_at_loki_unavailable: bool,
    /// Trace downsampling ratio applied while Jaeger is overloaded.
    pub sample_traces_at_jaeger_overload: f32,
}

/// Observability config (R27, R28).
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// Retention windows.
    pub retention: RetentionConfig,
    /// Degraded-mode behaviour.
    pub degraded_mode: DegradedModeConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        defaults::observability()
    }
}

impl Default for HedgeConfig {
    fn default() -> Self {
        defaults::hedge_config()
    }
}

// ---------------------------------------------------------------------------
// WarmCache -----------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Last-known-value cache surface read by the Risk_Engine on the Hot_Path
/// (R9.4, R9.5, R17.4, R19.7; design § Architecture § Hot_Path Architecture
/// (WarmCache)).
///
/// The Risk_Engine reads cached `trade_confidence`, `market_stability`,
/// `trader_stability`, `priority`, and `news_impact` values via a single
/// `arc_swap::ArcSwap` atomic load on the per-tick path. Population is
/// driven by a separate `WarmCacheUpdater` task that subscribes to the
/// `ai.*` NATS subjects defined in design § NATS Subject Naming Convention.
///
/// Defaults match design § Hot_Path Architecture: 8192-entry LRU on
/// `trade_confidence`, 5-second staleness window, and the canonical local
/// NATS endpoint (`nats://127.0.0.1:4222`) used by the bundled
/// `docker/nats` deployment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WarmCacheConfig {
    /// Maximum number of `correlation_id → trade_confidence` entries the
    /// LRU layer holds before evicting the least-recently-touched entry.
    /// Default 8192 (one entry comfortably covers a full session of
    /// concurrent rankings on a ₹20 000 base).
    pub trade_confidence_lru_size: u32,
    /// Staleness window in milliseconds. Entries older than this when
    /// looked up are reported as missing so the Risk_Engine falls back to
    /// `Signal_v1.confidence`. Default 5_000 ms (5 s) — matches the
    /// Warm_AI_Pipeline ranking p95 budget plus headroom.
    pub staleness_window_ms: u32,
    /// NATS endpoint the `WarmCacheUpdater` task connects to. Used at
    /// service startup; the read path never touches NATS. Default
    /// `nats://127.0.0.1:4222`.
    pub nats_url: String,
}

impl Default for WarmCacheConfig {
    fn default() -> Self {
        defaults::warm_cache()
    }
}

// ---------------------------------------------------------------------------
// Replay_Engine -------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Default replay-player speed when the UI does not override it on
/// the `/replay` control plane (R22.2).
///
/// Wire form is the snake_case lowercase token shared with the UI
/// (`ui/src/types/replay.ts` `ReplaySpeed`):
///
/// | YAML | Meaning                       |
/// |------|-------------------------------|
/// | `x1` | Real-time pacing.             |
/// | `x10`| Ten-times pacing.             |
/// | `max`| As fast as the player can run.|
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaySpeed {
    /// Real-time pacing — `monotonic_ns` deltas honoured 1:1.
    X1,
    /// Ten-times pacing — `monotonic_ns` deltas divided by 10.
    X10,
    /// As-fast-as-possible — no pacing.
    Max,
}

impl ReplaySpeed {
    /// Multiplier for wall-clock pacing. `None` means "no pacing"
    /// (`Max`); `Some(n)` means each recorded `monotonic_ns` delta is
    /// divided by `n` before sleeping.
    #[inline]
    pub const fn divisor(self) -> Option<u32> {
        match self {
            Self::X1 => Some(1),
            Self::X10 => Some(10),
            Self::Max => None,
        }
    }

    /// Stable lower-case token used in UI events and metric labels.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::X1 => "x1",
            Self::X10 => "x10",
            Self::Max => "max",
        }
    }
}

/// Replay_Engine recorder + player surface (R22).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayConfig {
    /// Directory under which `replay/<session_id>/seg-NNNN.rkyv`
    /// segments are written. Default `./replay`.
    pub segment_dir: String,
    /// Maximum size of one segment file before rotation, in bytes.
    /// Segments also rotate on session boundary regardless of size.
    /// Default 1 GiB (`1_073_741_824`).
    pub max_segment_bytes: u64,
    /// Default speed used by the player when the UI does not specify
    /// one (R22.2).
    pub default_speed: ReplaySpeed,
    /// Deterministic seed for any stochastic component in the player
    /// (R22.2). Mirrors `Player::new(seed: u64, ...)`.
    pub rng_seed: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        defaults::replay()
    }
}

// ---------------------------------------------------------------------------
// Serde adapters ------------------------------------------------------------
// ---------------------------------------------------------------------------

/// `NaiveTime <-> "HH:MM:SS"` adapter so YAML stays human-friendly.
mod time_ist {
    use chrono::NaiveTime;
    use serde::{Deserialize, Deserializer, Serializer};

    const FMT: &str = "%H:%M:%S";

    pub fn serialize<S: Serializer>(t: &NaiveTime, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&t.format(FMT).to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<NaiveTime, D::Error> {
        let raw = String::deserialize(d)?;
        NaiveTime::parse_from_str(&raw, FMT).map_err(serde::de::Error::custom)
    }
}

/// snake_case serde for `hedge_core::BrokerId` so YAML reads
/// `primary: zerodha` while the rest of the workspace keeps using the
/// PascalCase Rust enum unchanged.
mod broker_id_serde {
    use hedge_core::BrokerId;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(b: &BrokerId, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(match b {
            BrokerId::Zerodha => "zerodha",
            BrokerId::Dhan => "dhan",
            BrokerId::Shoonya => "shoonya",
            BrokerId::AngelOne => "angel_one",
            BrokerId::Upstox => "upstox",
            BrokerId::Simulated => "simulated",
        })
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<BrokerId, D::Error> {
        let raw = String::deserialize(d)?;
        Ok(match raw.as_str() {
            "zerodha" => BrokerId::Zerodha,
            "dhan" => BrokerId::Dhan,
            "shoonya" => BrokerId::Shoonya,
            "angel_one" => BrokerId::AngelOne,
            "upstox" => BrokerId::Upstox,
            "simulated" => BrokerId::Simulated,
            other => {
                return Err(serde::de::Error::unknown_variant(
                    other,
                    &["zerodha", "dhan", "shoonya", "angel_one", "upstox", "simulated"],
                ));
            }
        })
    }
}
