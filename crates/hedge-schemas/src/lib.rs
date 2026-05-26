//! `hedge-schemas` — canonical wire schemas for PROJECT HEDGE.
//!
//! Two separate concerns live in this crate:
//!
//! * **Hot_Path FlatBuffers schemas** — `Tick_v1`, `OrderBook_v1`,
//!   `OpenInterest_v1`, `FeatureSnapshot_v1`, `Signal_v1`, `RiskApproval_v1`,
//!   `OrderIntent_v1`, `OrderState_v1`, `LatencyRecord_v1`, plus the
//!   `RiskProfile_v1` struct embedded inside `Signal_v1`. These are defined
//!   once in `schemas/*.fbs` and re-generated into `src/generated/` whenever
//!   `flatc` is on `PATH` at build time. When `flatc` is missing the
//!   committed bindings in `src/generated/` are used unchanged.
//!
//! * **Warm_AI_Pipeline JSON schemas** — every `ai.*`, `mem.*`, `trader.*`,
//!   `ops.*`, and `obs.*` subject defined in
//!   `design.md § Data Models § Warm_AI_Pipeline Events (JSON)`. The
//!   schemas live as JSON files under `json_schemas/` and are re-exported
//!   through [`json_schemas`] as `&'static str` constants.
//!
//! Consumers should write `use hedge_schemas::Tick;` rather than reaching
//! into the generated module path so the import surface stays stable across
//! `flatc` regenerations.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod generated;
pub mod json_schemas;

// Re-export the flat namespace at the crate root for ergonomic consumer use.
// Consumers write `hedge_schemas::Tick`, `hedge_schemas::Signal`, etc.
pub use generated::hedge::v1::{
    BookLevel, FeatureSnapshot_v1 as FeatureSnapshot, LatencyRecord_v1 as LatencyRecord,
    OpenInterest_v1 as OpenInterest, OrderBook_v1 as OrderBook, OrderIntent_v1 as OrderIntent,
    OrderState_v1 as OrderState, RiskApproval_v1 as RiskApproval, RiskProfile_v1 as RiskProfile,
    Signal_v1 as Signal, Tick_v1 as Tick,
};

/// The FlatBuffers `file_identifier` declared by every schema in this crate.
pub use generated::FILE_IDENTIFIER;

/// Hot_Path stage discriminant used in [`LatencyRecord`].
///
/// The values map 1:1 to `LatencyRecord_v1.stage: u8`, in stage order from
/// tick ingest to broker submission. The order matches the design's
/// "Latency Budget Allocation" table; new stages must be appended so existing
/// recordings stay deserializable.
pub mod stage {
    use core::fmt;

    /// Discriminant for `LatencyRecord_v1.stage`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[repr(u8)]
    pub enum Stage {
        /// Market_Data_Engine tick ingest (R28.1).
        TickIngest = 0,
        /// Feature_Extraction_Engine compute (R28.2).
        FeatureExtraction = 1,
        /// Risk_Engine fetch of the AI score from WarmCache.
        AiScoringFetch = 2,
        /// Risk_Engine evaluate (R28.3).
        RiskCheck = 3,
        /// Execution_Engine routing (R28.4).
        ExecutionRouting = 4,
        /// Broker_Adapter network submission (R28.5).
        BrokerSubmit = 5,
    }

    impl Stage {
        /// Convert from the wire `u8` discriminant. Returns `None` for
        /// unknown values so deserializers can flag schema-evolution
        /// mismatches.
        #[inline]
        pub const fn from_u8(byte: u8) -> Option<Self> {
            match byte {
                0 => Some(Self::TickIngest),
                1 => Some(Self::FeatureExtraction),
                2 => Some(Self::AiScoringFetch),
                3 => Some(Self::RiskCheck),
                4 => Some(Self::ExecutionRouting),
                5 => Some(Self::BrokerSubmit),
                _ => None,
            }
        }

        /// Convert to the wire `u8` discriminant.
        #[inline]
        pub const fn as_u8(self) -> u8 {
            self as u8
        }

        /// Stable canonical name used as the trailing segment of
        /// `obs.latency.<stage>` and `obs.budget.breach.<stage>` subjects.
        #[inline]
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::TickIngest => "TickIngest",
                Self::FeatureExtraction => "FeatureExtraction",
                Self::AiScoringFetch => "AiScoringFetch",
                Self::RiskCheck => "RiskCheck",
                Self::ExecutionRouting => "ExecutionRouting",
                Self::BrokerSubmit => "BrokerSubmit",
            }
        }
    }

    impl fmt::Display for Stage {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }
}

/// `OrderState_v1.state` discriminant.
pub mod order_state {
    use core::fmt;
    use serde::{Deserialize, Serialize};

    /// FSM states for an order in the Execution_Engine (R6.3).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[repr(u8)]
    pub enum OrderLifecycleState {
        /// Order created locally; not yet sent to broker.
        New = 0,
        /// Submitted to broker; awaiting acknowledgement.
        Submitted = 1,
        /// Partially filled; remainder still working.
        PartiallyFilled = 2,
        /// Fully filled.
        Filled = 3,
        /// Cancelled, either by trader, supervisor, or broker.
        Cancelled = 4,
        /// Rejected by broker before any fill.
        Rejected = 5,
    }

    impl OrderLifecycleState {
        /// Convert from the wire `u8` discriminant. Returns `None` for
        /// unknown values.
        #[inline]
        pub const fn from_u8(byte: u8) -> Option<Self> {
            match byte {
                0 => Some(Self::New),
                1 => Some(Self::Submitted),
                2 => Some(Self::PartiallyFilled),
                3 => Some(Self::Filled),
                4 => Some(Self::Cancelled),
                5 => Some(Self::Rejected),
                _ => None,
            }
        }

        /// Convert to the wire `u8` discriminant.
        #[inline]
        pub const fn as_u8(self) -> u8 {
            self as u8
        }

        /// Canonical name; used as the trailing segment of
        /// `exec.order.<state>` subjects.
        #[inline]
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::New => "New",
                Self::Submitted => "Submitted",
                Self::PartiallyFilled => "PartiallyFilled",
                Self::Filled => "Filled",
                Self::Cancelled => "Cancelled",
                Self::Rejected => "Rejected",
            }
        }

        /// Returns `true` when the state is terminal — no further transitions
        /// are valid. Property 9 (Order Lifecycle FSM Validity) relies on
        /// this to bound the legal transition graph.
        #[inline]
        pub const fn is_terminal(self) -> bool {
            matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
        }
    }

    impl fmt::Display for OrderLifecycleState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }
}

/// `Signal_v1.strategy` discriminant for the six configured Hot_Path
/// strategies (design § Components § Signal_Engine).
pub mod strategy_id {
    use core::fmt;

    /// Configured strategies. Order is stable across the codebase; new
    /// strategies must be appended.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    #[repr(u8)]
    pub enum StrategyId {
        /// Opening Range Breakout (ORB).
        OpeningRangeBreakout = 0,
        /// VWAP Pullback continuation.
        VwapPullback = 1,
        /// Momentum Breakout.
        MomentumBreakout = 2,
        /// Liquidity Sweep Reversal.
        LiquiditySweepReversal = 3,
        /// Options OI Expansion Breakout.
        OptionsOiExpansionBreakout = 4,
        /// Volatility Compression Breakout.
        VolatilityCompressionBreakout = 5,
    }

    impl StrategyId {
        /// Convert from the wire `u8` discriminant. Returns `None` for
        /// unknown values.
        #[inline]
        pub const fn from_u8(byte: u8) -> Option<Self> {
            match byte {
                0 => Some(Self::OpeningRangeBreakout),
                1 => Some(Self::VwapPullback),
                2 => Some(Self::MomentumBreakout),
                3 => Some(Self::LiquiditySweepReversal),
                4 => Some(Self::OptionsOiExpansionBreakout),
                5 => Some(Self::VolatilityCompressionBreakout),
                _ => None,
            }
        }

        /// Convert to the wire `u8` discriminant.
        #[inline]
        pub const fn as_u8(self) -> u8 {
            self as u8
        }

        /// Canonical PascalCase name used in JSON schemas and metrics labels.
        #[inline]
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::OpeningRangeBreakout => "OpeningRangeBreakout",
                Self::VwapPullback => "VwapPullback",
                Self::MomentumBreakout => "MomentumBreakout",
                Self::LiquiditySweepReversal => "LiquiditySweepReversal",
                Self::OptionsOiExpansionBreakout => "OptionsOiExpansionBreakout",
                Self::VolatilityCompressionBreakout => "VolatilityCompressionBreakout",
            }
        }
    }

    impl fmt::Display for StrategyId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }
}

/// Stable Risk_Engine rejection codes carried in `RiskApproval_v1.rationale_code`
/// (when no approval is issued the same code is published on
/// `risk.decision.rejected`). The numeric values are wire-stable and must
/// not be reordered.
pub mod rejection_reason {
    use core::fmt;
    use serde::{Deserialize, Serialize};

    /// Reasons the Risk_Engine may reject a signal (design § Components §
    /// Risk_Engine; § Error Handling § Hot_Path Error Discipline).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[repr(u8)]
    pub enum RejectionReason {
        /// The signal evaluated successfully — i.e. this is the rationale on
        /// an approved decision (R5.13 sized using `Adaptive_Risk`). It is
        /// kept in the same enum so `rationale_code` round-trips through a
        /// single namespace.
        Approved = 0,
        /// Outside the configured IST trading session window (R31.1).
        SessionClosed = 1,
        /// Daily loss cap reached (R5.2).
        MaxDailyLoss = 2,
        /// Per-symbol or portfolio position cap reached (R5.3).
        MaxPosition = 3,
        /// Per-symbol or account leverage cap reached (R5.4).
        MaxLeverage = 4,
        /// Drawdown cap reached; Kill_Switch armed (R5.5).
        MaxDrawdown = 5,
        /// Per-minute / hour / session trade-frequency cap reached (R5.6).
        TradeFrequency = 6,
        /// Per-symbol or per-sector exposure cap reached (R5.7).
        MaxExposure = 7,
        /// Slippage threshold breached; symbol cooldown active (R5.8).
        SlippageCooldown = 8,
        /// Realized volatility above configured block threshold (R5.10).
        VolatilityBlock = 9,
        /// Active broker latency above block threshold (R5.11).
        BrokerLatencyBlock = 10,
        /// Daily-profit-target post-target policy active (R32.3).
        ProfitTargetReached = 11,
        /// Trader Kill_Switch engaged (R5.9, R20.6).
        KillSwitchEngaged = 12,
        /// Market_Regime_Engine has gated this strategy / symbol (R12.6).
        RegimeBlocked = 13,
        /// News_Intelligence has gated this sector / symbol (R13.4).
        NewsBlocked = 14,
        /// War_Mode confidence threshold not met (R26.2).
        WarModeConfidenceTooLow = 15,
        /// `Adaptive_Risk` collapsed to zero (R5.13 + degraded WarmCache).
        AdaptiveRiskZero = 16,
        /// `ApprovalToken` HMAC verification failed inside the Risk_Engine
        /// (defence in depth; the Execution_Engine also re-verifies, R6.8).
        InvalidApprovalToken = 17,
        /// Internal error during evaluation; the Risk_Engine never approves
        /// under uncertainty (design § Error Handling § Hot_Path Error
        /// Discipline).
        InternalError = 18,
    }

    impl RejectionReason {
        /// Convert from the wire `u8` discriminant. Returns `None` for
        /// unknown values so deserializers can flag schema-evolution
        /// mismatches.
        #[inline]
        pub const fn from_u8(byte: u8) -> Option<Self> {
            match byte {
                0 => Some(Self::Approved),
                1 => Some(Self::SessionClosed),
                2 => Some(Self::MaxDailyLoss),
                3 => Some(Self::MaxPosition),
                4 => Some(Self::MaxLeverage),
                5 => Some(Self::MaxDrawdown),
                6 => Some(Self::TradeFrequency),
                7 => Some(Self::MaxExposure),
                8 => Some(Self::SlippageCooldown),
                9 => Some(Self::VolatilityBlock),
                10 => Some(Self::BrokerLatencyBlock),
                11 => Some(Self::ProfitTargetReached),
                12 => Some(Self::KillSwitchEngaged),
                13 => Some(Self::RegimeBlocked),
                14 => Some(Self::NewsBlocked),
                15 => Some(Self::WarModeConfidenceTooLow),
                16 => Some(Self::AdaptiveRiskZero),
                17 => Some(Self::InvalidApprovalToken),
                18 => Some(Self::InternalError),
                _ => None,
            }
        }

        /// Convert to the wire `u8` discriminant.
        #[inline]
        pub const fn as_u8(self) -> u8 {
            self as u8
        }

        /// Canonical short string used in metric labels and structured logs.
        #[inline]
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Approved => "approved",
                Self::SessionClosed => "session_closed",
                Self::MaxDailyLoss => "max_daily_loss",
                Self::MaxPosition => "max_position",
                Self::MaxLeverage => "max_leverage",
                Self::MaxDrawdown => "max_drawdown",
                Self::TradeFrequency => "trade_frequency",
                Self::MaxExposure => "max_exposure",
                Self::SlippageCooldown => "slippage_cooldown",
                Self::VolatilityBlock => "volatility_block",
                Self::BrokerLatencyBlock => "broker_latency_block",
                Self::ProfitTargetReached => "profit_target_reached",
                Self::KillSwitchEngaged => "kill_switch_engaged",
                Self::RegimeBlocked => "regime_blocked",
                Self::NewsBlocked => "news_blocked",
                Self::WarModeConfidenceTooLow => "war_mode_confidence_too_low",
                Self::AdaptiveRiskZero => "adaptive_risk_zero",
                Self::InvalidApprovalToken => "invalid_approval_token",
                Self::InternalError => "internal_error",
            }
        }
    }

    impl fmt::Display for RejectionReason {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.as_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use json_schemas::ALL_SCHEMAS;
    use order_state::OrderLifecycleState;
    use rejection_reason::RejectionReason;
    use stage::Stage;
    use strategy_id::StrategyId;

    /// Round-trip every defined `Stage` through `u8` and back.
    #[test]
    fn stage_roundtrip_u8() {
        let all = [
            Stage::TickIngest,
            Stage::FeatureExtraction,
            Stage::AiScoringFetch,
            Stage::RiskCheck,
            Stage::ExecutionRouting,
            Stage::BrokerSubmit,
        ];
        for s in all {
            let byte = s.as_u8();
            assert_eq!(Stage::from_u8(byte), Some(s));
            assert_eq!(byte, s.as_u8());
        }
        assert_eq!(Stage::from_u8(255), None);
    }

    /// Round-trip every defined `OrderLifecycleState` through `u8` and back.
    #[test]
    fn order_state_roundtrip_u8() {
        let all = [
            OrderLifecycleState::New,
            OrderLifecycleState::Submitted,
            OrderLifecycleState::PartiallyFilled,
            OrderLifecycleState::Filled,
            OrderLifecycleState::Cancelled,
            OrderLifecycleState::Rejected,
        ];
        for s in all {
            let byte = s.as_u8();
            assert_eq!(OrderLifecycleState::from_u8(byte), Some(s));
        }
        assert_eq!(OrderLifecycleState::from_u8(99), None);
        assert!(OrderLifecycleState::Filled.is_terminal());
        assert!(!OrderLifecycleState::Submitted.is_terminal());
    }

    /// Round-trip every defined `StrategyId` through `u8` and back.
    #[test]
    fn strategy_id_roundtrip_u8() {
        let all = [
            StrategyId::OpeningRangeBreakout,
            StrategyId::VwapPullback,
            StrategyId::MomentumBreakout,
            StrategyId::LiquiditySweepReversal,
            StrategyId::OptionsOiExpansionBreakout,
            StrategyId::VolatilityCompressionBreakout,
        ];
        for s in all {
            assert_eq!(StrategyId::from_u8(s.as_u8()), Some(s));
        }
        assert_eq!(StrategyId::from_u8(50), None);
    }

    /// Round-trip every defined `RejectionReason` through `u8` and back.
    #[test]
    fn rejection_reason_roundtrip_u8() {
        let all = [
            RejectionReason::Approved,
            RejectionReason::SessionClosed,
            RejectionReason::MaxDailyLoss,
            RejectionReason::MaxPosition,
            RejectionReason::MaxLeverage,
            RejectionReason::MaxDrawdown,
            RejectionReason::TradeFrequency,
            RejectionReason::MaxExposure,
            RejectionReason::SlippageCooldown,
            RejectionReason::VolatilityBlock,
            RejectionReason::BrokerLatencyBlock,
            RejectionReason::ProfitTargetReached,
            RejectionReason::KillSwitchEngaged,
            RejectionReason::RegimeBlocked,
            RejectionReason::NewsBlocked,
            RejectionReason::WarModeConfidenceTooLow,
            RejectionReason::AdaptiveRiskZero,
            RejectionReason::InvalidApprovalToken,
            RejectionReason::InternalError,
        ];
        for r in all {
            assert_eq!(RejectionReason::from_u8(r.as_u8()), Some(r));
        }
        assert_eq!(RejectionReason::from_u8(200), None);
    }

    /// Every JSON schema parses as a JSON object. Validates that the
    /// `include_str!` content is well-formed and that we did not commit a
    /// truncated file.
    #[test]
    fn every_json_schema_is_a_valid_object() {
        assert_eq!(ALL_SCHEMAS.len(), 20, "expected 20 JSON schemas");
        for (name, body) in ALL_SCHEMAS {
            let value: serde_json::Value = serde_json::from_str(body)
                .unwrap_or_else(|e| panic!("schema {name} did not parse: {e}"));
            assert!(value.is_object(), "schema {name} root must be an object");

            let obj = value.as_object().unwrap();
            assert_eq!(
                obj.get("$schema").and_then(|v| v.as_str()),
                Some("https://json-schema.org/draft/2020-12/schema"),
                "schema {name} must declare draft 2020-12",
            );
            assert_eq!(
                obj.get("type").and_then(|v| v.as_str()),
                Some("object"),
                "schema {name} root type must be object",
            );
            assert_eq!(
                obj.get("additionalProperties"),
                Some(&serde_json::Value::Bool(false)),
                "schema {name} must set additionalProperties: false",
            );
            assert!(obj.contains_key("required"), "schema {name} must list required fields");
            assert!(obj.contains_key("properties"), "schema {name} must define properties");
        }
    }

    /// Spot-check that range constraints survived (the design's named
    /// fields all have explicit minimum / maximum where they exist).
    #[test]
    fn ai_rank_score_range_constrained() {
        let v: serde_json::Value = serde_json::from_str(json_schemas::AI_RANK_SCHEMA).unwrap();
        let score = &v["properties"]["trade_confidence_score"];
        assert_eq!(score["minimum"], serde_json::json!(0.0));
        assert_eq!(score["maximum"], serde_json::json!(1.0));
    }

    /// Spot-check that the news sentiment range is `[-1.0, 1.0]` and
    /// impact_magnitude is `[0.0, 1.0]`.
    #[test]
    fn ai_news_impact_ranges() {
        let v: serde_json::Value =
            serde_json::from_str(json_schemas::AI_NEWS_IMPACT_SCHEMA).unwrap();
        let s = &v["properties"]["sentiment"];
        assert_eq!(s["minimum"], serde_json::json!(-1.0));
        assert_eq!(s["maximum"], serde_json::json!(1.0));
        let m = &v["properties"]["impact_magnitude"];
        assert_eq!(m["minimum"], serde_json::json!(0.0));
        assert_eq!(m["maximum"], serde_json::json!(1.0));
    }
}
