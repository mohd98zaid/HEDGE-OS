//! Single-byte tagged enums shared across the Hot_Path.
//!
//! Every enum in this module is `#[repr(u8)]` so it fits in a FlatBuffers
//! `ubyte` field (matching the design's `Tick_v1.exchange`, `Signal_v1.side`,
//! `Signal_v1.strategy`, and the `Regime`/`Priority`/`BrokerId` payloads in
//! `ai.regime.changed`, `ai.priority.changed.<sym>`, and `broker.metric.*`).
//!
//! The discriminant values are explicit and stable. **Do not renumber** —
//! they are part of the FlatBuffers wire contract.

use serde::{Deserialize, Serialize};

/// Order direction. Matches `Signal_v1.side` (R4.2) and `OrderIntent_v1.side`.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    /// Long entry / cover-short.
    Buy = 0,
    /// Short entry / exit-long.
    Sell = 1,
}

impl Side {
    /// Returns the opposite side. Useful when computing closing-order intents
    /// from a held position.
    #[inline]
    pub const fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    /// Returns the byte tag (matches the FlatBuffers wire value).
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Market regime classification published by the Market_Regime_Engine
/// (R13.1, design § Components — Market_Regime_Engine).
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Regime {
    /// Sustained directional move with rising volume.
    Trending = 0,
    /// Mean-reverting range-bound conditions.
    Sideways = 1,
    /// Sharp risk-off move; volatility spikes.
    Panic = 2,
    /// Realized vol exceeds the configured high threshold.
    HighVolatility = 3,
    /// Price action driven primarily by news catalysts.
    NewsDriven = 4,
    /// Top-of-book liquidity collapses.
    LiquidityCrisis = 5,
    /// Volume well below the rolling average; thin order flow.
    LowParticipation = 6,
}

impl Regime {
    /// Returns the byte tag.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Broker identifier (R7.1). The `Simulated` variant is bound by the
/// Execution_Engine when `ReplayMode::On` (R22.4).
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BrokerId {
    /// Zerodha Kite (default primary broker).
    Zerodha = 0,
    /// Dhan.co.
    Dhan = 1,
    /// Shoonya / Finvasia.
    Shoonya = 2,
    /// Angel One SmartAPI.
    AngelOne = 3,
    /// Upstox API v2.
    Upstox = 4,
    /// In-process simulated broker used in replay and tests.
    Simulated = 255,
}

impl BrokerId {
    /// Returns the byte tag.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Symbol priority tier (R14.1). Determines CPU, AI inference, scan frequency,
/// and alert frequency budget per the `PriorityAllocationTable`.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    /// Highest priority: maximum scan frequency and AI inference budget.
    P1 = 1,
    /// Standard priority.
    P2 = 2,
    /// Lower priority; reduced scan frequency.
    P3 = 3,
    /// Background tier; alerts only.
    P4 = 4,
}

impl Priority {
    /// Returns the byte tag.
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_opposite_is_involutive() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
        assert_eq!(Side::Buy.opposite().opposite(), Side::Buy);
    }

    #[test]
    fn enum_byte_tags_are_stable() {
        // Wire-format guard: changing these values is an ABI break.
        assert_eq!(Side::Buy.as_u8(), 0);
        assert_eq!(Side::Sell.as_u8(), 1);

        assert_eq!(Regime::Trending.as_u8(), 0);
        assert_eq!(Regime::LowParticipation.as_u8(), 6);

        assert_eq!(BrokerId::Zerodha.as_u8(), 0);
        assert_eq!(BrokerId::Simulated.as_u8(), 255);

        assert_eq!(Priority::P1.as_u8(), 1);
        assert_eq!(Priority::P4.as_u8(), 4);
    }

    #[test]
    fn enums_fit_in_one_byte() {
        // FlatBuffers `ubyte` compatibility (R1.5).
        assert_eq!(std::mem::size_of::<Side>(), 1);
        assert_eq!(std::mem::size_of::<Regime>(), 1);
        assert_eq!(std::mem::size_of::<BrokerId>(), 1);
        assert_eq!(std::mem::size_of::<Priority>(), 1);
    }

    #[test]
    fn priority_ordering_is_p1_then_down() {
        // P1 is "highest priority"; the derived Ord places it lowest, so we
        // document the convention here. Hot_Path consumers compare by
        // discriminant explicitly when they want "P1 first".
        assert!(Priority::P1 < Priority::P2);
        assert!(Priority::P3 < Priority::P4);
    }
}
