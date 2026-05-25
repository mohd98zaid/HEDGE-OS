//! Aggregate Risk_Engine state (R5.2–R5.11, R31.1, R31.4, R32.3).
//!
//! Holds the mutable parts the engine reads on every `evaluate` call:
//!
//! * Realized / unrealized PnL — fed by `pos.risk_state` (R8.5).
//! * Drawdown peak/current — derived from peak equity less current equity.
//! * Frequency counters (per-minute / per-hour / per-session, R5.6).
//! * Slippage cooldown registry (R5.8).
//! * Volatility table (R5.10).
//! * Broker latency table (R5.11).
//! * Daily-profit-target tracker (R32.3, R31.4).
//!
//! Concurrency: per the brief, the mutable parts live behind a single
//! [`parking_lot::Mutex<RiskState>`] and the kill_switch lives outside the
//! mutex as an atomic. The Risk_Engine evaluator is single-threaded on
//! the Hot_Path so contention is minimal — the lock exists to allow the
//! NATS subscriber tasks (one per subject domain) to mutate shared state
//! between evaluations without `unsafe`.

use std::collections::BTreeMap;

use hedge_config::{CapitalConfig, RiskConfig};
use hedge_core::BrokerId;

use crate::cooldown::CooldownRegistry;
use crate::frequency::FrequencyCounters;
use crate::target::TargetState;
use crate::volatility::VolatilityTable;

/// Per-broker latency tracker. Stores the most recent round-trip
/// latency (in ms) per broker; the Risk_Engine consults this table
/// during `evaluate` and rejects with [`RejectionReason::BrokerLatencyBlock`]
/// when the active broker's latency exceeds `config.broker_latency_block_ms`.
#[derive(Debug, Default)]
pub struct BrokerLatencyTable {
    inner: BTreeMap<BrokerId, u32>,
}

impl BrokerLatencyTable {
    /// Construct an empty table.
    pub const fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Record the latest latency reading (ms) for a broker.
    pub fn record(&mut self, broker: BrokerId, latency_ms: u32) {
        self.inner.insert(broker, latency_ms);
    }

    /// Returns `true` when `broker`'s latest reading exceeds `threshold_ms`.
    /// A broker with no reading is considered unblocked.
    #[inline]
    pub fn is_blocked(&self, broker: BrokerId, threshold_ms: u32) -> bool {
        match self.inner.get(&broker) {
            Some(v) => *v > threshold_ms,
            None => false,
        }
    }

    /// Borrow the most recent reading for `broker`, if any.
    #[inline]
    pub fn latency_ms(&self, broker: BrokerId) -> Option<u32> {
        self.inner.get(&broker).copied()
    }
}

/// Aggregate Risk_Engine state — the mutable half.
pub struct RiskState {
    /// Realized PnL for the session, in paise.
    pub realized_pnl_paise: i64,
    /// Unrealized PnL (mark-to-market on open positions), in paise.
    pub unrealized_pnl_paise: i64,
    /// Current drawdown in paise (peak equity − current equity).
    pub drawdown_paise: i64,
    /// Peak equity observed this session, in paise. Initialised to the
    /// configured capital base on `reset_session`.
    pub peak_equity_paise: i64,
    /// Aggregate position size held across all symbols (units).
    pub portfolio_position: u64,
    /// Per-symbol position sizes, keyed by `SymbolId`.
    pub per_symbol_position: BTreeMap<hedge_core::SymbolId, u64>,
    /// Per-symbol notional exposure, in paise.
    pub per_symbol_exposure_paise: BTreeMap<hedge_core::SymbolId, i64>,
    /// Per-sector notional exposure (sector key as u32), in paise.
    pub per_sector_exposure_paise: BTreeMap<u32, i64>,
    /// Aggregate notional exposure, in paise — used for leverage check.
    pub aggregate_notional_paise: i64,
    /// Frequency counters (R5.6).
    pub frequency: FrequencyCounters,
    /// Slippage cooldown registry (R5.8).
    pub cooldowns: CooldownRegistry,
    /// Volatility table (R5.10).
    pub volatility: VolatilityTable,
    /// Broker latency table (R5.11).
    pub broker_latency: BrokerLatencyTable,
    /// Daily-profit-target tracker (R32.3).
    pub target: TargetState,
}

impl RiskState {
    /// Construct fresh state seeded from the configured capital base
    /// and post-target policy.
    pub fn new(capital: &CapitalConfig, _risk: &RiskConfig) -> Self {
        let initial_equity_paise = i64::from(capital.base_inr) * 100;
        Self {
            realized_pnl_paise: 0,
            unrealized_pnl_paise: 0,
            drawdown_paise: 0,
            peak_equity_paise: initial_equity_paise,
            portfolio_position: 0,
            per_symbol_position: BTreeMap::new(),
            per_symbol_exposure_paise: BTreeMap::new(),
            per_sector_exposure_paise: BTreeMap::new(),
            aggregate_notional_paise: 0,
            frequency: FrequencyCounters::new(),
            cooldowns: CooldownRegistry::new(),
            volatility: VolatilityTable::new(),
            broker_latency: BrokerLatencyTable::new(),
            target: TargetState::new(capital.daily_profit_target_max_inr, capital.post_target_policy),
        }
    }

    /// Reset the per-session counters — wired to `ops.session.start`.
    pub fn reset_session(&mut self, capital: &CapitalConfig) {
        self.realized_pnl_paise = 0;
        self.unrealized_pnl_paise = 0;
        self.drawdown_paise = 0;
        self.peak_equity_paise = i64::from(capital.base_inr) * 100;
        self.portfolio_position = 0;
        self.per_symbol_position.clear();
        self.per_symbol_exposure_paise.clear();
        self.per_sector_exposure_paise.clear();
        self.aggregate_notional_paise = 0;
        self.frequency.reset_session();
        self.target.reset_session();
        // Cooldowns intentionally NOT cleared on session reset — they may
        // span the open if engaged near close. Volatility / broker
        // latency tables likewise persist across the boundary.
    }

    /// Update PnL aggregates. Recomputes drawdown as
    /// `peak_equity − (capital_base + realized + unrealized)`.
    pub fn update_pnl(
        &mut self,
        realized_paise: i64,
        unrealized_paise: i64,
        capital: &CapitalConfig,
    ) {
        self.realized_pnl_paise = realized_paise;
        self.unrealized_pnl_paise = unrealized_paise;
        let capital_paise = i64::from(capital.base_inr) * 100;
        let current_equity = capital_paise
            .saturating_add(realized_paise)
            .saturating_add(unrealized_paise);
        if current_equity > self.peak_equity_paise {
            self.peak_equity_paise = current_equity;
        }
        self.drawdown_paise = self.peak_equity_paise.saturating_sub(current_equity);
    }

    /// Total PnL: realized + unrealized.
    #[inline]
    pub fn total_pnl_paise(&self) -> i64 {
        self.realized_pnl_paise.saturating_add(self.unrealized_pnl_paise)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_config::{defaults, PostTargetPolicy};

    #[test]
    fn fresh_state_seeds_peak_equity_to_capital_base() {
        let cap = defaults::capital();
        let risk = defaults::risk();
        let s = RiskState::new(&cap, &risk);
        // 20_000 INR = 2_000_000 paise.
        assert_eq!(s.peak_equity_paise, 2_000_000);
        assert_eq!(s.realized_pnl_paise, 0);
        assert_eq!(s.unrealized_pnl_paise, 0);
        assert_eq!(s.drawdown_paise, 0);
    }

    #[test]
    fn update_pnl_advances_peak_equity_on_gains() {
        let cap = defaults::capital();
        let risk = defaults::risk();
        let mut s = RiskState::new(&cap, &risk);
        // +₹500 realized.
        s.update_pnl(50_000, 0, &cap);
        assert_eq!(s.peak_equity_paise, 2_050_000);
        assert_eq!(s.drawdown_paise, 0);
    }

    #[test]
    fn update_pnl_grows_drawdown_on_losses() {
        let cap = defaults::capital();
        let risk = defaults::risk();
        let mut s = RiskState::new(&cap, &risk);
        // First climb to a peak.
        s.update_pnl(50_000, 0, &cap);
        assert_eq!(s.peak_equity_paise, 2_050_000);
        // Then lose ₹200 from the peak.
        s.update_pnl(50_000, -20_000, &cap);
        assert_eq!(s.drawdown_paise, 20_000);
    }

    #[test]
    fn reset_session_zeroes_pnl_and_peak() {
        let cap = defaults::capital();
        let risk = defaults::risk();
        let mut s = RiskState::new(&cap, &risk);
        s.update_pnl(100_000, 0, &cap);
        s.reset_session(&cap);
        assert_eq!(s.realized_pnl_paise, 0);
        assert_eq!(s.peak_equity_paise, 2_000_000);
    }

    #[test]
    fn broker_latency_table_round_trip() {
        let mut t = BrokerLatencyTable::new();
        t.record(BrokerId::Zerodha, 50);
        assert_eq!(t.latency_ms(BrokerId::Zerodha), Some(50));
        assert!(!t.is_blocked(BrokerId::Zerodha, 100));
        assert!(t.is_blocked(BrokerId::Zerodha, 25));
        assert!(!t.is_blocked(BrokerId::Dhan, 100), "no reading → unblocked");
    }

    #[test]
    fn target_state_propagates_post_target_policy() {
        let cap = defaults::capital();
        let risk = defaults::risk();
        let s = RiskState::new(&cap, &risk);
        assert_eq!(s.target.policy(), PostTargetPolicy::ReduceSizeToZero);
    }
}
