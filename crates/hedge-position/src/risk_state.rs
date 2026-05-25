//! Aggregate trader risk state (R8.5).
//!
//! Published on `pos.risk_state` whenever any [`Position`](crate::Position)
//! changes. The Risk_Engine and the Human_Control_UI consume this event to
//! adjust their `RiskState` and dashboard, respectively.
//!
//! All money values are kept in paise internally (`i64`) so arithmetic stays
//! exact (Property 4); the helpers `*_inr()` divide by 100 for display.
//!
//! ```text
//! aggregate_exposure_paise = Σ |position_i.quantity| × position_i.avg_entry_px
//! total_pnl_paise          = Σ (realized_i + unrealized_i)
//! peak_equity_paise        = max(peak_equity_paise, base_capital + total_pnl_paise)
//! drawdown_paise           = peak_equity_paise - (base_capital + total_pnl_paise)
//! available_margin_paise   = base_capital + realized_pnl_paise - aggregate_exposure_paise
//! ```
//!
//! `drawdown_paise` is always ≥ 0 — equity above the previous peak resets
//! the peak to the new high water mark with `drawdown = 0`.

use serde::{Deserialize, Serialize};

use crate::position::Position;

/// Aggregate risk surface for the entire account, keyed only by time
/// (no per-symbol breakdown). Published on `pos.risk_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraderRiskState {
    /// Sum of |qty| × avg_entry_px across all symbols, in paise.
    pub aggregate_exposure_paise: i64,
    /// Peak-to-current drawdown in paise. Always ≥ 0.
    pub drawdown_paise: i64,
    /// Margin available for new positions, in paise.
    /// `base_capital + realized_pnl - aggregate_exposure`. Can go negative
    /// if the trader is over-leveraged on unrealised losses.
    pub available_margin_paise: i64,
    /// Highest equity value observed so far during the session, in paise.
    /// Equity = `base_capital + realized + unrealized`.
    pub peak_equity_paise: i64,
}

impl TraderRiskState {
    /// Bootstrapping state for a fresh session at the configured base
    /// capital. Equity starts at `base_capital`, drawdown at zero, exposure
    /// and PnL are zero.
    #[inline]
    pub fn fresh(base_capital_paise: i64) -> Self {
        Self {
            aggregate_exposure_paise: 0,
            drawdown_paise: 0,
            available_margin_paise: base_capital_paise,
            peak_equity_paise: base_capital_paise,
        }
    }

    /// Aggregate exposure in whole INR (truncated). Display helper.
    #[inline]
    pub fn aggregate_exposure_inr(&self) -> i64 {
        self.aggregate_exposure_paise / 100
    }

    /// Drawdown in whole INR (truncated). Display helper.
    #[inline]
    pub fn drawdown_inr(&self) -> i64 {
        self.drawdown_paise / 100
    }

    /// Available margin in whole INR (truncated). Display helper.
    #[inline]
    pub fn available_margin_inr(&self) -> i64 {
        self.available_margin_paise / 100
    }

    /// Peak equity in whole INR (truncated). Display helper.
    #[inline]
    pub fn peak_equity_inr(&self) -> i64 {
        self.peak_equity_paise / 100
    }
}

/// Recompute [`TraderRiskState`] from the current positions and the previous
/// peak equity. The caller threads the previous peak through every call so
/// `peak_equity_paise` is monotonic across the session.
///
/// `base_capital_paise` is `capital.base_inr × 100` (R32.1 default ₹20,000
/// → 2,000,000 paise).
///
/// Returns the new state. The caller may persist `state.peak_equity_paise`
/// and pass it back as `previous_peak_equity_paise` on the next call.
#[inline]
pub fn aggregate_state<'a, I>(
    positions: I,
    base_capital_paise: i64,
    previous_peak_equity_paise: i64,
) -> TraderRiskState
where
    I: IntoIterator<Item = &'a Position>,
{
    let mut aggregate_exposure: i128 = 0;
    let mut realized: i128 = 0;
    let mut unrealized: i128 = 0;

    for p in positions {
        aggregate_exposure += p.exposure_paise() as i128;
        realized += p.realized_pnl_paise as i128;
        unrealized += p.unrealized_pnl_paise as i128;
    }

    let total_pnl = realized + unrealized;
    let equity = base_capital_paise as i128 + total_pnl;

    let new_peak = equity.max(previous_peak_equity_paise as i128);
    // Drawdown is non-negative by construction (peak ≥ equity).
    let drawdown = (new_peak - equity).max(0);

    // available_margin = base_capital + realized - aggregate_exposure.
    // We exclude unrealised PnL because brokers do not extend margin against
    // open-position MTM; this matches the design's "used margin / available
    // margin" framing in R8.4 / R8.5.
    let available_margin = base_capital_paise as i128 + realized - aggregate_exposure;

    TraderRiskState {
        aggregate_exposure_paise: clamp_to_i64(aggregate_exposure),
        drawdown_paise: clamp_to_i64(drawdown),
        available_margin_paise: clamp_to_i64(available_margin),
        peak_equity_paise: clamp_to_i64(new_peak),
    }
}

#[inline]
fn clamp_to_i64(v: i128) -> i64 {
    if v > i64::MAX as i128 {
        i64::MAX
    } else if v < i64::MIN as i128 {
        i64::MIN
    } else {
        v as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_core::{Px, Side, SymbolId};

    fn px(paise: i64) -> Px {
        Px::from_paise(paise)
    }

    fn pos(symbol: u32) -> Position {
        Position::flat(SymbolId::new(symbol))
    }

    const BASE_CAPITAL: i64 = 20_000 * 100; // ₹20,000 → 2_000_000 paise.

    #[test]
    fn fresh_state_starts_at_base_capital() {
        let s = TraderRiskState::fresh(BASE_CAPITAL);
        assert_eq!(s.aggregate_exposure_paise, 0);
        assert_eq!(s.drawdown_paise, 0);
        assert_eq!(s.available_margin_paise, BASE_CAPITAL);
        assert_eq!(s.peak_equity_paise, BASE_CAPITAL);
    }

    #[test]
    fn aggregate_with_no_positions_yields_fresh() {
        let s = aggregate_state(std::iter::empty(), BASE_CAPITAL, BASE_CAPITAL);
        assert_eq!(s, TraderRiskState::fresh(BASE_CAPITAL));
    }

    #[test]
    fn open_long_uses_exposure_and_consumes_margin() {
        let mut p = pos(1);
        p.apply_fill(Side::Buy, 10, px(100_00));
        let s = aggregate_state([&p], BASE_CAPITAL, BASE_CAPITAL);
        assert_eq!(s.aggregate_exposure_paise, 10 * 100_00);
        assert_eq!(s.available_margin_paise, BASE_CAPITAL - 10 * 100_00);
        assert_eq!(s.drawdown_paise, 0); // no PnL movement yet.
        assert_eq!(s.peak_equity_paise, BASE_CAPITAL);
    }

    #[test]
    fn drawdown_grows_on_losing_unrealized() {
        let mut p = pos(1);
        p.apply_fill(Side::Buy, 10, px(100_00));
        // Mark drops to 90 → unrealised = -10/unit × 10 = -100 paise * 10? No:
        // unrealized = (90-100) * 10 paise/unit ... wait — diff is paise:
        // (9000 - 10000) * 10 = -10000 paise.
        p.apply_mark(px(90_00));
        assert_eq!(p.unrealized_pnl_paise, -10_00 * 10);

        let s = aggregate_state([&p], BASE_CAPITAL, BASE_CAPITAL);
        // Equity = 2_000_000 + 0 + (-10_000) = 1_990_000.
        // Peak still 2_000_000 → drawdown = 10_000.
        assert_eq!(s.drawdown_paise, 10_00 * 10);
        assert_eq!(s.peak_equity_paise, BASE_CAPITAL);
    }

    #[test]
    fn peak_equity_advances_on_winning_unrealized() {
        let mut p = pos(1);
        p.apply_fill(Side::Buy, 10, px(100_00));
        p.apply_mark(px(110_00)); // unrealised = +10_000 paise.

        let s = aggregate_state([&p], BASE_CAPITAL, BASE_CAPITAL);
        assert_eq!(s.peak_equity_paise, BASE_CAPITAL + 10_00 * 10);
        assert_eq!(s.drawdown_paise, 0);
    }

    #[test]
    fn drawdown_grows_after_a_realized_loss() {
        let mut p = pos(1);
        // Open and close at a loss: realised = -10_000 paise.
        p.apply_fill(Side::Buy, 10, px(100_00));
        p.apply_fill(Side::Sell, 10, px(90_00));
        assert_eq!(p.realized_pnl_paise, -10_00 * 10);

        let s = aggregate_state([&p], BASE_CAPITAL, BASE_CAPITAL);
        // Equity = base + realized = base - 10_000.
        assert_eq!(s.drawdown_paise, 10_00 * 10);
        // Available margin recovers exposure (position is flat) but reflects realised:
        assert_eq!(s.available_margin_paise, BASE_CAPITAL - 10_00 * 10);
    }

    #[test]
    fn peak_equity_carries_over_across_calls() {
        // Move up, peak advances. Then move down, drawdown reflects peak.
        let mut p = pos(1);
        p.apply_fill(Side::Buy, 10, px(100_00));
        p.apply_mark(px(120_00));
        let s1 = aggregate_state([&p], BASE_CAPITAL, BASE_CAPITAL);
        assert_eq!(s1.peak_equity_paise, BASE_CAPITAL + 20_00 * 10);

        // Mark drops to 110 → unrealised = +10*10 = 100_paise * 100 = 10_000 paise.
        p.apply_mark(px(110_00));
        let s2 = aggregate_state([&p], BASE_CAPITAL, s1.peak_equity_paise);
        // Peak holds at the previous high; drawdown = peak - equity = 100_000.
        assert_eq!(s2.peak_equity_paise, s1.peak_equity_paise);
        assert_eq!(s2.drawdown_paise, 10_00 * 10);
    }

    #[test]
    fn available_margin_recovers_when_position_closes() {
        let mut p = pos(1);
        p.apply_fill(Side::Buy, 10, px(100_00));
        let s_open = aggregate_state([&p], BASE_CAPITAL, BASE_CAPITAL);
        assert_eq!(s_open.available_margin_paise, BASE_CAPITAL - 10 * 100_00);

        // Close at break-even: exposure goes to 0.
        p.apply_fill(Side::Sell, 10, px(100_00));
        let s_close = aggregate_state([&p], BASE_CAPITAL, s_open.peak_equity_paise);
        assert_eq!(s_close.aggregate_exposure_paise, 0);
        assert_eq!(s_close.available_margin_paise, BASE_CAPITAL);
    }

    #[test]
    fn aggregates_multiple_symbols() {
        let mut a = pos(1);
        let mut b = pos(2);
        a.apply_fill(Side::Buy, 10, px(100_00));
        b.apply_fill(Side::Sell, 5, px(200_00));

        let s = aggregate_state([&a, &b], BASE_CAPITAL, BASE_CAPITAL);
        // |10|*100 + |5|*200 paise = 100_000 + 100_000 = 200_000.
        assert_eq!(s.aggregate_exposure_paise, 10 * 100_00 + 5 * 200_00);
    }

    #[test]
    fn display_helpers_truncate_to_inr() {
        let s = TraderRiskState {
            aggregate_exposure_paise: 1_234_56,
            drawdown_paise: 50_99,
            available_margin_paise: 19_900_00,
            peak_equity_paise: 21_000_50,
        };
        assert_eq!(s.aggregate_exposure_inr(), 1234);
        assert_eq!(s.drawdown_inr(), 50);
        assert_eq!(s.available_margin_inr(), 19_900);
        assert_eq!(s.peak_equity_inr(), 21_000);
    }
}
