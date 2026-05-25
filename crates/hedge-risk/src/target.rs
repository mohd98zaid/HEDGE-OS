//! Daily-profit-target tracker (R32.2, R32.3, R31.4).
//!
//! Once cumulative realized PnL crosses
//! `capital.daily_profit_target_max_inr`, the Risk_Engine emits
//! `risk.target.reached` exactly once and applies the configured
//! [`PostTargetPolicy`](hedge_config::PostTargetPolicy):
//!
//! | Policy | Behaviour after target hit |
//! |---|---|
//! | `ReduceSizeToZero` | New entries rejected with `ProfitTargetReached` |
//! | `StopForSession` | Same; semantically equivalent for the engine path |
//! | `HalveSize` | Sized quantity is halved; entries still admitted |
//! | `Continue` | Entry still admitted at full size; emission is informational |
//!
//! ### Edge-triggered emission (Property 8)
//!
//! [`record_realized`](TargetState::record_realized) returns
//! [`TargetTransition::Crossed`] **only on the first crossing** of the
//! upper band. Subsequent calls — even with higher PnL — return
//! [`TargetTransition::Steady`] so the Risk_Engine emits exactly one
//! `risk.target.reached`.

use hedge_config::PostTargetPolicy;
use serde::{Deserialize, Serialize};

/// Outcome of a `record_realized` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetTransition {
    /// No transition: either still below the upper band, or already
    /// above it.
    Steady,
    /// First crossing of the upper band — the Risk_Engine should emit
    /// `risk.target.reached` exactly once on this transition.
    Crossed,
}

/// Daily-profit-target tracking state.
#[derive(Debug)]
pub struct TargetState {
    /// Cumulative realized PnL for the session, in paise.
    realized_pnl_paise: i64,
    /// Upper band (`daily_profit_target_max_inr × 100`, in paise).
    upper_band_paise: i64,
    /// Whether we have already crossed the upper band this session.
    crossed: bool,
    /// The configured policy applied once the band is crossed.
    policy: PostTargetPolicy,
}

impl TargetState {
    /// Construct a tracker. `upper_band_inr` is the rupees value from
    /// `capital.daily_profit_target_max_inr` (R32.2). The constructor
    /// converts it to paise internally.
    pub fn new(upper_band_inr: u32, policy: PostTargetPolicy) -> Self {
        Self {
            realized_pnl_paise: 0,
            upper_band_paise: i64::from(upper_band_inr) * 100,
            crossed: false,
            policy,
        }
    }

    /// Reset the tracker — typically wired to `ops.session.start`.
    pub fn reset_session(&mut self) {
        self.realized_pnl_paise = 0;
        self.crossed = false;
    }

    /// Record a fresh cumulative realized PnL value.
    ///
    /// `realized_pnl_paise` is the **session total**, not a delta. The
    /// caller computes the running total elsewhere (typically the
    /// Position_Engine via `pos.risk_state`) and passes the absolute
    /// value here.
    pub fn record_realized(&mut self, realized_pnl_paise: i64) -> TargetTransition {
        self.realized_pnl_paise = realized_pnl_paise;
        if !self.crossed && realized_pnl_paise >= self.upper_band_paise {
            self.crossed = true;
            TargetTransition::Crossed
        } else {
            TargetTransition::Steady
        }
    }

    /// `true` when the session has already crossed the upper band.
    #[inline]
    pub fn is_reached(&self) -> bool {
        self.crossed
    }

    /// The configured policy.
    #[inline]
    pub fn policy(&self) -> PostTargetPolicy {
        self.policy
    }

    /// Returns `true` when the current policy + reached state should
    /// reject a fresh entry. (`ReduceSizeToZero` and `StopForSession`
    /// both block; `HalveSize` and `Continue` admit but with size
    /// adjustments handled by the engine.)
    #[inline]
    pub fn should_reject(&self) -> bool {
        self.crossed
            && matches!(
                self.policy,
                PostTargetPolicy::ReduceSizeToZero | PostTargetPolicy::StopForSession
            )
    }

    /// Returns `true` when the current policy + reached state should
    /// halve the sized quantity (`HalveSize` only).
    #[inline]
    pub fn should_halve(&self) -> bool {
        self.crossed && matches!(self.policy, PostTargetPolicy::HalveSize)
    }

    /// Borrow the cumulative realized PnL recorded.
    #[inline]
    pub fn realized_pnl_paise(&self) -> i64 {
        self.realized_pnl_paise
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_tracker_is_not_reached() {
        let t = TargetState::new(1_000, PostTargetPolicy::ReduceSizeToZero);
        assert!(!t.is_reached());
        assert!(!t.should_reject());
    }

    #[test]
    fn first_crossing_returns_crossed_then_steady() {
        let mut t = TargetState::new(1_000, PostTargetPolicy::ReduceSizeToZero);
        // Below upper band → steady.
        let tr = t.record_realized(50_000); // ₹500 in paise
        assert_eq!(tr, TargetTransition::Steady);
        // Cross the upper band (₹1_000 = 100_000 paise).
        let tr = t.record_realized(100_000);
        assert_eq!(tr, TargetTransition::Crossed);
        // Subsequent updates remain steady — exactly-one transition.
        let tr = t.record_realized(150_000);
        assert_eq!(tr, TargetTransition::Steady);
        let tr = t.record_realized(200_000);
        assert_eq!(tr, TargetTransition::Steady);
    }

    #[test]
    fn reduce_to_zero_policy_blocks_entries_after_crossing() {
        let mut t = TargetState::new(1_000, PostTargetPolicy::ReduceSizeToZero);
        t.record_realized(100_000);
        assert!(t.should_reject());
        assert!(!t.should_halve());
    }

    #[test]
    fn stop_for_session_policy_blocks_entries_after_crossing() {
        let mut t = TargetState::new(1_000, PostTargetPolicy::StopForSession);
        t.record_realized(100_000);
        assert!(t.should_reject());
    }

    #[test]
    fn halve_size_policy_does_not_block_but_halves() {
        let mut t = TargetState::new(1_000, PostTargetPolicy::HalveSize);
        t.record_realized(100_000);
        assert!(!t.should_reject());
        assert!(t.should_halve());
    }

    #[test]
    fn continue_policy_admits_at_full_size() {
        let mut t = TargetState::new(1_000, PostTargetPolicy::Continue);
        t.record_realized(100_000);
        assert!(!t.should_reject());
        assert!(!t.should_halve());
    }

    #[test]
    fn reset_session_clears_crossed_flag() {
        let mut t = TargetState::new(1_000, PostTargetPolicy::ReduceSizeToZero);
        t.record_realized(100_000);
        assert!(t.is_reached());
        t.reset_session();
        assert!(!t.is_reached());
        // Subsequent crossing emits again.
        let tr = t.record_realized(100_000);
        assert_eq!(tr, TargetTransition::Crossed);
    }
}
