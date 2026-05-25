//! [`RiskDecision`] — the typed result of a Risk_Engine evaluation.
//!
//! The Risk_Engine emits exactly one `RiskDecision` per `evaluate` call.
//! `Approved` carries the minted [`ApprovalToken`] and the sized quantity
//! produced by `Adaptive_Risk` (R5.13). `Rejected` carries a stable
//! `RejectionReason` enum so downstream consumers and metrics dashboards
//! can pivot on the cause.
//!
//! Both variants carry a `RiskRationale` describing which gates were
//! evaluated. The rationale is informational — it is not part of the
//! HMAC-protected payload — but it lands in `risk.decision.{approved,
//! rejected}` JSON envelopes for operational visibility (R27.4).

use hedge_schemas::rejection_reason::RejectionReason;
use serde::{Deserialize, Serialize};

use crate::approval::ApprovalToken;

/// Per-gate evaluation diagnostic. `Some(Pass)` means the gate ran and
/// passed; `Some(Reject(reason))` means the gate ran and rejected;
/// `None` means the gate was short-circuited because an earlier gate
/// already rejected (so its result is not informative).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateOutcome {
    /// Gate ran and passed.
    Pass,
    /// Gate ran and rejected with the carried reason.
    Reject(RejectionReason),
}

/// Per-gate trace. The gates are listed in evaluation order so a
/// rejection's `Some(Reject(_))` is always the last `Some(_)` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RiskRationale {
    /// `kill_switch.is_active()` (R5.9).
    pub kill_switch: Option<GateOutcome>,
    /// `[09:15, 15:30] IST` (R31.1).
    pub session_time: Option<GateOutcome>,
    /// `realized + unrealized <= -max_daily_loss` (R5.2).
    pub daily_loss: Option<GateOutcome>,
    /// `drawdown >= max_drawdown` (R5.5).
    pub drawdown: Option<GateOutcome>,
    /// Per-minute / hour / session counters (R5.6).
    pub frequency: Option<GateOutcome>,
    /// Per-symbol / portfolio position cap (R5.3).
    pub position_size: Option<GateOutcome>,
    /// Per-symbol / account leverage cap (R5.4).
    pub leverage: Option<GateOutcome>,
    /// Per-symbol / per-sector exposure cap (R5.7).
    pub exposure: Option<GateOutcome>,
    /// Slippage cooldown (R5.8).
    pub slippage_cooldown: Option<GateOutcome>,
    /// Realized-vol block (R5.10).
    pub volatility_block: Option<GateOutcome>,
    /// Active broker latency block (R5.11).
    pub broker_latency_block: Option<GateOutcome>,
    /// Daily-profit-target post-policy (R32.3, R31.4).
    pub profit_target: Option<GateOutcome>,
    /// `Adaptive_Risk` reduction to zero (R5.13).
    pub adaptive_risk: Option<GateOutcome>,
}

impl RiskRationale {
    /// `true` when every recorded gate is `Pass` (and at least one was
    /// recorded). Useful for tests and assertions.
    pub fn all_passed(&self) -> bool {
        let gates = [
            self.kill_switch,
            self.session_time,
            self.daily_loss,
            self.drawdown,
            self.frequency,
            self.position_size,
            self.leverage,
            self.exposure,
            self.slippage_cooldown,
            self.volatility_block,
            self.broker_latency_block,
            self.profit_target,
            self.adaptive_risk,
        ];
        let any_recorded = gates.iter().any(|g| g.is_some());
        let all_pass = gates
            .iter()
            .all(|g| matches!(g, Some(GateOutcome::Pass) | None));
        any_recorded && all_pass
    }
}

/// The result of an `evaluate` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskDecision {
    /// All gates passed and `Adaptive_Risk` produced a non-zero size.
    Approved {
        /// Single-use HMAC token over the canonical intent bytes.
        token: ApprovalToken,
        /// Sized quantity to forward to the Execution_Engine. Always > 0.
        sized_quantity: u64,
        /// Diagnostic gate trace.
        rationale: RiskRationale,
    },
    /// At least one gate rejected.
    Rejected {
        /// Stable rejection reason carried in the wire payload.
        reason: RejectionReason,
        /// Diagnostic gate trace.
        rationale: RiskRationale,
    },
}

impl RiskDecision {
    /// Convenience accessor — `true` for `Approved`.
    #[inline]
    pub fn is_approved(&self) -> bool {
        matches!(self, Self::Approved { .. })
    }

    /// Convenience accessor — `Some(reason)` for `Rejected`.
    #[inline]
    pub fn rejection_reason(&self) -> Option<RejectionReason> {
        match self {
            Self::Rejected { reason, .. } => Some(*reason),
            _ => None,
        }
    }

    /// Borrow the approval token, if any.
    #[inline]
    pub fn token(&self) -> Option<&ApprovalToken> {
        match self {
            Self::Approved { token, .. } => Some(token),
            _ => None,
        }
    }

    /// The sized quantity, or `0` when rejected.
    #[inline]
    pub fn sized_quantity(&self) -> u64 {
        match self {
            Self::Approved { sized_quantity, .. } => *sized_quantity,
            _ => 0,
        }
    }

    /// Borrow the rationale.
    #[inline]
    pub fn rationale(&self) -> &RiskRationale {
        match self {
            Self::Approved { rationale, .. } => rationale,
            Self::Rejected { rationale, .. } => rationale,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::ApprovalToken;

    #[test]
    fn approved_decision_is_approved() {
        let d = RiskDecision::Approved {
            token: ApprovalToken::from_bytes([0u8; 32]),
            sized_quantity: 7,
            rationale: RiskRationale::default(),
        };
        assert!(d.is_approved());
        assert_eq!(d.sized_quantity(), 7);
        assert_eq!(d.rejection_reason(), None);
        assert!(d.token().is_some());
    }

    #[test]
    fn rejected_decision_carries_reason() {
        let d = RiskDecision::Rejected {
            reason: RejectionReason::SessionClosed,
            rationale: RiskRationale::default(),
        };
        assert!(!d.is_approved());
        assert_eq!(d.sized_quantity(), 0);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::SessionClosed));
        assert!(d.token().is_none());
    }

    #[test]
    fn rationale_all_passed_returns_false_when_any_gate_rejected() {
        let mut r = RiskRationale::default();
        r.session_time = Some(GateOutcome::Pass);
        r.kill_switch = Some(GateOutcome::Reject(RejectionReason::KillSwitchEngaged));
        assert!(!r.all_passed());
    }

    #[test]
    fn rationale_all_passed_returns_true_when_all_recorded_pass() {
        let mut r = RiskRationale::default();
        r.kill_switch = Some(GateOutcome::Pass);
        r.session_time = Some(GateOutcome::Pass);
        assert!(r.all_passed());
    }

    #[test]
    fn rationale_all_passed_returns_false_when_no_gate_recorded() {
        let r = RiskRationale::default();
        assert!(!r.all_passed());
    }
}
