//! Kill_Switch (R5.5, R5.9, R16.7).
//!
//! A single atomic flag with an associated reason. Activation blocks every
//! subsequent `evaluate()` call (R5.9) and is **edge-triggered** —
//! re-activating an already-active switch is a no-op (Property 8). Only
//! the first transition from `false → true` produces a
//! `risk.killswitch.activated` event, and only that first activation
//! records the reason.
//!
//! ### Concurrency
//!
//! `active` is an `AtomicBool` for lock-free reads on the steady-state
//! happy path. The `reason` text is held behind a `parking_lot::Mutex` and
//! is mutated only on the (cold) transition, so the lock contention is
//! negligible.

use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

/// Categorical reason for a Kill_Switch activation.
///
/// Wire-stable: discriminant values must not be reordered. The trailing
/// fields are documentation aids only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum KillReason {
    /// Drawdown limit breached (R5.5) — the engine self-engages on this
    /// path, no external trigger required.
    MaxDrawdown = 0,
    /// Trader request from the UI (`trader.intent.killswitch`, R20.6).
    TraderRequest = 1,
    /// Trader_Psychology_Engine critical threshold (R16.7).
    PsychologyCritical = 2,
    /// Self_Healing_Supervisor manual halt (R20.6, R23.2).
    SupervisorHalt = 3,
    /// AI_Governance critical drift detected (R23.2 second branch).
    AiGovernanceCritical = 4,
}

impl KillReason {
    /// Stable canonical short string for metrics and structured logs.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MaxDrawdown => "max_drawdown",
            Self::TraderRequest => "trader_request",
            Self::PsychologyCritical => "psychology_critical",
            Self::SupervisorHalt => "supervisor_halt",
            Self::AiGovernanceCritical => "ai_governance_critical",
        }
    }
}

/// Process-local Kill_Switch state.
///
/// Cheap to construct, cheap to read on the hot path. The transition write
/// path takes a brief `parking_lot::Mutex` lock to record the reason and
/// signal "first activation".
pub struct KillSwitchState {
    active: AtomicBool,
    reason: parking_lot::Mutex<Option<KillReason>>,
}

impl KillSwitchState {
    /// Construct a fresh Kill_Switch in the inactive state.
    pub const fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            // `parking_lot::Mutex::new` is `const fn` since 0.12.
            reason: parking_lot::Mutex::new(None),
        }
    }

    /// Returns `true` while the switch is engaged.
    ///
    /// Hot_Path readers call this first thing in `evaluate()` (R5.9) so
    /// it must be a single relaxed atomic load with no further synchronization.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Attempt to engage the switch.
    ///
    /// Returns `true` when this call performed the `false → true`
    /// transition (so the caller is responsible for emitting the
    /// `risk.killswitch.activated` event). Returns `false` when the
    /// switch was already engaged — guaranteeing exactly-once edge
    /// emission per Property 8.
    pub fn activate(&self, reason: KillReason) -> bool {
        // `compare_exchange` returns `Ok` exactly when the previous value
        // was the expected one — the canonical "lock-free transition"
        // pattern. We use `AcqRel` so the reason write below
        // happens-before any subsequent reader observing `active = true`.
        let transitioned = self
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if transitioned {
            *self.reason.lock() = Some(reason);
        }
        transitioned
    }

    /// Reset the switch to inactive. Used by replay / test harnesses; in
    /// production the supervisor opts to restart the process rather than
    /// reset the switch in place.
    pub fn reset(&self) {
        // Drop the reason first, *then* clear the flag, so a concurrent
        // reader that observes `active = false` cannot observe a stale
        // reason from the previous activation.
        *self.reason.lock() = None;
        self.active.store(false, Ordering::Release);
    }

    /// Borrow a copy of the recorded reason (`None` while inactive or in
    /// the brief window before the reason has been recorded).
    #[inline]
    pub fn reason(&self) -> Option<KillReason> {
        *self.reason.lock()
    }
}

impl Default for KillSwitchState {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for KillSwitchState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KillSwitchState")
            .field("active", &self.is_active())
            .field("reason", &self.reason())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_kill_switch_is_inactive_with_no_reason() {
        let k = KillSwitchState::new();
        assert!(!k.is_active());
        assert_eq!(k.reason(), None);
    }

    #[test]
    fn activate_returns_true_only_on_first_transition() {
        let k = KillSwitchState::new();
        assert!(k.activate(KillReason::MaxDrawdown), "first activate transitions");
        assert!(!k.activate(KillReason::TraderRequest), "second activate is a no-op");
        assert!(!k.activate(KillReason::PsychologyCritical), "third activate is a no-op");
        // The reason recorded is the *first* one — re-activation does not
        // overwrite.
        assert_eq!(k.reason(), Some(KillReason::MaxDrawdown));
        assert!(k.is_active());
    }

    #[test]
    fn reset_clears_state_and_allows_subsequent_activate() {
        let k = KillSwitchState::new();
        assert!(k.activate(KillReason::TraderRequest));
        k.reset();
        assert!(!k.is_active());
        assert_eq!(k.reason(), None);
        assert!(k.activate(KillReason::PsychologyCritical));
        assert_eq!(k.reason(), Some(KillReason::PsychologyCritical));
    }

    #[test]
    fn kill_reason_str_is_stable() {
        assert_eq!(KillReason::MaxDrawdown.as_str(), "max_drawdown");
        assert_eq!(KillReason::TraderRequest.as_str(), "trader_request");
        assert_eq!(KillReason::PsychologyCritical.as_str(), "psychology_critical");
        assert_eq!(KillReason::SupervisorHalt.as_str(), "supervisor_halt");
        assert_eq!(KillReason::AiGovernanceCritical.as_str(), "ai_governance_critical");
    }

    #[test]
    fn concurrent_activate_only_one_caller_observes_transition() {
        // Property 8: edge-triggered. With many threads racing to engage,
        // exactly one observes `transitioned = true`.
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        let k = Arc::new(KillSwitchState::new());
        let wins = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let kc = Arc::clone(&k);
            let wc = Arc::clone(&wins);
            handles.push(std::thread::spawn(move || {
                if kc.activate(KillReason::MaxDrawdown) {
                    wc.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(wins.load(Ordering::Relaxed), 1, "exactly one transition");
        assert!(k.is_active());
    }
}
