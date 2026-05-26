//! [`ReplayMode`] — the runtime flag that forces the Execution_Engine to
//! bind to [`hedge_broker_simulated::SimulatedBroker`] when set to
//! [`ReplayMode::On`] (R22.4).
//!
//! ### Wiring contract (design § Replay-Recording Flow)
//!
//! The Replay_Engine deliberately does **not** depend on
//! `hedge-broker-simulated` or `hedge-exec` at the crate level. Instead
//! the wiring is config-driven:
//!
//! 1. The recorder/player set [`ReplayMode::On`] at startup when running
//!    against a recorded session, by writing the value to either
//!    [`hedge_warmcache`](https://docs.rs/hedge-warmcache) or to the
//!    `replay.replay_mode` field in the loaded `HedgeConfig`.
//! 2. The Execution_Engine reads the flag at startup and, when on,
//!    constructs its `BrokerAdapter` as a [`SimulatedBroker`] rather than
//!    a live broker. The flag is stable for the lifetime of the process
//!    so the engine never has to re-check on the per-tick path.
//!
//! Keeping the linkage at the config layer rather than at the type
//! layer means:
//!
//! * the recorder/player never link to broker code (smaller hot-path
//!   image, no transitive cloud-LLM-SDK risk through broker REST
//!   shims);
//! * the Execution_Engine retains its single source of truth for the
//!   adapter choice (its own startup builder);
//! * the contract is testable in isolation: `ReplayMode::On.is_replay()`
//!   is a pure boolean check the engine asserts in its constructor.

use serde::{Deserialize, Serialize};

/// Runtime flag indicating whether the Execution_Engine should bind to
/// the simulated broker (R22.4).
///
/// Wire form is the snake_case lowercase token shared with the UI:
/// `"on"` or `"off"`. The default is [`ReplayMode::Off`] so a process
/// that does not opt in stays on the live broker stack.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    /// The Execution_Engine binds to its configured live broker. This is
    /// the default — production processes never enable replay mode.
    #[default]
    Off,
    /// The Execution_Engine binds to `SimulatedBroker` and routes every
    /// approval there. The replay player feeds recorded ticks into the
    /// Hot_Path; the simulated broker derives synthetic fills from the
    /// recorded orderbook (R22.2, R22.4).
    On,
}

impl ReplayMode {
    /// `true` when replay mode is on.
    #[inline]
    pub const fn is_replay(self) -> bool {
        matches!(self, Self::On)
    }

    /// Stable lower-case token used in UI events and metric labels.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
        }
    }
}

impl std::fmt::Display for ReplayMode {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_off() {
        assert_eq!(ReplayMode::default(), ReplayMode::Off);
        assert!(!ReplayMode::default().is_replay());
    }

    #[test]
    fn on_is_replay() {
        assert!(ReplayMode::On.is_replay());
        assert!(!ReplayMode::Off.is_replay());
    }

    #[test]
    fn as_str_round_trip() {
        assert_eq!(ReplayMode::Off.as_str(), "off");
        assert_eq!(ReplayMode::On.as_str(), "on");
    }

    #[test]
    fn json_round_trips_as_snake_case() {
        let on = ReplayMode::On;
        let off = ReplayMode::Off;
        let on_json = serde_json::to_string(&on).unwrap();
        let off_json = serde_json::to_string(&off).unwrap();
        assert_eq!(on_json, "\"on\"");
        assert_eq!(off_json, "\"off\"");
        let on_back: ReplayMode = serde_json::from_str(&on_json).unwrap();
        let off_back: ReplayMode = serde_json::from_str(&off_json).unwrap();
        assert_eq!(on, on_back);
        assert_eq!(off, off_back);
    }

    #[test]
    fn display_uses_snake_case_token() {
        assert_eq!(format!("{}", ReplayMode::Off), "off");
        assert_eq!(format!("{}", ReplayMode::On), "on");
    }
}
