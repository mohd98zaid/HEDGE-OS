//! `hedge-config`
//!
//! Typed YAML configuration loader for PROJECT HEDGE (task **6.1**).
//!
//! Design references:
//! - _Data Models § Configuration Surface and Defaults_ (R32, R5, R20, R26)
//! - _Error Handling § Configuration_: fail closed at startup, emit
//!   `cfg.error`, exit non-zero on any schema or invariant violation.
//!
//! Public entry points:
//!
//! ```ignore
//! use hedge_config::{load_from_path, load_default, fail_closed};
//! use std::path::Path;
//!
//! // Hot_Path startup pattern.
//! let cfg = match load_from_path(Path::new("/etc/hedge/config.yaml")) {
//!     Ok(c) => c,
//!     Err(e) => fail_closed(e),  // emits cfg.error, exits 2
//! };
//! hedge_config::pinned::global().install(cfg).unwrap();
//! ```
//!
//! Hot_Path processes call [`pinned::global`] once at startup and read via
//! `get_or_panic` afterwards. Non-Hot_Path processes use
//! [`pinned::MutableConfig`] which supports SIGHUP-driven swap.

#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(missing_docs)]

pub mod defaults;
pub mod error;
pub mod loader;
pub mod models;
pub mod pinned;
pub mod validation;

// Public API re-exports.
pub use error::ConfigError;
pub use loader::{fail_closed, load_default, load_from_path, load_from_str, load_or_default};
pub use models::{
    AiConfig, BrokerConfig, CapitalConfig, DegradedModeConfig, GovernanceConfig, HedgeConfig,
    ObservabilityConfig, OllamaConfig, OllamaModelConfig, OllamaRole, PostTargetPolicy,
    PsychologyThresholds, RankingFactorsConfig, RetentionConfig, RiskConfig, SessionConfig,
    TraderPsychologyConfig, UiConfig, WarModeConfig, WarmCacheConfig,
};
pub use pinned::{MutableConfig, PinnedConfig};
pub use validation::{validate_json, validator, SCHEMA_JSON};

// Re-export the IST timezone constant so callers do not have to add a
// direct `chrono-tz` dependency to compute IST-anchored timestamps from
// the `NaiveTime` values stored in `SessionConfig` / `WarModeConfig`.
pub use chrono_tz::Asia::Kolkata as IST;
