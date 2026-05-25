//! Configuration loader.
//!
//! Pipeline:
//!
//! 1. Read the file from disk.
//! 2. Parse YAML into a `serde_json::Value` (one shape covers both YAML and
//!    JSON because YAML is a superset).
//! 3. Validate the value against the bundled JSON Schema (R32, design § Error
//!    Handling — Configuration: fail closed at startup).
//! 4. Re-parse the YAML directly into the typed [`HedgeConfig`].
//! 5. Run cross-field invariant checks (e.g. profit-target band ordering,
//!    psychology threshold ladder).
//!
//! On any failure the loader emits a structured `tracing::error!` event with
//! `event = "cfg.error"`. Callers should then exit non-zero — see
//! [`fail_closed`] for the canonical helper.

use std::fs;
use std::path::Path;
use std::process;

use serde_json::Value;

use crate::defaults;
use crate::error::ConfigError;
use crate::models::HedgeConfig;
use crate::validation;

/// Load and validate a `HedgeConfig` from disk.
///
/// Returns `Err(ConfigError::*)` on any of:
/// - I/O failure
/// - YAML parse failure
/// - JSON Schema violation (any unknown field, missing required field,
///   wrong type, or out-of-range value)
/// - Cross-field invariant violation (band/threshold ordering)
pub fn load_from_path(path: &Path) -> Result<HedgeConfig, ConfigError> {
    let raw = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    load_from_str(&raw)
}

/// Same as [`load_from_path`] but takes the YAML body directly. Useful in
/// tests and in places where the bytes have already been read (e.g. by a
/// supervisor that wants to log the source content).
pub fn load_from_str(raw: &str) -> Result<HedgeConfig, ConfigError> {
    // 1. Validate against the JSON Schema. YAML is a superset of JSON, so
    //    `serde_yaml::from_str::<serde_json::Value>` works for any valid
    //    JSON-compatible YAML.
    let json: Value = serde_yaml::from_str(raw)?;
    validation::validate_json(&json)?;

    // 2. Decode into the typed config. We re-parse the raw YAML rather than
    //    the JSON so that the typed adapters (e.g. `time_ist`) see the
    //    canonical YAML strings.
    let cfg: HedgeConfig = serde_yaml::from_str(raw)?;

    // 3. Cross-field invariants.
    check_invariants(&cfg)?;
    Ok(cfg)
}

/// Returns the typed defaults from [`crate::defaults`]. Never fails.
pub fn load_default() -> HedgeConfig {
    defaults::hedge_config()
}

/// If a path is supplied, load and validate it; otherwise fall back to
/// [`load_default`]. Errors propagate from the file-load case.
pub fn load_or_default(path: Option<&Path>) -> Result<HedgeConfig, ConfigError> {
    match path {
        Some(p) => load_from_path(p),
        None => Ok(load_default()),
    }
}

/// Cross-field invariant checks. Anything the JSON Schema cannot express in
/// terms of a single field range lives here.
fn check_invariants(cfg: &HedgeConfig) -> Result<(), ConfigError> {
    if cfg.capital.daily_profit_target_min_inr >= cfg.capital.daily_profit_target_max_inr {
        return Err(ConfigError::InvariantViolation(format!(
            "capital.daily_profit_target_min_inr ({}) must be < capital.daily_profit_target_max_inr ({})",
            cfg.capital.daily_profit_target_min_inr, cfg.capital.daily_profit_target_max_inr
        )));
    }

    if cfg.session.start_ist >= cfg.session.end_ist {
        return Err(ConfigError::InvariantViolation(format!(
            "session.start_ist ({}) must be < session.end_ist ({})",
            cfg.session.start_ist, cfg.session.end_ist
        )));
    }

    if cfg.war_mode.start_ist >= cfg.war_mode.end_ist {
        return Err(ConfigError::InvariantViolation(format!(
            "war_mode.start_ist ({}) must be < war_mode.end_ist ({})",
            cfg.war_mode.start_ist, cfg.war_mode.end_ist
        )));
    }

    if cfg.ai.governance.drift_warn >= cfg.ai.governance.drift_critical {
        return Err(ConfigError::InvariantViolation(format!(
            "ai.governance.drift_warn ({}) must be < drift_critical ({})",
            cfg.ai.governance.drift_warn, cfg.ai.governance.drift_critical
        )));
    }

    if cfg.brokers.primary == cfg.brokers.backup {
        return Err(ConfigError::InvariantViolation(format!(
            "brokers.primary and brokers.backup must differ; both are {:?}",
            cfg.brokers.primary
        )));
    }

    cfg.trader_psychology.thresholds.check_ordering()?;
    Ok(())
}

/// Print the error to stderr in `cfg.error` form and exit the process with
/// status code 2 (matching the design's "fail closed at startup" rule for
/// configuration errors).
///
/// `!` return type signals to the type system that callers do not need to
/// produce a value after calling.
pub fn fail_closed(err: ConfigError) -> ! {
    tracing::error!(event = "cfg.error", error = %err, "configuration failure; exiting");
    eprintln!("cfg.error: {err}");
    process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::PostTargetPolicy;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_tmp(yaml: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        f
    }

    #[test]
    fn load_default_returns_design_defaults() {
        let cfg = load_default();
        assert_eq!(cfg.capital.base_inr, 20_000);
        assert_eq!(cfg.capital.daily_profit_target_min_inr, 300);
        assert_eq!(cfg.capital.daily_profit_target_max_inr, 1_000);
        assert_eq!(cfg.capital.post_target_policy, PostTargetPolicy::ReduceSizeToZero);
        assert_eq!(cfg.session.start_ist, defaults::ist_0915());
        assert_eq!(cfg.session.end_ist, defaults::ist_1530());
        assert_eq!(cfg.war_mode.start_ist, defaults::ist_0915());
        assert_eq!(cfg.war_mode.end_ist, defaults::ist_0945());
        assert_eq!(cfg.brokers.primary, hedge_core::BrokerId::Zerodha);
        assert_eq!(cfg.brokers.backup, hedge_core::BrokerId::Dhan);
    }

    #[test]
    fn round_trip_default_yaml_equals_default() {
        let cfg = load_default();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let parsed = load_from_str(&yaml).expect("default round-trips");
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn load_full_example_yaml() {
        let yaml = include_str!("../examples/full_config.yaml");
        let cfg = load_from_str(yaml).expect("full example loads");
        // Sanity-check a representative field from each subsection.
        assert_eq!(cfg.capital.base_inr, 20_000);
        assert_eq!(cfg.risk.max_daily_loss_inr, 600);
        assert_eq!(cfg.session.start_ist, defaults::ist_0915());
        assert_eq!(cfg.war_mode.min_confidence, 0.6);
        assert_eq!(cfg.ui.high_vol_threshold, 0.05);
        assert_eq!(cfg.ai.rank_p95_budget_ms, 5);
        assert_eq!(cfg.trader_psychology.thresholds.warning, 0.6);
        assert_eq!(cfg.brokers.primary, hedge_core::BrokerId::Zerodha);
        assert_eq!(cfg.ollama.models.len(), 4);
        assert_eq!(cfg.observability.retention.metrics_days, 30);
    }

    #[test]
    fn rejects_unknown_field() {
        let mut yaml = serde_yaml::to_string(&load_default()).unwrap();
        yaml.push_str("\nrogue: 1\n");
        let err = load_from_str(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::SchemaViolation(_)), "got {err:?}");
    }

    #[test]
    fn rejects_missing_required_field() {
        // Drop the `capital` block.
        let yaml = "
risk:
  max_daily_loss_inr: 600
  max_position_per_symbol: 200
  max_position_portfolio: 500
  max_leverage_per_symbol: 5.0
  max_leverage_account: 5.0
  max_drawdown_inr: 1000
  max_trades_per_minute: 4
  max_trades_per_hour: 30
  max_trades_per_session: 60
  max_exposure_per_symbol_inr: 20000
  max_exposure_per_sector_inr: 30000
  slippage_threshold_bps: 25
  slippage_cooldown_ms: 60000
  volatility_block_threshold: 0.06
  broker_latency_block_ms: 250
  base_risk_per_trade_inr: 100
session:
  start_ist: \"09:15:00\"
  end_ist: \"15:30:00\"
";
        let err = load_from_str(yaml).unwrap_err();
        assert!(matches!(err, ConfigError::SchemaViolation(_)), "got {err:?}");
    }

    #[test]
    fn rejects_min_greater_than_max_profit_target() {
        let mut cfg = load_default();
        cfg.capital.daily_profit_target_min_inr = 1_500;
        cfg.capital.daily_profit_target_max_inr = 1_000;
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let err = load_from_str(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::InvariantViolation(_)), "got {err:?}");
    }

    #[test]
    fn rejects_disordered_psychology_thresholds() {
        let mut cfg = load_default();
        // critical >= suppression breaks ordering.
        cfg.trader_psychology.thresholds.critical = 0.5;
        cfg.trader_psychology.thresholds.suppression = 0.4;
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let err = load_from_str(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::InvariantViolation(_)), "got {err:?}");
    }

    #[test]
    fn rejects_session_start_after_end() {
        let mut cfg = load_default();
        cfg.session.start_ist = defaults::ist_1530();
        cfg.session.end_ist = defaults::ist_0915();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let err = load_from_str(&yaml).unwrap_err();
        assert!(matches!(err, ConfigError::InvariantViolation(_)), "got {err:?}");
    }

    #[test]
    fn load_from_path_reads_file() {
        let cfg = load_default();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        let f = write_tmp(&yaml);
        let loaded = load_from_path(f.path()).unwrap();
        assert_eq!(loaded, cfg);
    }

    #[test]
    fn load_or_default_with_none() {
        let cfg = load_or_default(None).unwrap();
        assert_eq!(cfg, load_default());
    }

    #[test]
    fn psychology_thresholds_validated_constructor() {
        use crate::models::PsychologyThresholds;
        // Valid ordering.
        PsychologyThresholds::validated(0.6, 0.5, 0.4, 0.3).unwrap();
        // Invalid ordering returns an error.
        let err = PsychologyThresholds::validated(0.3, 0.4, 0.5, 0.6).unwrap_err();
        assert!(matches!(err, ConfigError::InvariantViolation(_)));
    }
}
