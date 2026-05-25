//! JSON Schema validation for `HedgeConfig`.
//!
//! The schema is bundled at compile time from `crates/hedge-config/schema.json`
//! so production binaries do not depend on a runtime file lookup.

use jsonschema::{Draft, JSONSchema};
use once_cell::sync::OnceCell;
use serde_json::Value;

use crate::error::ConfigError;

/// JSON Schema source bundled at compile time.
pub const SCHEMA_JSON: &str = include_str!("../schema.json");

/// Lazily-compiled validator. We compile once per process — schema compilation
/// is expensive enough to dominate config-load time on small configs.
static VALIDATOR: OnceCell<JSONSchema> = OnceCell::new();

/// Returns the compiled validator, lazily building it on first use.
pub fn validator() -> Result<&'static JSONSchema, ConfigError> {
    VALIDATOR.get_or_try_init(|| {
        let schema: Value = serde_json::from_str(SCHEMA_JSON)
            .map_err(|e| ConfigError::SchemaCompile(e.to_string()))?;
        JSONSchema::options()
            .with_draft(Draft::Draft202012)
            .compile(&schema)
            .map_err(|e| ConfigError::SchemaCompile(e.to_string()))
    })
}

/// Validate a parsed JSON value against the bundled schema. On any violation
/// returns a single `ConfigError::SchemaViolation` whose message lists every
/// failing path.
pub fn validate_json(instance: &Value) -> Result<(), ConfigError> {
    let v = validator()?;
    let result = v.validate(instance);
    match result {
        Ok(_) => Ok(()),
        Err(errors) => {
            let messages: Vec<String> = errors
                .map(|e| format!("{} at {}", e, e.instance_path))
                .collect();
            Err(ConfigError::SchemaViolation(messages.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_compiles() {
        validator().expect("bundled schema compiles");
    }

    #[test]
    fn defaults_round_trip_validate() {
        // `serde_yaml::to_value` of the default config must satisfy the schema.
        let yaml = serde_yaml::to_string(&crate::defaults::hedge_config()).unwrap();
        let json: Value = serde_yaml::from_str(&yaml).unwrap();
        validate_json(&json).expect("defaults satisfy schema");
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let mut json: Value =
            serde_yaml::from_str(&serde_yaml::to_string(&crate::defaults::hedge_config()).unwrap())
                .unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("nonsense".to_string(), Value::String("x".into()));
        let err = validate_json(&json).unwrap_err();
        assert!(matches!(err, ConfigError::SchemaViolation(_)), "got {err:?}");
    }

    #[test]
    fn rejects_missing_required_field() {
        let yaml = serde_yaml::to_string(&crate::defaults::hedge_config()).unwrap();
        let mut json: Value = serde_yaml::from_str(&yaml).unwrap();
        json.as_object_mut().unwrap().remove("capital");
        let err = validate_json(&json).unwrap_err();
        assert!(matches!(err, ConfigError::SchemaViolation(_)), "got {err:?}");
    }

    #[test]
    fn rejects_bad_time_format() {
        let yaml = serde_yaml::to_string(&crate::defaults::hedge_config()).unwrap();
        let mut json: Value = serde_yaml::from_str(&yaml).unwrap();
        json["session"]["start_ist"] = Value::String("9:15".into());
        let err = validate_json(&json).unwrap_err();
        assert!(matches!(err, ConfigError::SchemaViolation(_)), "got {err:?}");
    }
}
