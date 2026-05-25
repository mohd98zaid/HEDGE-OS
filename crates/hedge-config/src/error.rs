//! Typed configuration errors.
//!
//! Configuration is the only error class in PROJECT HEDGE that is
//! **fail-closed at startup** (design § Error Handling — Configuration).
//! Every variant here maps to either a startup abort (non-zero exit) or
//! a run-time refusal in the loader.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

/// All ways `hedge-config` can refuse to produce a valid `HedgeConfig`.
///
/// On any of these, the caller is expected to:
///
/// 1. Emit a `cfg.error` structured log via `tracing::error!`.
/// 2. Exit non-zero (helper: [`crate::loader::fail_closed`]).
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The config file could not be opened or read.
    #[error("cannot read config file {path:?}: {source}")]
    Io {
        /// File path that failed to read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// The file's bytes did not parse as YAML.
    #[error("config YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// The parsed YAML did not satisfy the JSON Schema (missing required
    /// fields, unknown fields, wrong types, out-of-range values).
    #[error("config schema violation: {0}")]
    SchemaViolation(String),

    /// The parsed JSON was structurally valid for the schema but failed a
    /// cross-field invariant such as `min < max` or threshold ordering.
    #[error("config invariant violated: {0}")]
    InvariantViolation(String),

    /// The bundled JSON Schema itself failed to compile. Indicates a bug in
    /// `schema.json`; surfaces in tests, never in production.
    #[error("internal: schema compile error: {0}")]
    SchemaCompile(String),

    /// `PinnedConfig::install` was called twice in the same process.
    #[error("hot-path pinned config already installed")]
    AlreadyInstalled,
}
