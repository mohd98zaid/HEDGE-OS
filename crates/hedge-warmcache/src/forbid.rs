//! Architectural-prohibition manifest for `hedge-warmcache`.
//!
//! Mirrors `crates/hedge-bus/src/forbid.rs`. The list is the source of
//! truth for the CI gate from task 8.1 — `scripts/check-forbidden-deps.sh`
//! and `scripts/check-forbidden-source.sh` walk every Hot_Path crate's
//! transitive dependency graph and source tree against the same set of
//! prohibitions (R30.1–R30.8).
//!
//! The WarmCache crate is doubly subject to these rules:
//!
//! * It is on the Hot_Path read side (R9.4, R17.4, R19.7) — atomic loads
//!   only, no Python, no cloud LLMs, no blocking HTTP.
//! * It is also the boundary where data flows in **from** the Warm_AI_Pipeline
//!   over NATS. The crate must not pull in any Python or LLM library to
//!   parse those payloads; the schemas live in `hedge-schemas` and decode
//!   through `serde_json` only (for `ai.*` JSON subjects) or FlatBuffers
//!   accessors (for the few FB-defined ones).

/// Crate names whose transitive presence on `hedge-warmcache` is forbidden.
///
/// Categories (verbatim mirror of `hedge-bus`):
///
/// * **Python runtime** — `pyo3`, `numpy`, `pandas`, `cpython` (R30.8).
/// * **Blocking HTTP** — `reqwest-blocking` placeholder; the CI script
///   additionally inspects `reqwest`'s feature flags (R30.7).
/// * **Cloud LLM SDKs** — every SDK that talks to a hosted inference API
///   (R30.4, R30.6).
/// * **Pine Script / TradingView** — explicitly prohibited by R30.1, R30.2.
///
/// _Spec references_: R30.1, R30.2, R30.4, R30.6, R30.7, R30.8.
pub const FORBIDDEN_DEPENDENCIES: &[&str] = &[
    // Python runtime
    "pyo3",
    "pyo3-build-config",
    "pyo3-ffi",
    "pyo3-macros",
    "cpython",
    "numpy",
    "pandas",
    // Blocking HTTP (catches `reqwest` with the `blocking` feature flag).
    "reqwest-blocking",
    // Cloud LLM SDKs
    "openai",
    "openai-api-rs",
    "async-openai",
    "anthropic",
    "anthropic-rs",
    "anthropic-sdk",
    "cohere-rust",
    "google-cloud-aiplatform",
    "vertexai",
    "vertex-ai",
    "gemini",
    // TradingView / Pine Script (R30.1, R30.2)
    "tradingview",
    "pine-script",
    "pinescript",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forbidden_list_includes_pyo3_and_numpy_and_pandas() {
        for needle in ["pyo3", "numpy", "pandas"] {
            assert!(
                FORBIDDEN_DEPENDENCIES.contains(&needle),
                "missing {} in FORBIDDEN_DEPENDENCIES",
                needle
            );
        }
    }

    #[test]
    fn forbidden_list_includes_at_least_one_cloud_llm_sdk() {
        let any_llm = FORBIDDEN_DEPENDENCIES
            .iter()
            .any(|d| d.contains("openai") || d.contains("anthropic"));
        assert!(any_llm, "no cloud LLM SDK entries in FORBIDDEN_DEPENDENCIES");
    }

    #[test]
    fn forbidden_list_has_no_blank_or_duplicate_entries() {
        let mut seen = std::collections::HashSet::new();
        for entry in FORBIDDEN_DEPENDENCIES {
            assert!(!entry.is_empty(), "blank entry in FORBIDDEN_DEPENDENCIES");
            assert!(
                seen.insert(*entry),
                "duplicate entry `{}` in FORBIDDEN_DEPENDENCIES",
                entry
            );
        }
    }
}
