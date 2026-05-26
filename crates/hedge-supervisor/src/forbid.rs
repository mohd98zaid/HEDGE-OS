//! Architectural-prohibition manifest for `hedge-supervisor`.
//!
//! Mirrors `crates/hedge-bus/src/forbid.rs` and
//! `crates/hedge-warmcache/src/forbid.rs`. The list is the source of
//! truth for the CI gate from task 8.1 — `scripts/check-forbidden-deps.sh`
//! and `scripts/check-forbidden-source.sh` walk every Hot_Path-adjacent
//! crate's transitive dependency graph and source tree against the same
//! set of prohibitions (R30.1–R30.8).
//!
//! The supervisor crate is subject to these rules even though it runs in
//! a separate process: it is the component the Hot_Path relies on for
//! self-healing, so the supervisor must itself be small, deterministic,
//! and free of heavyweight runtime dependencies.

/// Crate names whose transitive presence on `hedge-supervisor` is forbidden.
///
/// Categories (verbatim mirror of `hedge-bus` and `hedge-warmcache`):
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
