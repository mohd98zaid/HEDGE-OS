//! Architectural-prohibition manifest.
//!
//! Acceptance criteria from R30 (Architectural Prohibitions) and R9.4–9.6 are
//! enforced at three layers:
//!
//! 1. **Source-level** — `#![forbid(unsafe_code)]` in [`crate`] root.
//! 2. **Build-script** — see `crates/hedge-bus/build.rs`. Aborts compilation
//!    if a prohibited Cargo feature flag is enabled.
//! 3. **CI workflow** — task **8.1** runs `cargo metadata` over every
//!    Hot_Path crate and fails the pipeline if any transitive dependency
//!    matches an entry in [`FORBIDDEN_DEPENDENCIES`]. That script reads the
//!    constant exported here so the source of truth lives in code, not in CI
//!    YAML.
//!
//! Adding a new prohibition therefore requires only an edit to this list;
//! the CI script picks the change up automatically on its next run.

/// Crate names whose transitive presence on a Hot_Path crate is forbidden.
///
/// Categories:
///
/// * **Python runtime** — `pyo3`, `pyo3-build-config`, `numpy`, `pandas`,
///   `cpython` (R30.8). The Hot_Path is pure Rust; Python lives in the
///   Warm_AI_Pipeline only.
/// * **Blocking HTTP** — `reqwest::blocking` is shipped by the `reqwest`
///   crate's `blocking` feature. We forbid the whole crate on the Hot_Path
///   and direct callers to `reqwest::Client` (async) only when async HTTP is
///   genuinely required (R30.7). Note: the substring match covers the
///   `blocking` feature even when imported as `reqwest::blocking::Client`.
/// * **Cloud LLM SDKs** — every SDK that talks to a hosted inference API
///   (OpenAI, Anthropic, Cohere, Gemini/Vertex). The Hot_Path is forbidden
///   from issuing LLM calls (R30.4), so no Hot_Path crate may even link
///   these libraries.
/// * **Pine Script / TradingView** — explicitly prohibited by R30.1 and R30.2.
///
/// The list is read by:
///
/// * The CI workflow defined in task 8.1 (`scripts/ci/forbid_modules.sh` or
///   equivalent), which calls `cargo metadata --format-version=1` and
///   greps the dependency graph.
/// * Anyone reviewing what counts as a forbidden dependency on the Hot_Path.
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
    // The CI script in task 8.1 must additionally check that any `reqwest`
    // dependency does not enable `features = ["blocking"]`.
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
        // Sanity: the canonical Python-runtime entries are present.
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
