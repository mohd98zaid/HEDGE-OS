//! Build script for `hedge-supervisor`.
//!
//! Implements the **defensive in-crate** half of the `forbid_modules`
//! contract from task 44.1 (Requirements R30.6, R30.7, R30.8). The full
//! transitive-closure check across every Hot_Path-adjacent crate ships
//! in task 8.1 as a CI workflow (`scripts/check-forbidden-deps.sh`); this
//! script catches the local case where a developer enables a Cargo
//! feature flag whose name matches a prohibited dependency on the
//! supervisor crate itself.
//!
//! The pattern mirrors `crates/hedge-warmcache/build.rs` exactly so the
//! two crates fail closed in the same way. Adding this script is the only
//! `forbid_modules`-style guard the supervisor crate carries beyond the
//! source-level [`forbid::FORBIDDEN_DEPENDENCIES`](crate::forbid).
//!
//! The supervisor is **separate from** the Hot_Path proper (design
//! § Self-Healing Flow): a Hot_Path crash must never kill the
//! supervisor, so the supervisor binary runs in its own process. The
//! prohibition list, however, remains identical: no Python runtime, no
//! cloud LLM SDKs, no blocking HTTP. The supervisor must be cheap and
//! reliable, which is precisely the discipline these prohibitions
//! enforce.

const FORBIDDEN: &[&str] = &[
    "pyo3",
    "numpy",
    "pandas",
    "blocking", // catches `reqwest::blocking` re-exports surfaced via features
    "openai",
    "anthropic",
    "cohere",
    "gemini",
    "vertexai",
    "vertex-ai",
    "tradingview",
    "pine-script",
    "pinescript",
];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");

    for (key, _value) in std::env::vars() {
        if let Some(feature) = key.strip_prefix("CARGO_FEATURE_") {
            // Cargo upper-cases and underscore-substitutes feature names; we
            // normalise back to the kebab-case form developers actually
            // type, so the substring match is meaningful.
            let normalised = feature.to_lowercase().replace('_', "-");
            for bad in FORBIDDEN {
                if normalised.contains(bad) {
                    panic!(
                        "hedge-supervisor: forbidden dependency feature `{}` enabled. \
                         The Hot_Path supervisor may not link `{}` (R30.6, R30.7, R30.8). \
                         See `crates/hedge-supervisor/src/forbid.rs`.",
                        normalised, bad
                    );
                }
            }
        }
    }
}
