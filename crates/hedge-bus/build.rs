//! Build script for `hedge-bus`.
//!
//! Implements the **defensive in-crate** half of the `forbid_modules`
//! contract from task 3.1 (Requirements R30.6, R30.7, R30.8). The
//! transitive-closure check across every Hot_Path crate ships in task 8.1
//! as a CI workflow; this script catches the local case where a developer
//! enables a Cargo feature flag whose name matches a prohibited dependency.
//!
//! Detection strategy:
//!
//! 1. Walk every `CARGO_FEATURE_*` env var Cargo exposes to the build
//!    script. These correspond one-for-one to the active features on
//!    `hedge-bus`.
//! 2. Compare each enabled feature name (lowercased, with `_` mapped to `-`)
//!    against the prohibited list. Match on substring so feature names like
//!    `with-pyo3-bridge` still trip the gate.
//! 3. Print `cargo:warning=` for any soft hits and abort with a compile-time
//!    `panic!` for hard hits.
//!
//! No prohibited Cargo features are currently declared on this crate, so in
//! a clean tree this script is a no-op. Its value is purely defensive.

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
    // Re-run only when the Cargo manifest, this script, or feature flags
    // change. We deliberately do not register every source file because the
    // check has nothing to do with source content.
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
                        "hedge-bus: forbidden dependency feature `{}` enabled. \
                         The Hot_Path may not link `{}` (R30.6, R30.7, R30.8). \
                         See `crates/hedge-bus/src/forbid.rs`.",
                        normalised, bad
                    );
                }
            }
        }
    }
}
