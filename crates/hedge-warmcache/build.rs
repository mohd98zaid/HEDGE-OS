//! Build script for `hedge-warmcache`.
//!
//! Implements the **defensive in-crate** half of the `forbid_modules`
//! contract from task 44.1 (Requirements R30.6, R30.7, R30.8). The
//! transitive-closure check across every Hot_Path crate ships in task 8.1
//! as a CI workflow (`scripts/check-forbidden-deps.sh`); this script
//! catches the local case where a developer enables a Cargo feature flag
//! whose name matches a prohibited dependency.
//!
//! The pattern matches `crates/hedge-bus/build.rs` exactly so the two
//! crates fail closed in the same way. Adding this script is the only
//! `forbid_modules`-style guard the WarmCache crate carries beyond the
//! source-level [`forbid::FORBIDDEN_DEPENDENCIES`](crate::forbid).
//!
//! In a clean tree this script is a no-op — no prohibited Cargo features
//! are declared on the crate and none can ever be enabled. Its value is
//! purely defensive.

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
                        "hedge-warmcache: forbidden dependency feature `{}` enabled. \
                         The Hot_Path may not link `{}` (R30.6, R30.7, R30.8). \
                         See `crates/hedge-warmcache/src/forbid.rs`.",
                        normalised, bad
                    );
                }
            }
        }
    }
}
