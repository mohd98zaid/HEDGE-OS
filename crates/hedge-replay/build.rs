//! Build script for `hedge-replay`.
//!
//! Implements the **defensive in-crate** half of the `forbid_modules`
//! contract from task 44.1 (Requirements R30.6, R30.7, R30.8). The
//! transitive-closure check across every Hot_Path-adjacent crate ships
//! in task 8.1 as a CI workflow (`scripts/check-forbidden-deps.sh`);
//! this script catches the local case where a developer enables a
//! Cargo feature flag whose name matches a prohibited dependency.
//!
//! Pattern mirrors `crates/hedge-warmcache/build.rs` and
//! `crates/hedge-bus/build.rs` exactly so the three crates fail closed
//! in the same way. The Replay_Engine binds to the `SimulatedBroker`
//! when `ReplayMode::On` (R22.4) and is therefore subject to the same
//! Hot_Path-purity rules as any other broker-side component: no
//! `pyo3`, `numpy`, `pandas`, `reqwest::blocking`, no cloud LLM SDK.
//!
//! In a clean tree this script is a no-op — no prohibited Cargo
//! features are declared on the crate and none can ever be enabled.
//! Its value is purely defensive.

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
                        "hedge-replay: forbidden dependency feature `{}` enabled. \
                         The Hot_Path may not link `{}` (R30.6, R30.7, R30.8). \
                         See `crates/hedge-replay/src/forbid.rs`.",
                        normalised, bad
                    );
                }
            }
        }
    }
}
