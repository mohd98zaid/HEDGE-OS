#!/usr/bin/env bash
#
# Layer 2 of the Hot_Path Purity CI gate.
# Fails the build if any Hot_Path crate's source files or Cargo.toml
# reference a forbidden symbol. Implements R30.7 and R30.8.
#
# See docs/hot-path-purity.md for the operator guide.

set -euo pipefail

HOT_PATH_DIRS=(
    crates/hedge-broker-angelone
    crates/hedge-broker-dhan
    crates/hedge-broker-shoonya
    crates/hedge-broker-simulated
    crates/hedge-broker-zerodha
    crates/hedge-bus
    crates/hedge-config
    crates/hedge-core
    crates/hedge-exec
    crates/hedge-features
    crates/hedge-market-data
    crates/hedge-obs
    crates/hedge-orderflow
    crates/hedge-position
    crates/hedge-replay
    crates/hedge-risk
    crates/hedge-schemas
    crates/hedge-session
    crates/hedge-signals
    crates/hedge-supervisor
    crates/hedge-warmcache
)

# --------------------------------------------------------------------------- #
# Forbidden Rust source patterns. POSIX extended regex.
# --------------------------------------------------------------------------- #
FORBIDDEN_SOURCE=(
    "reqwest::blocking::Client"   # R30.7
    "use[[:space:]]+pyo3"         # R30.8
    "pythonize"                   # R30.8
    "numpy::"                     # R30.8
    "pandas::"                    # R30.8
)

# --------------------------------------------------------------------------- #
# Forbidden Cargo.toml dependency declarations. Matched case-insensitively
# at the start of a TOML key or inside a feature list.
# --------------------------------------------------------------------------- #
FORBIDDEN_CARGO=(
    "^[[:space:]]*pyo3[[:space:]]*="
    "^[[:space:]]*pythonize[[:space:]]*="
    "^[[:space:]]*numpy[[:space:]]*="
    "^[[:space:]]*pandas[[:space:]]*="
    "^[[:space:]]*pine-script"
    "^[[:space:]]*tradingview"
    "^[[:space:]]*openai[[:space:]]*="
    "^[[:space:]]*anthropic[[:space:]]*="
    "^[[:space:]]*langchain"
    "^[[:space:]]*cohere"
    "^[[:space:]]*azure-openai"
    "^[[:space:]]*google-genai"
    "^[[:space:]]*replicate"
    "^[[:space:]]*groq"
    "^[[:space:]]*mistralai"
    # reqwest with the blocking feature enabled (R30.7)
    "reqwest[^#]*features[[:space:]]*=[[:space:]]*\\[[^]]*\"blocking\""
)

violation_count=0

for dir in "${HOT_PATH_DIRS[@]}"; do
    if [[ ! -d "$dir" ]]; then
        continue
    fi

    # --- .rs files ------------------------------------------------------- #
    while IFS= read -r -d '' rs_file; do
        for pattern in "${FORBIDDEN_SOURCE[@]}"; do
            if grep -E -n "$pattern" "$rs_file" >/dev/null 2>&1; then
                while IFS= read -r match; do
                    echo "FAIL [source] $rs_file: forbidden pattern '$pattern' at $match"
                    violation_count=$((violation_count + 1))
                done < <(grep -E -n "$pattern" "$rs_file" 2>/dev/null || true)
            fi
        done
    done < <(find "$dir" -type f -name "*.rs" -print0)

    # --- Cargo.toml ------------------------------------------------------ #
    cargo_toml="$dir/Cargo.toml"
    if [[ -f "$cargo_toml" ]]; then
        for pattern in "${FORBIDDEN_CARGO[@]}"; do
            if grep -E -i -n "$pattern" "$cargo_toml" >/dev/null 2>&1; then
                while IFS= read -r match; do
                    echo "FAIL [Cargo.toml] $cargo_toml: forbidden declaration '$pattern' at $match"
                    violation_count=$((violation_count + 1))
                done < <(grep -E -i -n "$pattern" "$cargo_toml" 2>/dev/null || true)
            fi
        done
    fi
done

if [[ "$violation_count" -gt 0 ]]; then
    echo
    echo "Hot_Path purity layer 2 FAILED with $violation_count violation(s)."
    echo "See docs/hot-path-purity.md for guidance."
    exit 1
fi

echo "Hot_Path purity layer 2 OK (${#HOT_PATH_DIRS[@]} crates, ${#FORBIDDEN_SOURCE[@]} source patterns, ${#FORBIDDEN_CARGO[@]} cargo patterns)."
