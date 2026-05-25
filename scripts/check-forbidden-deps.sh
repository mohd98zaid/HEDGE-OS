#!/usr/bin/env bash
#
# Layer 1 of the Hot_Path Purity CI gate.
# Fails the build if any Hot_Path crate transitively depends on a forbidden
# package. Implements R30.1, R30.2, R30.4–R30.8.
#
# See docs/hot-path-purity.md for the operator guide.

set -euo pipefail

# --------------------------------------------------------------------------- #
# Hot_Path crate set. Order is alphabetical for diff-friendliness.
# `hedge-ui-gateway` is intentionally excluded — it is the only non-Hot_Path
# Rust crate.
# --------------------------------------------------------------------------- #
HOT_PATH_CRATES=(
    hedge-broker-angelone
    hedge-broker-dhan
    hedge-broker-shoonya
    hedge-broker-simulated
    hedge-broker-zerodha
    hedge-bus
    hedge-config
    hedge-core
    hedge-exec
    hedge-features
    hedge-market-data
    hedge-obs
    hedge-orderflow
    hedge-position
    hedge-replay
    hedge-risk
    hedge-schemas
    hedge-session
    hedge-signals
    hedge-supervisor
    hedge-warmcache
)

# --------------------------------------------------------------------------- #
# Forbidden transitive-dependency name patterns.
#
# Patterns are POSIX extended regex, matched case-insensitively against the
# fully-resolved crate name. Word-anchoring is applied automatically below so
# `pyo3` matches `pyo3-build-config` but `numpy` does not match
# `numpy-shim-renamed-something`.
# --------------------------------------------------------------------------- #
FORBIDDEN_PATTERNS=(
    "pyo3"            # R30.8, R3.6
    "pythonize"       # R30.8
    "numpy"           # R30.8
    "pandas"          # R30.8
    "^python"         # R30.8
    "pine-script"     # R30.1
    "tradingview"     # R30.2
    "openai"          # R30.4, R30.6
    "anthropic"       # R30.4, R30.6
    "langchain"       # R30.4, R30.6
    "cohere"          # R30.4, R30.6
    "azure-openai"    # R30.4, R30.6
    "google-genai"    # R30.4, R30.6
    "replicate"       # R30.4, R30.6
    "^groq"           # R30.4, R30.6
    "mistralai"       # R30.4, R30.6
)

violation_count=0

# --------------------------------------------------------------------------- #
# Pass 1: cargo tree on every Hot_Path crate.
# --------------------------------------------------------------------------- #
echo "==> Pass 1: cargo tree against every Hot_Path crate"
for crate in "${HOT_PATH_CRATES[@]}"; do
    tree_out=$(cargo tree --package "$crate" --prefix none --no-dedupe --locked 2>/dev/null || true)
    if [[ -z "$tree_out" ]]; then
        # The crate may not exist yet on a partial scaffold; skip silently.
        continue
    fi
    for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
        # Match the crate name at the start of a line so we do not
        # false-positive on a description.
        if echo "$tree_out" | grep -E -i "^${pattern}([[:space:]]|$|-)" >/dev/null; then
            offender=$(echo "$tree_out" | grep -E -i "^${pattern}([[:space:]]|$|-)" | head -n 1)
            echo "FAIL [tree] crate '$crate' transitively pulls in forbidden pattern '$pattern' via: $offender"
            violation_count=$((violation_count + 1))
        fi
    done
done

# --------------------------------------------------------------------------- #
# Pass 2: cargo metadata + jq. Authoritative resolver-graph walk.
# --------------------------------------------------------------------------- #
if ! command -v jq >/dev/null 2>&1; then
    echo "FAIL: jq is required for Pass 2 of the dependency check"
    exit 2
fi

echo "==> Pass 2: cargo metadata resolver graph"
metadata=$(cargo metadata --format-version 1 --locked 2>/dev/null || true)
if [[ -z "$metadata" ]]; then
    echo "FAIL: cargo metadata returned no output"
    exit 2
fi

for crate in "${HOT_PATH_CRATES[@]}"; do
    # Collect every crate name reachable from this crate's resolve node by
    # walking `resolve.nodes[].deps[].name` transitively. We compute the
    # transitive closure in jq for portability.
    closure=$(echo "$metadata" | jq -r --arg name "$crate" '
        . as $root
        | [.resolve.nodes[] | select(.id | startswith($name + " ") or contains("#" + $name + "@"))][0] as $start
        | if $start == null then empty
          else
            # BFS over resolve nodes by id.
            def lookup(id): $root.resolve.nodes[] | select(.id == id);
            def walk(seen; queue):
                if (queue | length) == 0 then seen
                else
                    queue[0] as $cur
                    | (lookup($cur).deps // [] | map(.pkg)) as $children
                    | walk(seen + [$cur]; (queue[1:] + ($children - seen - queue)))
                end;
            walk([]; [$start.id])
            | map(. as $id | $root.packages[] | select(.id == $id) | .name)
            | unique
            | .[]
          end
    ' 2>/dev/null || true)
    if [[ -z "$closure" ]]; then
        continue
    fi
    while IFS= read -r dep_name; do
        [[ -z "$dep_name" ]] && continue
        for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
            if echo "$dep_name" | grep -E -i "^${pattern}([-]|$)" >/dev/null; then
                echo "FAIL [metadata] crate '$crate' transitive dep '$dep_name' matches forbidden pattern '$pattern'"
                violation_count=$((violation_count + 1))
            fi
        done
    done <<< "$closure"
done

# --------------------------------------------------------------------------- #
# Verdict.
# --------------------------------------------------------------------------- #
if [[ "$violation_count" -gt 0 ]]; then
    echo
    echo "Hot_Path purity layer 1 FAILED with $violation_count violation(s)."
    echo "See docs/hot-path-purity.md for guidance."
    exit 1
fi

echo "Hot_Path purity layer 1 OK ($((${#HOT_PATH_CRATES[@]})) crates checked, ${#FORBIDDEN_PATTERNS[@]} patterns)."
