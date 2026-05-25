#!/usr/bin/env bash
#
# Layer 3 of the Hot_Path Purity CI gate.
# Fails the build if Hot_Path steady-state code uses Tokio polling
# primitives. Implements R30.3 ("all flow is push/event-driven in steady
# state").
#
# Three escape hatches:
#   1. The supervisor and broker-adapter crates are excluded entirely
#      because retry timers and backoff loops are legitimate (R25, R25.3).
#   2. Test code is skipped: files under any `tests/` directory and any
#      `#[cfg(test)] mod <name> { ... }` block are stripped before greppping.
#   3. Per-line `// hedge-allow: polling-loop` opt-out marker.
#
# See docs/hot-path-purity.md for the operator guide.

set -euo pipefail

# Crates that are subject to the no-polling rule. Note: hedge-supervisor and
# every hedge-broker-* crate are NOT in this list; they are explicitly
# allowed to run retry / backoff timers.
HOT_PATH_DIRS=(
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
    crates/hedge-warmcache
)

# Tokio polling primitives banned in Hot_Path steady state.
POLLING_PATTERNS=(
    "tokio::time::interval"
    "tokio::time::sleep"
)

ALLOW_MARKER="hedge-allow: polling-loop"

violation_count=0

# Strip `#[cfg(test)] mod <name> { ... }` blocks using awk brace tracking.
strip_test_modules() {
    local file="$1"
    awk '
        BEGIN { depth = 0; in_test = 0; }
        {
            if (in_test == 0 && match($0, /#\[cfg\(test\)\][[:space:]]*$/)) {
                # Look ahead — we expect the next non-blank line to start a mod block.
                next_line = ""
                # Simple heuristic: if this line ends in #[cfg(test)] flag the next mod.
                pending_test = 1
                print ""
                next
            }
            if (in_test == 0 && match($0, /#\[cfg\(test\)\]/) && match($0, /mod[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\{/)) {
                in_test = 1
                depth = 1
                # Count any extra braces on this same line.
                rest = $0
                n_open  = gsub(/\{/, "{", rest); n_open  = split($0, _, "{") - 1
                n_close = split($0, _, "}") - 1
                depth += (n_open - 1) - n_close
                if (depth <= 0) { in_test = 0; depth = 0 }
                print ""
                next
            }
            if (in_test == 0 && pending_test == 1 && match($0, /^[[:space:]]*mod[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\{/)) {
                in_test = 1
                pending_test = 0
                depth = 1
                rest = $0
                n_open  = split($0, _, "{") - 1
                n_close = split($0, _, "}") - 1
                depth += (n_open - 1) - n_close
                if (depth <= 0) { in_test = 0; depth = 0 }
                print ""
                next
            }
            if (in_test == 1) {
                n_open  = split($0, _, "{") - 1
                n_close = split($0, _, "}") - 1
                depth  += n_open - n_close
                if (depth <= 0) { in_test = 0; depth = 0 }
                print ""
                next
            }
            print $0
        }
    ' "$file"
}

for dir in "${HOT_PATH_DIRS[@]}"; do
    if [[ ! -d "$dir" ]]; then
        continue
    fi

    while IFS= read -r -d '' rs_file; do
        # Skip files inside any tests/ directory.
        case "$rs_file" in
            */tests/*) continue ;;
        esac

        cleaned=$(strip_test_modules "$rs_file")

        for pattern in "${POLLING_PATTERNS[@]}"; do
            line_no=0
            while IFS= read -r line; do
                line_no=$((line_no + 1))
                if echo "$line" | grep -F "$pattern" >/dev/null 2>&1; then
                    if echo "$line" | grep -F "$ALLOW_MARKER" >/dev/null 2>&1; then
                        # Per-line opt-out.
                        continue
                    fi
                    echo "FAIL [polling] $rs_file:$line_no: forbidden polling primitive '$pattern' — $line"
                    violation_count=$((violation_count + 1))
                fi
            done <<< "$cleaned"
        done
    done < <(find "$dir" -type f -name "*.rs" -print0)
done

if [[ "$violation_count" -gt 0 ]]; then
    echo
    echo "Hot_Path purity layer 3 FAILED with $violation_count violation(s)."
    echo "See docs/hot-path-purity.md for guidance."
    exit 1
fi

echo "Hot_Path purity layer 3 OK (${#HOT_PATH_DIRS[@]} crates, ${#POLLING_PATTERNS[@]} polling primitives banned)."
