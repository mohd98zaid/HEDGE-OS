# Hot_Path Purity Scripts — Local Testing

This file describes how to validate the three Hot_Path purity scripts
locally before pushing.

> **Note:** This file was reconstructed from the spec on 2026-05-25 after
> being accidentally deleted. The exact prior wording may differ — the
> intent and the underlying scripts are preserved verbatim.

## Layer 1: forbidden transitive dependencies

```bash
bash scripts/check-forbidden-deps.sh
```

Expected output on a clean tree:

```
==> Pass 1: cargo tree against every Hot_Path crate
==> Pass 2: cargo metadata resolver graph
Hot_Path purity layer 1 OK (21 crates checked, 16 patterns).
```

To smoke-test, temporarily add `pyo3 = "0.21"` to any Hot_Path crate's
`Cargo.toml`, run `cargo update -p <that crate>`, then re-run the script.
You should see one or more `FAIL [tree]` and `FAIL [metadata]` lines and a
non-zero exit.

## Layer 2: forbidden source-level imports

```bash
bash scripts/check-forbidden-source.sh
```

To smoke-test, add `use reqwest::blocking::Client;` to any `.rs` file
inside a Hot_Path crate and re-run the script. You should see one
`FAIL [source]` line and a non-zero exit.

## Layer 3: no steady-state polling

```bash
bash scripts/check-no-polling.sh
```

To smoke-test, add `let _ = tokio::time::sleep(...);` to any non-test
function inside a Hot_Path crate (excluding `hedge-supervisor` and the
broker adapters) and re-run the script. The line should be reported
unless the `// hedge-allow: polling-loop` marker is appended.

## Running all three at once

```bash
for s in scripts/check-forbidden-deps.sh \
         scripts/check-forbidden-source.sh \
         scripts/check-no-polling.sh; do
    bash "$s" || exit 1
done
```

Returns 0 when the tree is clean, non-zero otherwise.
