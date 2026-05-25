# Hot_Path Purity — CI Gate

This page describes the three CI checks that enforce the Non-Goals listed in
`.kiro/specs/project-hedge/design.md` (R30) on every Hot_Path crate.

The purity gate has three independent layers. Each runs as its own GitHub
Actions job (`.github/workflows/hot-path-purity.yml`) so a partial failure
stays visible.

| Layer | Job name        | Script                             | What it catches                                                |
| ----- | --------------- | ---------------------------------- | -------------------------------------------------------------- |
| 1     | `forbidden-deps` | `scripts/check-forbidden-deps.sh`   | Forbidden *transitive* dependencies (R30.1, R30.2, R30.4–R30.8) |
| 2     | `forbidden-grep` | `scripts/check-forbidden-source.sh` | Forbidden *source-level* imports / Cargo features (R30.7, R30.8) |
| 3     | `polling-grep`   | `scripts/check-no-polling.sh`       | Steady-state polling loops (R30.3)                              |

The gate runs on every `pull_request` and every `push` to any branch.

---

## Hot_Path crate set

The following crates are considered **Hot_Path** and are subject to all three
checks (note that `hedge-ui-gateway` is intentionally *excluded*: it is the
only non-Hot_Path Rust crate, sitting between NATS and the React UI):

```
hedge-core              hedge-bus              hedge-schemas
hedge-obs               hedge-config           hedge-market-data
hedge-orderflow         hedge-features         hedge-signals
hedge-risk              hedge-exec             hedge-position
hedge-broker-zerodha    hedge-broker-dhan      hedge-broker-shoonya
hedge-broker-angelone   hedge-broker-simulated hedge-warmcache
hedge-replay            hedge-supervisor       hedge-session
```

The polling check (layer 3) further excludes `hedge-supervisor` and every
`hedge-broker-*` from the ban, because both are explicitly allowed to run
retry / backoff timers (R25, R25.3).

---

## Layer 1 — Forbidden transitive dependencies

`scripts/check-forbidden-deps.sh` runs **two** independent passes against
every Hot_Path crate and fails on any match:

1. `cargo tree --package <crate> --prefix none --no-dedupe --locked` and
   greps the human-readable output for the package name patterns below.
2. `cargo metadata --format-version 1 --locked` parsed with `jq`. We walk
   `resolve.nodes` from the crate's resolve node and collect every reachable
   dependency `name`, then word-grep that list. This is the authoritative
   pass — `cargo tree` formatting can change between toolchains, but the
   resolver graph cannot.

### Forbidden package-name patterns

| Pattern         | Reason                            | Requirement |
| --------------- | --------------------------------- | ----------- |
| `pyo3`          | Python embedding                  | R30.8, R3.6 |
| `pythonize`     | Python object marshalling         | R30.8       |
| `numpy`         | NumPy FFI                         | R30.8       |
| `pandas`        | pandas FFI                        | R30.8       |
| `^python`       | Any crate prefixed `python*`      | R30.8       |
| `pine-script`   | Pine Script execution             | R30.1       |
| `tradingview`   | TradingView SDK                   | R30.2       |
| `openai`        | OpenAI cloud LLM SDK              | R30.4, R30.6 |
| `anthropic`     | Anthropic cloud LLM SDK           | R30.4, R30.6 |
| `langchain`     | LangChain orchestration           | R30.4, R30.6 |
| `cohere`        | Cohere cloud LLM SDK              | R30.4, R30.6 |
| `azure-openai`  | Azure-hosted OpenAI               | R30.4, R30.6 |
| `google-genai`  | Google Gemini SDK                 | R30.4, R30.6 |
| `replicate`     | Replicate model-hosting SDK       | R30.4, R30.6 |
| `^groq`         | Groq cloud inference SDK          | R30.4, R30.6 |
| `mistralai`     | Mistral cloud LLM SDK             | R30.4, R30.6 |

Matching is case-insensitive and word-anchored, so `pyo3-build-config` is
caught by the `pyo3` pattern.

---

## Layer 2 — Forbidden source-level imports

`scripts/check-forbidden-source.sh` greps both the `.rs` files and the
`Cargo.toml` of every Hot_Path crate.

### Forbidden Rust source patterns

| Pattern                      | Reason                              | Requirement |
| ---------------------------- | ----------------------------------- | ----------- |
| `reqwest::blocking::Client`  | Blocking external HTTP on per-tick path | R30.7       |
| `use\s+pyo3`                 | Python interop                      | R30.8       |
| `pythonize`                  | Python object marshalling           | R30.8       |
| `numpy::`                    | NumPy module path                   | R30.8       |
| `pandas::`                   | pandas module path                  | R30.8       |

### Forbidden `Cargo.toml` patterns

The script also grep-fails on dependency declarations for any of the names
above, plus `pine-script`, `tradingview`, the cloud LLM SDK list, and
`reqwest` declared with the `blocking` feature enabled.

---

## Layer 3 — No steady-state polling loops

`scripts/check-no-polling.sh` enforces R30.3 ("all flow is push/event-driven
in steady state") by greppping for the two canonical Tokio polling
primitives:

- `tokio::time::interval`
- `tokio::time::sleep`

These are *banned in steady-state Hot_Path code*. Three escape hatches
exist:

1. **Recovery code** — `hedge-supervisor` and every `hedge-broker-*` crate
   are excluded from this check entirely. Retry timers and backoff loops
   are legitimate uses (R25, R25.3).
2. **Test code** — files inside any `tests/` directory are skipped, and
   `#[cfg(test)] mod <name> { ... }` blocks are stripped before greppping.
   Brace-tracking is done with `awk`.
3. **Per-line opt-out** — append the comment marker
   `// hedge-allow: polling-loop` to the offending line. Use sparingly and
   document why in the surrounding comment.

```rust
// Acceptable, supervisor-only retry:
let mut backoff = tokio::time::interval(Duration::from_millis(50));   // hedge-allow: polling-loop
```

A violation in any other context fails the build.

---

## Adding a new forbidden pattern

1. Decide which layer it belongs to:
   - **Whole-crate dependency name** → add to `FORBIDDEN_PATTERNS` in
     `scripts/check-forbidden-deps.sh`.
   - **Source token** → add to `FORBIDDEN_SOURCE` in
     `scripts/check-forbidden-source.sh`.
   - **`Cargo.toml` declaration** → add to `FORBIDDEN_CARGO` in the same
     script.
   - **Polling primitive** → add to `POLLING_PATTERNS` in
     `scripts/check-no-polling.sh`.

2. Update the relevant table in this document with the requirement number
   that motivates the ban.

3. Run the affected script locally (`bash scripts/check-*.sh`) to confirm it
   still passes against the current `main`.

---

## Why three independent gates

Defense in depth. Each layer covers a failure mode the others miss:

- A reviewer who pulls in a forbidden crate as a test-only dev-dependency
  bypasses layer 1 (cargo tree only walks the regular dep graph by default
  in stable formatting), but layer 2 catches it the moment any Hot_Path
  source file imports a type from it.
- A reviewer who copy-pastes a `pyo3` example without adding the dep to
  `Cargo.toml` would not pass `cargo build`, but layer 2 reports it as a
  *purity* failure with a precise `R30.8` reference instead of a generic
  compile error.
- A reviewer who replaces a real event-driven Redis subscription with
  `tokio::time::interval` polling would pass both layer 1 and layer 2 (no
  new deps, no new imports) but is caught by layer 3.
