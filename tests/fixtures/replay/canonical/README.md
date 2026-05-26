# Canonical replay fixture

This directory holds the **canonical recorded session** the nightly
replay-regression harness diffs against (task **59.1** of
[`project-hedge`](../../../.kiro/specs/project-hedge/tasks.md)).

## Layout

```
tests/fixtures/replay/canonical/
└── <session_id>/
    └── seg-NNNN.rkyv     # length-prefixed rkyv archives
```

The default canonical session id is **`20251130`** with **64
records** spanning every `RecordKind` the design's Testing Strategy
requires the regression to diff:

| RecordKind                     | Count |
|--------------------------------|-------|
| `Tick`                         | 14    |
| `SignalEmitted` *(Signal_v1)*  | 7     |
| `RiskDecision`                 | 7     |
| `OrderSubmitted` *(OrderIntent_v1)* | 6 |
| `OrderModified` *(OrderState_v1)*   | 6 |
| `OrderCancelled` *(OrderState_v1)*  | 6 |
| `Fill` *(OrderState_v1)*       | 6     |
| `AIDecision/Ranking`           | 6     |
| `MarketConditionSnapshot`      | 6     |

The four streams the spec calls out (`Signal_v1`, `RiskDecision`,
`OrderIntent_v1`, `OrderState_v1`) are all present and exercised by
the regression diff at byte level.

## Determinism contract

The fixture is produced by [`gen-canonical-replay`](../../../crates/hedge-replay/src/bin/gen_canonical.rs),
which writes records via [`Recorder::record_raw`] using a fixed
monotonic anchor (`5_000_000` ns) and a fixed wall-clock anchor
(2025-11-30 09:15:00.000 UTC, `1_764_493_200_000_000_000` ns since
the Unix epoch). The payload for each record is derived deterministically
from the sequence number and `RecordKind` discriminant, so two
invocations of the generator with the same arguments produce
**byte-identical** segment files.

## Regenerating the fixture

If you intentionally change the canonical session shape — for
example, after introducing a new `RecordKind` variant — regenerate
with:

```bash
# From the repo root.
cargo run --release --bin gen-canonical-replay -- \
    --out tests/fixtures/replay/canonical \
    --session-id 20251130 \
    --records 64
```

Then commit the resulting `seg-NNNN.rkyv` files. The nightly
regression workflow
([`.github/workflows/nightly.yml`](../../../.github/workflows/nightly.yml))
falls back to running `gen-canonical-replay` automatically when the
checked-in fixture is missing, so a deletion of this directory is
recoverable. The regeneration is the only supported "edit" workflow:
**do not hand-edit the rkyv segment files**.

## Verifying locally

```bash
cargo run --release --bin replay-regression -- \
    --dir tests/fixtures/replay/canonical \
    --session-id 20251130 \
    --seed 0xDEADBEEFCAFEBABE
```

A passing run prints `OK: <N> records identical across both runs`. A
failing run prints `FAIL: <reason>` and exits non-zero — that is the
signal the nightly job uses to fail closed (R22.2, Property 12).
