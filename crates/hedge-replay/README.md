# `hedge-replay` — Replay_Engine

Implements task **40.1** of the PROJECT HEDGE implementation plan
([tasks.md](../../.kiro/specs/project-hedge/tasks.md), §F.40.1).

The Replay_Engine is the system that backs **Property 12: Replay
Determinism, Recording Completeness, and Simulated-Broker Routing**.

## What this crate provides

| Item | Type | Purpose |
|------|------|---------|
| [`Recorder`] | struct | Append-only ledger writer. One `XADD` per record on `hedge.hot.replay_record`, plus a length-prefixed rkyv frame in `<segment_dir>/<session_id>/seg-NNNN.rkyv`. |
| [`Player`] | struct | Single-threaded scheduler that loads a session into memory and releases records in `sequence_no` order at `1x | 10x | max` speed, with a seeded ChaCha20 RNG for any stochastic component. |
| [`ReplayMode`] | enum | `On / Off` flag the Execution_Engine reads at startup. When `On`, the engine binds to `SimulatedBroker`. |
| `replay.command.*` subjects | NATS request-reply | `/replay` UI control plane: list, open, scrub, step, play, status. |

## Disk layout

```
<segment_dir>/
    <session_id>/
        seg-0001.rkyv
        seg-0002.rkyv
        ...
```

* One directory per `session_id`.
* Files are zero-padded four-digit segment indices so a sorted listing
  reads in chronological order.
* Each segment file is a flat sequence of length-prefixed rkyv archives
  produced by `record::framed::write_framed`.
* Segments roll on **session boundary** (a `record_raw` with a different
  `session_id` opens a new directory) **or** when the active segment's
  on-disk size + the next record's wire size would exceed
  `max_segment_bytes` (default 1 GiB).

## Determinism contract (Property 12)

1. Every recorded event has a strict-monotonic gap-free `sequence_no`
   per session. The recorder validates this internally and returns
   `ReplayError::SequenceInvariant` on a violation.
2. The player loads records in `sequence_no` order and re-validates
   monotonicity at open time.
3. Stochastic components consume from a single `rand_chacha::ChaCha20Rng`
   seeded with the configured `rng_seed`. Two players seeded
   identically produce byte-identical RNG streams.
4. The Execution_Engine routes every approval to `SimulatedBroker`
   while `ReplayMode::On` is set.

## Wiring contract — `ReplayMode` and the Execution_Engine

The Replay_Engine deliberately does **not** depend on
`hedge-broker-simulated` or `hedge-exec` at the crate level.

The wiring is config-driven:

1. The recorder/player set `ReplayMode::On` at startup when running
   against a recorded session, by writing the value to either
   `hedge-warmcache` or to the `replay.replay_mode` field of the loaded
   `HedgeConfig`.
2. The Execution_Engine reads the flag at startup and constructs its
   `BrokerAdapter` as a `SimulatedBroker` rather than a live broker.
3. The flag is stable for the lifetime of the process so the engine
   never has to re-check on the per-tick path.

Keeping the linkage at the config layer rather than at the type
layer means:

* the recorder/player never link to broker code (smaller hot-path
  image, no transitive cloud-LLM-SDK risk through broker REST shims);
* the Execution_Engine retains its single source of truth for the
  adapter choice (its own startup builder);
* the contract is testable in isolation:
  `ReplayMode::On.is_replay()` is a pure boolean check the engine
  asserts in its constructor.

## `/replay` UI control plane

Subjects (all JSON request/response):

| Subject | Verb | Request body | Response body |
|---------|------|--------------|---------------|
| `replay.command.list` | GET | `{}` | `{ "sessions": [u64, ...] }` |
| `replay.command.open` | POST | `{ "session_id": u64, ... }` | `{ "ok": true, "total": u64 }` |
| `replay.command.scrub` | POST | `{ "sequence_no": u64 }` | `{ "ok": true, "cursor": u64 }` |
| `replay.command.step` | POST | `{}` | `{ "record": ReplayRecordWire? }` |
| `replay.command.play` | POST | `{ "speed": "x1" \| "x10" \| "max" }` | `{ "ok": true }` |
| `replay.command.status` | GET | `{}` | `{ "session_id": u64?, "cursor": u64, "total": u64, "speed": str }` |

The control plane lives outside the `hedge.hot.*` Redis stream
namespace because it carries control commands, not data. Redis
Streams are append-only — they are not the right substrate for
request-reply.

## Hot_Path purity (R30)

* `#![forbid(unsafe_code)]` (R30, defensive).
* No `pyo3`, `numpy`, `pandas`, `python-` runtime; verified by
  `forbid::FORBIDDEN_DEPENDENCIES` and the CI gate at
  `scripts/check-forbidden-deps.sh`.
* No `reqwest::blocking`, no cloud LLM SDKs.
* A defensive in-crate `build.rs` aborts compilation if a prohibited
  Cargo feature flag is ever turned on.

## CLI

```bash
hedge-replay [--dir <path>] list                # list every recorded session
hedge-replay [--dir <path>] info <session_id>   # show segment count / record count
hedge-replay [--dir <path>] dump <session_id>   # print each record's seq/kind to stdout
```

The CLI is for operator triage when the UI is not available; the
canonical control plane is the NATS subjects above.
