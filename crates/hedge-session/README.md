# hedge-session

Session manager and **Market_Open_War_Mode** controller for PROJECT
HEDGE.

This crate ships two cooperating IST-clock observers that share one
wall clock and one NATS connection:

| Controller          | Window (default IST)   | Subjects                                          | Task   |
|---------------------|------------------------|---------------------------------------------------|--------|
| `WarModeController` | `09:15:00 – 09:45:00`  | `ops.warmode.start`, `ops.warmode.end`            | 42.1   |
| `SessionController` | `09:15:00 – 15:30:00`  | `ops.session.start`, `ops.session.end`            | 43.1   |

Both are edge-triggered and idempotent (Property 8): one event per
genuine boundary crossing, no duplicates on clock-skew double-fires.

## What this crate emits

| Subject              | Schema                                                  | Trigger                                                       |
|----------------------|---------------------------------------------------------|---------------------------------------------------------------|
| `ops.warmode.start`  | `hedge-schemas/json_schemas/ops_warmode.schema.json`    | IST clock crosses `war_mode.start_ist` (default `09:15:00`). |
| `ops.warmode.end`    | `hedge-schemas/json_schemas/ops_warmode.schema.json`    | IST clock crosses `war_mode.end_ist` (default `09:45:00`).   |
| `ops.session.start`  | `hedge-schemas/json_schemas/ops_session.schema.json`    | IST clock crosses `session.start_ist` (default `09:15:00`).  |
| `ops.session.end`    | `hedge-schemas/json_schemas/ops_session.schema.json`    | IST clock crosses `session.end_ist` (default `15:30:00`).    |

War_Mode events carry the full `WarMode` profile so consumers that
join the bus mid-window can adopt the correct profile from the next
announcement without a round-trip to the config service:

```jsonc
{
  "session_id":      20251130,    // YYYYMMDD packed into u64
  "phase":           "start",     // or "end"
  "min_confidence":  0.6,         // mirrors WarModeConfig.min_confidence
  "scan_multiplier": 2.0,         // mirrors WarModeConfig.scan_multiplier
  "ts_ns":           1234567890   // hedge_core::now_ns at emission
}
```

Trading_Session events carry the minimal payload — no profile fields,
just identity and timestamp:

```jsonc
{
  "session_id": 20251130,    // YYYYMMDD packed into u64
  "phase":      "start",     // or "end"
  "ts_ns":      1234567890   // hedge_core::now_ns at emission
}
```

## Edge-triggered, idempotent emission (Property 8)

Each controller is a two-state machine — `Inactive` ↔ `Active` —
that fires exactly **one** event per genuine boundary crossing.
Re-entering the same boolean state (clock skew, double-fire, repeated
reconcile at the same instant) is a no-op. This is the design's
edge-triggered emission contract (Property 8) implemented locally so
the matching `proptest` tasks (42.2, 43.2) can verify the property
end-to-end.

Mechanically, each run loop computes a single deadline per iteration
(today's start, today's end, or tomorrow's start) and awaits exactly
one `tokio::time::sleep_until` for it. There is **no steady-state
polled timer**; the `sleep_until` call site carries the workspace's
documented `hedge-allow: polling-loop` marker because the no-polling
CI rule applies to busy loops, not to one-shot deadlines on a
state-machine edge (R30.3).

## Subscriber wiring (informational)

This crate **does not** modify any subscriber crate. The components
below are the documented consumers of `ops.warmode.*` and
`ops.session.*` per design § Components, design § Operating Modes,
design § Configuration Surface and Defaults; their NATS subscriptions
live in their own source trees.

### `ops.warmode.*` consumers

| Subscriber crate    | Behaviour while War_Mode is active                                                                              | Spec ref         |
|---------------------|-----------------------------------------------------------------------------------------------------------------|------------------|
| `hedge-features`    | Applies the `scan_multiplier` to the per-symbol scan rate.                                                      | R26.2            |
| `hedge-orderflow`   | Boosts orderflow sampling sensitivity by `scan_multiplier` (uniform sensitivity factor).                        | R26.2            |
| `hedge-signals`     | Boosts breakout detection sensitivity by `scan_multiplier` and gates emitted signals on `min_confidence`.       | R26.2, R26.3     |
| `hedge-risk`        | Re-applies the `min_confidence` floor as a defence-in-depth gate (rejection reason: `WarModeConfidenceTooLow`). | R26.3            |
| `hedge-ui-gateway`  | Applies the reduced-clutter presentation profile and suppresses signals below `min_confidence`.                 | R26.3, R26.4     |

### `ops.session.*` consumers

| Subscriber crate                | Behaviour                                                                                                         | Spec ref           |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------|--------------------|
| `hedge-risk`                    | Corroborates the local IST `[start_ist, end_ist]` gate; on `end`, requests Execution_Engine cancel non-persistent open orders. | R31.1, R31.4       |
| `hedge-features`                | Resets cumulative-since-session-start state (e.g. cumulative VWAP) on `start`.                                    | R15.3              |
| Previous_Day_Memory_Engine      | On `end`, schedules the next-session compute job that must complete before the next `start`.                      | R15.3              |
| `hedge-ui-gateway`              | Toggles the session-active banner on the trader cockpit edge-triggered.                                           | R31.2, R31.3       |

### Risk_Engine session-time gate (R31.1)

The Risk_Engine's session-time gate is implemented locally inside
`hedge_risk::RiskEngine::evaluate` against `hedge_config::SessionConfig`
— it consults the IST wall clock directly and rejects every
`evaluate` call outside `[start_ist, end_ist]` with
`Rejected { reason: SessionClosed }`. That gate is **independent** of
this crate's emissions: an isolated Risk_Engine without a connected
`hedge-session` still rejects orders correctly. This crate's
responsibility is to **announce** the boundary on the bus so other
Hot_Path components can pivot edge-triggered.

When wiring the consumers, prefer the well-known constants from
`hedge-bus` over raw string literals so subject drift is impossible:

```rust
use hedge_bus::{
    OPS_SESSION_END, OPS_SESSION_START,
    OPS_WARMODE_END, OPS_WARMODE_START,
};
```

## Configuration

War_Mode and Trading_Session each have their own block under the
top-level `HedgeConfig`:

```yaml
session:
  start_ist: "09:15:00"
  end_ist:   "15:30:00"

war_mode:
  start_ist: "09:15:00"
  end_ist:   "09:45:00"
  min_confidence: 0.6
  scan_multiplier: 2.0
```

Defaults match design § Configuration Surface and Defaults verbatim
(see `hedge-config/src/defaults.rs::session` and `::war_mode`).

## Test hooks

`controller::WallClock` is shared across both controllers; production
uses `controller::SystemWallClock`, unit tests substitute deterministic
fakes. Each controller has its own publisher trait
(`controller::OpsEventPublisher`, `session_controller::SessionEventPublisher`)
so tests can vector-record events without a live NATS connection.

The `tick_until(from, to)` helper on each controller drives it across
an explicit UTC window without sleeping, which is what tasks 42.2 and
43.2's `proptest` will use to verify the emission-count property over
generated time-streams.

## Spec references

- **Requirements**: 26.1, 26.2, 26.3, 26.4 (War_Mode); 31.1, 31.2,
  31.3, 31.4 (Trading_Session).
- **Design**: Operating Modes; Configuration Surface and Defaults;
  Components § Risk_Engine; Components § Signal_Engine.
- **Property**: 8 — Edge-Triggered Emission of State Changes.
