# `hedge_warm_ai.priority` — Symbol_Priority_Engine (R14, task 23.1)

This subpackage implements the Warm_AI_Pipeline component that assigns
each tracked symbol to exactly one priority tier (`P1 | P2 | P3 | P4`)
and edge-emits `ai.priority.changed.<sym>` whenever trader, regime, or
news inputs flip a symbol's tier.

## Modules

| Module           | Purpose                                                                                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `allocation.py`  | `PriorityAllocation` row + `PriorityAllocationTable` (R14.2). Read-only mapping `tier → (cpu_budget, ai_inference_budget, scan_hz, alert_hz)`.         |
| `policy.py`      | `PriorityPolicy` protocol + `DefaultPriorityPolicy` reference implementation. Trader intents win per Authority_Hierarchy (R21).                        |
| `engine.py`      | `SymbolPriorityEngine` — totality invariant (R14.1) + edge-triggered emission (R14.3, Property 8).                                                     |
| `publisher.py`   | `PriorityChangedPublisher` protocol + in-memory / NATS / no-op implementations for `ai.priority.changed.<sym>`.                                        |
| `cache.py`       | `PriorityWarmCache` — Redis-backed bridge that exposes the current `tier` and `allocation` to Hot_Path consumers until the WarmCache crate (44.x) lands. |

## Totality (R14.1)

The engine stores tiers in `_tiers: dict[str, PriorityTier]`. A symbol
is in `_tiers` **iff** it is tracked. Three rules keep this invariant:

1. `track(symbol, initial_tier=..., baseline=...)` is the **only** way
   to introduce a symbol; it always sets a tier.
2. `untrack(symbol)` is the **only** way to remove a symbol; it
   removes both `_tiers[symbol]` and `_states[symbol]` in lock-step.
3. `_recompute_and_emit` either keeps the existing entry or overwrites
   it; it never deletes `_tiers[symbol]`.

The companion property test (task 23.2) fuzzes this: every tracked
symbol carries exactly one tier at all times.

## Edge-triggered emission (R14.3, Property 8)

`_recompute_and_emit` recomputes the tier on every input edge —
`trader.intent.priority`, `ai.regime.changed`, `ai.news.impact.<sym>`
— and compares the new tier against the prior. A `PriorityChanged`
event is emitted **only** when the two differ; the count of emitted
events therefore equals the count of distinct adjacent-pair changes
in the per-symbol observation stream. The payload always carries
`from` and `to`.

## WarmCache transition (R14.4, task 44.x)

Hot_Path Rust components (Signal_Engine, Risk_Engine, Execution_Engine)
must be able to read the current tier and `PriorityAllocation` for any
symbol without blocking on the Warm_AI_Pipeline. Until the dedicated
`hedge-warmcache` crate ships (task 44.x), the engine writes through
to Redis under the namespace `hedge:warm:priority`:

```
hedge:warm:priority:tier:<symbol>         -> "P1" | "P2" | "P3" | "P4"
hedge:warm:priority:allocation:<symbol>   -> {"cpu_budget": .., "ai_inference_budget": .., "scan_hz": .., "alert_hz": ..}
```

Both keys are written together inside a Redis `MULTI/EXEC` pipeline so
a Hot_Path reader cannot observe a mismatched `(tier, allocation)`
pair. Connection params are resolved through
`hedge_memory_rag.redis_cache.config.load_redis_cache_config` so the
cache reads the same `HEDGE_REDIS_URL` as every other Hedge service —
no hardcoded URLs, no new env vars.

When `hedge-warmcache` ships:

* The engine will be retargeted to the new crate's
  `priority(symbol)` slot (the contract `(tier, allocation)` pair stays
  the same).
* The Redis namespace `hedge:warm:priority` is reserved for the
  WarmCache crate to take over verbatim, so Hot_Path readers do not
  need a key migration.
* `PriorityWarmCache` will become a thin shim around the crate's
  client; the public `put` / `get_tier` / `get_allocation` API is
  intentionally narrow for that reason.

## NATS subjects

| Direction | Subject                              | Schema                                |
| --------- | ------------------------------------ | ------------------------------------- |
| `out`     | `ai.priority.changed.<symbol>`       | `ai_priority_changed.schema.json`     |
| `in`      | `trader.intent.priority`             | `trader_intent_priority.schema.json`  |
| `in`      | `ai.regime.changed`                  | `ai_regime_changed.schema.json`       |
| `in`      | `ai.news.impact.<symbol>`            | `ai_news_impact.schema.json`          |

The publisher takes an `async def publish(subject, payload)` callable
(matching `NatsDegradedPublisher` and `NatsAiLatencyEmitter`) so the
service binary owns the NATS connection lifecycle. The engine itself
has no transport dependency — tests use
`InMemoryPriorityChangedPublisher` to assert on emitted events without
spinning up NATS.

## References

- Requirements §14 — Symbol Priority Allocation (R14.1–R14.4).
- Requirements §21 — Authority_Hierarchy (R21).
- Design § Components § Symbol_Priority_Engine.
- Design § Correctness Properties § Property 8.
- Task 23.1 (this task), 23.2 (property test), 44.x (WarmCache crate).
