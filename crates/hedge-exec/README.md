# `hedge-exec` — Execution_Engine

The Execution_Engine is the only component in PROJECT HEDGE that submits
orders to a broker. It enforces three structural invariants:

1. **Approval-token gating (R6.8, R21.1).** Every submission requires a
   valid HMAC-SHA256 `ApprovalToken` minted by the Risk_Engine over the
   canonical `OrderIntent_v1` bytes. `submit(&ApprovalToken,
   &OrderIntent)` is the only public entry point that produces a
   broker-side order — submission without a valid approval is
   unrepresentable.

2. **Lifecycle FSM (R6.3, Property 9).** Every order traverses
   `New → Submitted → {Partially_Filled → Filled, Filled, Cancelled,
   Rejected}` and exactly one `exec.order.<state>` event is published
   per transition.

3. **Atomic broker failover (R6.5).** `BrokerRouter` holds an
   active+backup adapter pair. A sliding-window error-rate or latency
   breach atomically swaps the active slot via a single CAS and emits
   `exec.broker.failover`.

## Module layout

| File             | Responsibility                                                   |
| ---------------- | ---------------------------------------------------------------- |
| `lib.rs`         | Public API surface, re-exports                                   |
| `error.rs`       | `ExecError` enum, retryability and failover classification       |
| `lifecycle.rs`   | `OrderLifecycleTracker` FSM + legal-transition table             |
| `retry.rs`       | Bounded exponential-backoff retry, jitter sources, sleeper trait |
| `router.rs`      | `BrokerRouter`, `AdapterStats`, atomic failover                  |
| `engine.rs`      | `ExecutionEngine` orchestrator + `EngineEvent` events            |
| `main.rs`        | Service entrypoint                                               |

The `BrokerAdapter` trait and its companion types (`OrderIntent`,
`BrokerError`, `SubmitAck`, `OrderStatus`, `BrokerMetric`,
`ReadyState`, `OrderModification`) live in the workspace-shared
`hedge-broker-api` crate. `hedge-exec` re-exports them so callers
don't need a direct dependency on `hedge-broker-api`.

## Wire integration

| Inbound subject / stream     | Action                                                             |
| ---------------------------- | ------------------------------------------------------------------ |
| `hedge.hot.approvals` (Redis Stream, group `execution_engine`) | call `submit` with the carried token+intent |
| broker fill stream (websocket / push API)                       | call `on_fill` with cumulative qty/avg     |
| `trader.intent.cancel`      | call `cancel`                                                      |

| Outbound subject / stream    | Trigger                                                  |
| ---------------------------- | -------------------------------------------------------- |
| `exec.order.<state>` (NATS)  | every FSM transition                                     |
| `exec.broker.failover` (NATS)| router atomic swap                                       |
| `obs.error.exec.<tag>` (NATS)| every `EngineEvent::Error`                               |
| `hedge.hot.fills` (Redis)    | every `EngineEvent::Fill`                                |

## Hot_Path discipline (R30)

`.github/workflows/hot-path-purity.yml` enforces in CI:

- No `pyo3`, `numpy`, `pandas`, or any Python runtime dependency.
- No `reqwest::blocking`. Adapters are async-only.
- No `tokio::time::interval` polling loops on steady-state paths.
- No cloud LLM SDK — broker decisions are deterministic and local.

## Replay mode (R22.4)

When the engine is constructed with `ReplayMode::On` the binary binds
both router slots to a `SimulatedBroker`. Live brokers are never
contacted; the replay regression suite uses this to exercise the
engine deterministically against recorded orderbooks.
