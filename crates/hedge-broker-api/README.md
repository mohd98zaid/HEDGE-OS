# `hedge-broker-api`

The single source of truth for the [`BrokerAdapter`] trait and the
small set of types every Hot_Path broker crate implements against:

| Symbol             | Purpose |
|--------------------|---------|
| `BrokerAdapter`    | `async_trait` defining `submit` / `modify` / `cancel` / `status` / `metrics` / `ready` / `broker_id`. |
| `OrderIntent`      | Broker-agnostic projection of `OrderIntent_v1`. Each adapter owns the per-broker translator that maps this to its REST / WebSocket payload. |
| `OrderModification`| Subset of an existing order that can be adjusted without cancel-and-re-submit. |
| `OrderStatus`      | Read-side projection returned by `status()`. |
| `SubmitAck`        | Response from a successful `submit()`. |
| `BrokerError`      | Stable error taxonomy (`NotReady`, `Rejected`, `Transient`, `Network`, `Http`, `Auth`, `InvalidApprovalToken`, `UnknownOrderId`, `Internal`). |
| `BrokerMetric`     | The `broker.metric.<broker>` payload published on every request. |
| `ReadyState`       | `Ready`, `ConfigError(reason)`, `Disconnected(reason)`. |
| `MetricPublisher`  | Pluggable transport for `BrokerMetric`. The default `VecMetricRecorder` is used in tests. |

### Why a separate crate

The trait must sit at a level no broker depends on a sibling broker.
`hedge-exec` (the Execution_Engine) and every `hedge-broker-*` crate
depend on `hedge-broker-api`; the per-broker crates do **not**
depend on each other. New brokers can be added without touching
`hedge-exec`.

### Hot_Path discipline

- No `pyo3`, `numpy`, `pandas`, `reqwest::blocking`, or cloud LLM SDK.
- All async; no blocking I/O.
- Errors are values, not panics — adapters map every broker response
  to a `BrokerError` variant.

Task **17.1** of the implementation plan.
