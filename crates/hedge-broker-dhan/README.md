# `hedge-broker-dhan`

[`BrokerAdapter`] implementation against **Dhan API v2**
(<https://dhanhq.co/docs/v2/>).

### Layout

* `client.rs` — async `reqwest::Client` REST wrapper. Auth is the
  `access-token` header + `Content-Type: application/json`.
* `translator.rs` — `OrderIntent` → Dhan JSON body translator.
* `lib.rs` — `DhanBroker` composing the two and emitting
  `broker.metric.dhan` after every request.

### Auth

Dhan uses a `client_id` plus a long-lived `access-token`. Missing or
empty credentials cause `ready()` to return [`ReadyState::ConfigError`]
and `submit()` to fail closed with [`BrokerError::NotReady`] (R7.5).

### Production protocol gaps

Dhan's WebSocket binary tick protocol lives in `hedge-market-data`.
Where Dhan's REST surface has insufficient public documentation we
leave a `// TODO: production protocol` marker.

Task **17.1** of the implementation plan.
