# `hedge-broker-zerodha`

[`BrokerAdapter`] implementation against Zerodha **Kite Connect v3**
(<https://kite.trade/docs/connect/v3/>).

### Layout

* `client.rs` — async `reqwest::Client` REST wrapper with auth header
  injection, timeouts, and stable `BrokerError` mapping.
* `translator.rs` — `OrderIntent` → Kite form translator
  (`OrderType::Limit` → `"LIMIT"`, paise → rupee decimal string,
  exchange tag, etc.).
* `lib.rs` — `ZerodhaBroker` composing the two and emitting
  `broker.metric.zerodha` after every request.

### Auth

Kite Connect uses an `api_key` plus a daily `access_token`. The
adapter's [`KiteCredentials`] type wraps the pair; missing or empty
credentials cause `ready()` to return [`ReadyState::ConfigError`] and
`submit()` to fail closed with [`BrokerError::NotReady`] (R7.5).

### Production protocol gaps

Kite's binary tick / post-trade WebSocket protocol is implemented in
`hedge-market-data`, not here. Where Kite's REST surface has insufficient
public documentation we leave a `// TODO: production protocol`
marker.

### Hot_Path discipline

* **Async only**; no `reqwest::blocking`. The `forbid_modules` CI gate
  (task 8.1) enforces this on the dependency closure.
* Every operation emits a `BrokerMetric` (R7.4).
* Errors are values, not panics — every broker response is mapped to a
  `BrokerError` variant.

Task **17.1** of the implementation plan.
