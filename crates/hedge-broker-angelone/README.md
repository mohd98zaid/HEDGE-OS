# `hedge-broker-angelone`

[`BrokerAdapter`] implementation against **Angel One SmartAPI**
(<https://smartapi.angelbroking.com/docs>).

### Layout

* `client.rs` — async `reqwest::Client` REST wrapper. Handles
  SmartAPI's specific header set (`Authorization: Bearer <jwt>`,
  `X-PrivateKey`, `X-UserType`, `X-SourceID`, `X-Client*IP`,
  `X-MACAddress`).
* `translator.rs` — `OrderIntent` → SmartAPI JSON body translator.
* `lib.rs` — `AngelOneBroker` composing the two and emitting
  `broker.metric.angel_one` after every request.

### Auth

SmartAPI uses an `api_key` + `jwtToken` (minted by the login flow) +
`client_code`. The credentials struct also carries network identification
fields (`local_ip`, `public_ip`, `mac_address`) that SmartAPI requires
on every request — empty values are accepted with sensible defaults
(`0.0.0.0`, `00:00:00:00:00:00`).

Missing or empty credentials cause `ready()` to return
[`ReadyState::ConfigError`] and `submit()` to fail closed with
[`BrokerError::NotReady`] (R7.5).

### Production protocol gaps

The SmartAPI WebSocket binary tick protocol lives in `hedge-market-data`.
The order-details GET endpoint has sparse public docs; production callers
should consider switching to `getOrderBook` + local filtering. See the
`// TODO: production protocol` markers in `client.rs`.

Task **17.1** of the implementation plan.
