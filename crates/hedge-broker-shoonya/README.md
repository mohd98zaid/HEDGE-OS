# `hedge-broker-shoonya`

[`BrokerAdapter`] implementation against **Shoonya / Finvasia
NorenAPI** (<https://api.shoonya.com/NorenWebApi.html>).

### Wire format quirk

Every NorenAPI endpoint accepts a single
`application/x-www-form-urlencoded` body of the form:

```
jData=<JSON>&jKey=<session_token>
```

The `client::ShoonyaClient::body_for` helper handles this serialisation
so the rest of the adapter works with normal typed `Serialize` structs.

### Auth

Shoonya uses a session token (NorenAPI `susertoken`) refreshed daily by
an out-of-process helper. Missing or empty credentials cause `ready()`
to return [`ReadyState::ConfigError`] and `submit()` to fail closed
(R7.5).

The `app_key_digest` helper exposes the canonical
`SHA256(uid|api_key)` hex digest some flows require.

### Production protocol gaps

The Shoonya WebSocket binary tick protocol lives in `hedge-market-data`,
not here. Where Shoonya's REST surface has insufficient public
documentation we leave a `// TODO: production protocol` marker.

Task **17.1** of the implementation plan.
