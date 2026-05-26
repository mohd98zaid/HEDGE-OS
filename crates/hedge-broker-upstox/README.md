# hedge-broker-upstox

[`BrokerAdapter`](../hedge-broker-api/src/lib.rs) implementation against
[Upstox API v2](https://upstox.com/developer/api-documentation/).

## Auth

Upstox uses a daily OAuth-minted `access_token` plus a long-lived
`api_key` (the developer-app key). Both are required:

* `api_key` — the developer-portal app key (one-time setup).
* `api_secret` — the developer-portal app secret (one-time setup).
* `access_token` — minted daily via the Upstox login redirect flow.
  The token is valid until 03:30 IST the following day.

Missing or empty credentials cause `ready()` to return
`ReadyState::ConfigError` and `submit()` to fail closed (R7.5).

## Endpoints used

| Verb | Path | Purpose |
|---|---|---|
| `POST` | `/v2/order/place` | Place a new order |
| `PUT` | `/v2/order/modify` | Modify a working order |
| `DELETE` | `/v2/order/cancel` | Cancel a working order |
| `GET` | `/v2/order/details` | Order status by id |
| `GET` | `/v2/user/profile` | Liveness probe (token validation) |

## Order status mapping

| Upstox status | FSM state |
|---|---|
| `complete` | `Filled` |
| `cancelled` / `expired` | `Cancelled` |
| `rejected` | `Rejected` |
| `partial filled` / `partially filled` | `PartiallyFilled` |
| `open` / `validation pending` / `put order req received` / `modify pending` / `cancel pending` | `Submitted` |
| _(unknown)_ | `Submitted` (conservative fallback) |

## Configuration

Set in `/etc/hedge/config.yaml` under `brokers.primary` or `brokers.backup`:

```yaml
brokers:
  primary: upstox
  backup:  angel_one
```

Credentials come from environment variables (or the `BrokerConfig`
section once it is extended):

```env
HEDGE_UPSTOX_API_KEY=your_app_key_here
HEDGE_UPSTOX_API_SECRET=your_app_secret_here
HEDGE_UPSTOX_ACCESS_TOKEN=your_daily_access_token_here
```
