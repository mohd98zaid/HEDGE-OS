# PROJECT HEDGE — Binance Crypto Module

Standalone Python package for the Binance crypto pipeline.
Lives alongside but completely independent of `hedge_warm_ai`.

## Services

| Console script    | NATS subject (pub)       | Description                          |
|-------------------|--------------------------|--------------------------------------|
| `binance-feed`    | `crypto.tick.<symbol>`   | WebSocket market-data feed           |
| `binance-risk`    | `crypto.risk.verdict`    | Pre-trade risk guard                 |
| `binance-strategy`| `crypto.signal`          | Strategy / signal engine             |
| `binance-exec`    | `crypto.order.ack`       | Order execution engine               |
| `binance-position`| `crypto.position`        | Position tracker & reconciler        |

## Quick start

```bash
pip install -e python/binance_module
binance-feed --check
```
