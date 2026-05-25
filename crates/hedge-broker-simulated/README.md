# `hedge-broker-simulated`

In-process [`BrokerAdapter`] used by the Replay_Engine (R22.4) and the
test suite. Unlike the live-broker crates, this is a **fully functional**
broker — no placeholders.

### What it does

* Holds a per-symbol **in-memory orderbook** ([`orderbook::OrderBook`])
  populated from recorded ticker data or a test-supplied `Vec<BookLevel>`.
* Walks the book on every `submit()`, deriving synthetic fills in
  price-time order (best price first).
* Tracks each order through the full FSM (`New → Submitted → {PartiallyFilled
  →} Filled | Cancelled | Rejected`) via [`lifecycle::OrderRecord`].
* Emits a [`BrokerMetric`] on `broker.metric.simulated` for every
  request through the supplied `MetricPublisher`.
* Returns [`ReadyState::ConfigError`] when constructed with
  `ready_at_construction = false` so the fail-closed contract is testable.

### Determinism

Property 12 (Replay Determinism) is the contract this crate exists to
satisfy. There are no clocks, no randomness, no global state. Identical
input sequences against identical starting books always produce identical
fills and FSM transitions.

### Use

```rust,no_run
use hedge_broker_simulated::{BookLevel, OrderBook, SimulatedBroker, SimulatedBrokerConfig};
use hedge_broker_api::VecMetricRecorder;
use std::sync::Arc;

# tokio_test::block_on(async {
let (broker, recorder) = SimulatedBroker::with_recorder();
broker.set_book(
    1, // symbol_raw
    OrderBook::from_levels(
        &[],
        &[BookLevel::new(100_00, 5), BookLevel::new(101_00, 5)],
    ),
);
// then submit / modify / cancel / status against the broker
# });
```

Task **17.1** of the implementation plan.
