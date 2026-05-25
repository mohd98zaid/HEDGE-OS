//! `hedge-market-data` — Market_Data_Engine (task **10.1**).
//!
//! Implements design § Components § Market_Data_Engine and Requirements
//! R1.1–R1.8. The crate provides:
//!
//! * [`protocol`] — vendor-agnostic [`MarketDataProtocol`] trait plus three
//!   placeholder parsers (`Nse`, `Bse`, `OptionsChain`) that all decode the
//!   canonical JSON development form and have a `// TODO: production
//!   protocol — replace with vendor-specific binary parser` marker so the
//!   future swap is greppable.
//! * [`adapter`] — [`WsAdapter`] trait and the live `tokio_tungstenite`
//!   implementation [`LiveWsAdapter`] with the full exponential-backoff
//!   reconnect schedule and `md.connection.<source>` event emission
//!   (R1.6).
//! * [`normalizer`] — [`TickNormalizer`], the per-tick stamp of
//!   `ts_recv_ns`, `correlation_id`, and the symbol → `SymbolId`
//!   resolution.
//! * [`interner`] — concurrent [`SymbolInterner`] backed by `DashMap`
//!   plus an atomic id counter.
//! * [`distributor`] — [`Distributor`], the per-symbol
//!   `tokio::broadcast` fan-out used by the Orderflow_Engine,
//!   Feature_Extraction_Engine, and the UI gateway (R1.8, lossy on slow
//!   consumers — acceptable for market data).
//! * [`breadth`] — [`BreadthAggregator`], the incremental sector and
//!   volatility breadth aggregator that publishes on
//!   `md.breadth.sector` / `md.breadth.volatility` per tick batch
//!   (every 100 ticks or 250 ms, whichever fires first; R1.7).
//! * [`engine`] — [`MarketDataEngine`], the orchestrator that wires all
//!   of the above together, spawning one Tokio task per adapter and
//!   measuring per-tick latency against the R28.1 2 ms budget. Latency
//!   is recorded via the configured [`hedge_obs::tracer::LatencyEmitter`]
//!   (one `obs.latency.TickIngest` record per tick; one
//!   `obs.budget.breach.TickIngest` event when the 2 ms budget is
//!   exceeded; the matching Prometheus histogram and breach counter are
//!   updated in lock-step).
//!
//! ### Hot_Path discipline
//!
//! No blocking HTTP, no Python, no cloud LLM SDKs (R30.4, R30.7, R30.8).
//! The crate's transitive closure is verified by the workspace CI gate
//! (task 8.1).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod adapter;
pub mod breadth;
pub mod distributor;
pub mod engine;
pub mod error;
pub mod interner;
pub mod normalizer;
pub mod protocol;

// Public re-exports — keep the top-level surface ergonomic for the binary
// and for downstream Hot_Path crates that need the `Tick` POD shape.
pub use adapter::{
    build_disconnected_event, build_reconnected_event, reconnect_delay_for, ConnectionEvent,
    ConnectionStatus, LiveWsAdapter, WsAdapter, RECONNECT_CAP_MS,
};
pub use breadth::{
    BreadthAggregator, BreadthSnapshot, SectorBreadth, VolatilityBreadth, DEFAULT_BATCH_INTERVAL,
    DEFAULT_BATCH_TICKS, VOLATILITY_WINDOW,
};
pub use distributor::{Distributor, CHANNEL_CAPACITY};
pub use engine::{
    tick_to_raw_bytes, BreadthByproduct, FlatBuffersCodecBridge, MarketDataEngine, RawTickPayload,
    TICK_INGEST_BUDGET_NS, TICK_WIRE_SIZE,
};
pub use error::MarketDataError;
pub use interner::SymbolInterner;
pub use normalizer::{Exchange, TickNormalizer};
pub use protocol::{
    BseProtocolPlaceholder, MarketDataProtocol, NseProtocolPlaceholder,
    OptionsChainProtocolPlaceholder, RawTick,
};
