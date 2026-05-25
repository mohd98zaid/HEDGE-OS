//! `hedge-warmcache` — last-known-value cache populated asynchronously
//! from the Warm_AI_Pipeline `ai.*` NATS subjects, read by the Hot_Path
//! [`hedge_risk`](https://docs.rs/hedge-risk) engine through a single
//! atomic pointer load (R9.4, R9.5, R17.4, R19.7).
//!
//! ### Architecture
//!
//! Two halves of the same crate:
//!
//! | Half     | Type                                | Cadence               | Hot_Path? |
//! |----------|-------------------------------------|-----------------------|-----------|
//! | Reader   | [`WarmCache`]                       | per signal evaluation | yes       |
//! | Writer   | [`WarmCacheUpdater`]                | per `ai.*` event      | no        |
//!
//! The reader holds every published value behind an
//! [`arc_swap::ArcSwap<Snapshot>`] for the four scalar/per-symbol fields
//! and a sharded [`dashmap::DashMap`] for the per-correlation
//! `trade_confidence` map. Lookups return in nanoseconds and never
//! allocate (R9.4 budget: < 50 µs for the AI-scoring fetch stage).
//!
//! The writer is a single tokio task (no extra threads) subscribed to
//! the canonical `ai.*` subjects. Decode is `serde_json` for every
//! subject (the Warm_AI_Pipeline emits JSON; design § Data Models §
//! Warm_AI_Pipeline Events (JSON)). On every event the writer copies
//! the previous snapshot, applies one field, and `ArcSwap::store`s the
//! new `Arc<Snapshot>` — readers never see a partially-applied update.
//!
//! ### Subscription set
//!
//! ```text
//! ai.rank.*               -> trade_confidence(correlation_id)
//! ai.regime.changed       -> market_stability  (label-to-factor table)
//! ai.psych.stability      -> trader_stability
//! ai.priority.changed.*   -> priority(symbol)
//! ai.news.impact.*        -> news_impact(symbol)
//! ```
//!
//! ### Hot_Path discipline (R30)
//!
//! * `#![forbid(unsafe_code)]` (R30, defensive).
//! * No `pyo3`, `numpy`, `pandas`, `python-` runtime; verified by
//!   [`forbid::FORBIDDEN_DEPENDENCIES`] and the CI gate at
//!   `scripts/check-forbidden-deps.sh`.
//! * No `tokio::time::interval`/`sleep` polling on the read path; the
//!   updater's own tokio task uses `tokio::select!` over long-lived
//!   subscriptions, never a polled timer (R30.3, enforced by
//!   `scripts/check-no-polling.sh`).
//! * No `reqwest::blocking`, no cloud LLM SDKs.
//!
//! ### Read-side fallback
//!
//! When `trade_confidence(cid)` misses or is stale (older than
//! `staleness_window_ms`), [`WarmCache::fallback_confidence`] returns
//! the original `Signal_v1.confidence` so the Risk_Engine can still
//! compute `Adaptive_Risk` (R24.2).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod cache;
pub mod config;
pub mod forbid;
pub mod lru;
pub mod snapshot;
pub mod updater;

pub use cache::WarmCache;
pub use config::WarmCacheConfig;
pub use forbid::FORBIDDEN_DEPENDENCIES;
pub use snapshot::{NewsImpactSnapshot, Snapshot, MAX_TRACKED_SYMBOLS};
pub use updater::WarmCacheUpdater;
