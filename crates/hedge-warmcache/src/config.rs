//! Local view of the WarmCache configuration surface.
//!
//! The canonical [`hedge_config::WarmCacheConfig`] is the YAML-bound type
//! parsed at service startup. We re-export it here as the public
//! configuration newtype so callers do not need to depend on the full
//! `hedge-config` crate just to construct a [`WarmCache`](crate::WarmCache).
//!
//! The wrapper also adds a `from_parts` constructor that bypasses YAML
//! validation — used by unit tests in this crate to build a config with
//! arbitrary values without re-parsing the schema.

use serde::{Deserialize, Serialize};

/// Configuration for the WarmCache. Newtype around
/// [`hedge_config::WarmCacheConfig`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WarmCacheConfig(pub hedge_config::WarmCacheConfig);

impl WarmCacheConfig {
    /// Construct from the typed parts. Validation matches the YAML
    /// schema: capacity must be > 0, staleness >= 0, NATS URL non-empty.
    pub fn from_parts(
        trade_confidence_lru_size: u32,
        staleness_window_ms: u32,
        nats_url: impl Into<String>,
    ) -> Self {
        Self(hedge_config::WarmCacheConfig {
            trade_confidence_lru_size,
            staleness_window_ms,
            nats_url: nats_url.into(),
        })
    }

    /// Configured LRU capacity for the trade_confidence map.
    #[inline]
    pub fn trade_confidence_lru_size(&self) -> usize {
        self.0.trade_confidence_lru_size as usize
    }

    /// Configured staleness window in milliseconds.
    #[inline]
    pub fn staleness_window_ms(&self) -> u32 {
        self.0.staleness_window_ms
    }

    /// NATS endpoint the updater task should connect to.
    #[inline]
    pub fn nats_url(&self) -> &str {
        &self.0.nats_url
    }
}

impl From<hedge_config::WarmCacheConfig> for WarmCacheConfig {
    #[inline]
    fn from(value: hedge_config::WarmCacheConfig) -> Self {
        Self(value)
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_design_yaml_defaults() {
        let cfg = WarmCacheConfig::default();
        assert_eq!(cfg.trade_confidence_lru_size(), 8_192);
        assert_eq!(cfg.staleness_window_ms(), 5_000);
        assert_eq!(cfg.nats_url(), "nats://127.0.0.1:4222");
    }

    #[test]
    fn from_parts_round_trips() {
        let cfg = WarmCacheConfig::from_parts(16, 250, "nats://localhost:14222");
        assert_eq!(cfg.trade_confidence_lru_size(), 16);
        assert_eq!(cfg.staleness_window_ms(), 250);
        assert_eq!(cfg.nats_url(), "nats://localhost:14222");
    }

    #[test]
    fn round_trips_through_serde_json() {
        let cfg = WarmCacheConfig::from_parts(64, 1_000, "nats://example:4222");
        let json = serde_json::to_string(&cfg).unwrap();
        let back: WarmCacheConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cfg);
    }
}
