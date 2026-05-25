//! Process-wide config installation strategies.
//!
//! Hot_Path processes (Risk_Engine, Execution_Engine, …) **pin** the
//! configuration at startup to avoid mid-session changes. Non-Hot_Path
//! processes (UI gateway, supervisor) are allowed to **swap** the config in
//! response to SIGHUP. This module provides both shapes and lets each
//! binary pick the right one.
//!
//! Note: this crate is intentionally agnostic about how SIGHUP is delivered;
//! that wiring is per-binary. Here we expose the storage primitives only.

use std::sync::Arc;
use std::sync::OnceLock;

use parking_lot::RwLock;

use crate::error::ConfigError;
use crate::models::HedgeConfig;

// ---------------------------------------------------------------------------
// Hot_Path: install-once, never change ---------------------------------------
// ---------------------------------------------------------------------------

/// Process-global, install-once configuration. Used by every Hot_Path binary
/// at startup — `PinnedConfig::install(cfg)` is called exactly once before
/// the runtime starts handling traffic. Subsequent calls return
/// `Err(ConfigError::AlreadyInstalled)` so a programming bug is detected
/// loudly.
#[derive(Debug, Default)]
pub struct PinnedConfig {
    inner: OnceLock<Arc<HedgeConfig>>,
}

impl PinnedConfig {
    /// Construct an empty slot. Tests can build their own per-test slot;
    /// production code uses [`global`].
    pub const fn new() -> Self {
        Self { inner: OnceLock::new() }
    }

    /// Install the configuration. Returns the installed value as an `Arc` on
    /// success; returns `Err(ConfigError::AlreadyInstalled)` if a previous
    /// installation already occurred.
    pub fn install(&self, cfg: HedgeConfig) -> Result<Arc<HedgeConfig>, ConfigError> {
        let arc = Arc::new(cfg);
        self.inner
            .set(arc.clone())
            .map(|_| arc)
            .map_err(|_| ConfigError::AlreadyInstalled)
    }

    /// Returns `Some(Arc)` if the config has been installed, else `None`.
    pub fn get(&self) -> Option<Arc<HedgeConfig>> {
        self.inner.get().cloned()
    }

    /// Returns the installed `Arc` or panics. Hot_Path code that runs after
    /// startup is expected to know that installation has occurred.
    pub fn get_or_panic(&self) -> Arc<HedgeConfig> {
        self.inner
            .get()
            .cloned()
            .expect("hot-path config accessed before install")
    }
}

/// Process-wide singleton used by Hot_Path binaries. Each binary calls
/// `hedge_config::pinned::global().install(cfg)` exactly once during startup
/// and reads via `global().get_or_panic()` afterwards.
pub fn global() -> &'static PinnedConfig {
    static GLOBAL: PinnedConfig = PinnedConfig::new();
    &GLOBAL
}

// ---------------------------------------------------------------------------
// Non-Hot_Path: SIGHUP-reloadable -------------------------------------------
// ---------------------------------------------------------------------------

/// Reloadable configuration for non-Hot_Path processes. Reads return a cheap
/// `Arc<HedgeConfig>` snapshot, and writes (driven by the binary's SIGHUP
/// handler) atomically swap the underlying config so in-flight readers
/// continue to see the previous value until they re-read.
#[derive(Debug, Clone)]
pub struct MutableConfig {
    inner: Arc<RwLock<Arc<HedgeConfig>>>,
}

impl MutableConfig {
    /// Construct a mutable holder seeded with `initial`.
    pub fn new(initial: HedgeConfig) -> Self {
        Self { inner: Arc::new(RwLock::new(Arc::new(initial))) }
    }

    /// Snapshot the current configuration. Cheap: clones an `Arc`.
    pub fn current(&self) -> Arc<HedgeConfig> {
        self.inner.read().clone()
    }

    /// Atomically swap in a new configuration; returns the previous one.
    pub fn replace(&self, new: HedgeConfig) -> Arc<HedgeConfig> {
        let new = Arc::new(new);
        std::mem::replace(&mut *self.inner.write(), new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defaults;

    #[test]
    fn pinned_config_install_once() {
        let pinned = PinnedConfig::new();
        let cfg = defaults::hedge_config();
        let arc = pinned.install(cfg.clone()).unwrap();
        assert_eq!(*arc, cfg);

        // Second install fails.
        let err = pinned.install(defaults::hedge_config()).unwrap_err();
        assert!(matches!(err, ConfigError::AlreadyInstalled));

        // Reads still see the original.
        assert_eq!(*pinned.get_or_panic(), cfg);
    }

    #[test]
    fn pinned_config_get_returns_none_before_install() {
        let pinned = PinnedConfig::new();
        assert!(pinned.get().is_none());
    }

    #[test]
    fn mutable_config_swap() {
        let cfg = defaults::hedge_config();
        let mc = MutableConfig::new(cfg.clone());
        assert_eq!(*mc.current(), cfg);

        let mut next = cfg.clone();
        next.capital.base_inr = 50_000;
        let prev = mc.replace(next.clone());
        assert_eq!(*prev, cfg);
        assert_eq!(mc.current().capital.base_inr, 50_000);
    }
}
