//! [`SupervisorState`] — persisted last-known-healthy bring-up snapshot.
//!
//! The supervisor persists a small JSON file every state change so that
//! after a host restart (`docker compose up` or systemd) the new
//! supervisor process can resume from the last-known-healthy
//! configuration rather than from a blank slate (R29.6, design
//! § Self-Healing Flow).
//!
//! The state intentionally captures only the *operational* counters the
//! Recovery_Policy needs to make a coherent decision after restart:
//!
//! * per-source WebSocket reconnect attempt counters (so the next
//!   `WsDisconnected` for the same source picks up where the previous
//!   process left off — no skipped attempts, R25.1);
//! * the broker pair the supervisor last failed over to (so the
//!   previous failover is honoured even across restart, R6.5);
//! * the active Ollama model (so an existing fallback survives
//!   restart, R10.9);
//! * the `cache.redis.degraded` latch (so the UI is not surprised by a
//!   "healthy" announcement immediately after a supervisor restart that
//!   did not yet observe a real recovery, R25.2);
//! * the active per-source mitigation labels for external APIs (R25.5).
//!
//! The on-disk format is pretty-printed JSON (`serde_json::to_writer_pretty`)
//! so an operator can inspect / hand-edit `/var/lib/hedge/supervisor/state.json`
//! during incident response.
//!
//! ### Atomicity
//!
//! [`SupervisorState::save_to`] writes to a sibling `*.tmp` file then
//! `rename`s it over the destination. On Unix this is atomic; on
//! Windows the rename is best-effort (we accept the small race window
//! because the supervisor's restart bring-up tolerates a missing or
//! truncated file).

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use hedge_core::BrokerId;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// Default on-disk location. Overridable via [`SupervisorStateStore::with_path`].
pub const DEFAULT_STATE_PATH: &str = "/var/lib/hedge/supervisor/state.json";

/// File format version. Bump when a non-additive change to
/// [`SupervisorState`] would otherwise silently misinterpret the file.
pub const STATE_VERSION: u32 = 1;

/// Persisted supervisor state. Hand-editable JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorState {
    /// Schema version — the supervisor refuses to load a file whose
    /// `version` does not equal [`STATE_VERSION`].
    #[serde(default = "default_version")]
    pub version: u32,
    /// Per-source WebSocket reconnect attempt counter.
    #[serde(default)]
    pub ws_attempts: HashMap<String, u32>,
    /// Active broker pair — `None` when no failover is in effect, in
    /// which case the workspace config's `(primary, backup)` is used.
    #[serde(default)]
    pub active_broker: Option<BrokerId>,
    /// Last broker the supervisor failed *over to*. Held as an
    /// audit-trail field for the UI and replay recorder.
    #[serde(default)]
    pub last_failover: Option<BrokerFailoverRecord>,
    /// Active Ollama model. `None` ⇒ use the configured primary.
    #[serde(default)]
    pub active_ollama_model: Option<String>,
    /// Whether the supervisor previously announced
    /// `cache.redis.degraded`. Used to suppress duplicate emissions on
    /// restart.
    #[serde(default)]
    pub redis_degraded: bool,
    /// Active per-source mitigations for external APIs.
    #[serde(default)]
    pub active_mitigations: HashMap<String, String>,
    /// Wall-clock timestamp of the last update (`hedge_core::now_ns()`).
    #[serde(default)]
    pub updated_ts_ns: u64,
}

fn default_version() -> u32 {
    STATE_VERSION
}

impl Default for SupervisorState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            ws_attempts: HashMap::new(),
            active_broker: None,
            last_failover: None,
            active_ollama_model: None,
            redis_degraded: false,
            active_mitigations: HashMap::new(),
            updated_ts_ns: 0,
        }
    }
}

/// Audit record for a supervisor-issued broker failover.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerFailoverRecord {
    /// Failing broker.
    pub from: BrokerId,
    /// Backup broker the supervisor swapped to.
    pub to: BrokerId,
    /// Wall-clock timestamp at the swap.
    pub ts_ns: u64,
}

impl SupervisorState {
    /// Construct a fresh state with no active recovery.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump the per-source attempt counter and return the *new* value
    /// (1-indexed for the first attempt). Mirrors the behaviour
    /// [`crate::policy::RecoveryPolicy`] applies in-process.
    pub fn record_ws_attempt(&mut self, source: &str) -> u32 {
        let entry = self.ws_attempts.entry(source.to_string()).or_insert(0);
        let n = entry.saturating_add(1);
        *entry = n;
        n
    }

    /// Reset the per-source attempt counter on a successful reconnect.
    pub fn reset_ws_attempt(&mut self, source: &str) {
        self.ws_attempts.remove(source);
    }

    /// Record that the supervisor switched the active broker from
    /// `from` to `to`.
    pub fn record_failover(&mut self, from: BrokerId, to: BrokerId, ts_ns: u64) {
        self.active_broker = Some(to);
        self.last_failover = Some(BrokerFailoverRecord { from, to, ts_ns });
    }

    /// Record an Ollama model swap.
    pub fn record_ollama_swap(&mut self, to: &str) {
        self.active_ollama_model = Some(to.to_string());
    }

    /// Mark the Redis-degraded latch.
    pub fn set_redis_degraded(&mut self, degraded: bool) {
        self.redis_degraded = degraded;
    }

    /// Record an active per-source mitigation.
    pub fn record_mitigation(&mut self, source: &str, mitigation: &str) {
        self.active_mitigations
            .insert(source.to_string(), mitigation.to_string());
    }

    /// Touch the updated timestamp. Called from [`SupervisorStateStore::save`].
    pub fn touch(&mut self, ts_ns: u64) {
        self.updated_ts_ns = ts_ns;
    }
}

// ---------------------------------------------------------------------------
// On-disk store -------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Errors surfaced by the persistent state store. Wrapping the
/// underlying `io::Error` and `serde_json::Error` keeps the run-loop
/// handling code simple and keeps the actual cause in the error chain.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// I/O failure (read, write, rename).
    #[error("supervisor state I/O at {path}: {source}")]
    Io {
        /// Path the operation was performed against.
        path: PathBuf,
        /// Wrapped source error.
        #[source]
        source: io::Error,
    },
    /// JSON encode/decode failure.
    #[error("supervisor state JSON at {path}: {source}")]
    Json {
        /// Path the operation was performed against.
        path: PathBuf,
        /// Wrapped source error.
        #[source]
        source: serde_json::Error,
    },
    /// Loaded file's `version` field disagrees with [`STATE_VERSION`].
    #[error(
        "supervisor state at {path} has version {found}; supervisor expects {expected}"
    )]
    VersionMismatch {
        /// Path the file was loaded from.
        path: PathBuf,
        /// Version found on disk.
        found: u32,
        /// Version the running supervisor expects.
        expected: u32,
    },
}

/// Persistent state store wrapping a [`SupervisorState`] behind a mutex.
///
/// The store is `Clone` (cheap — wraps an `Arc`) so multiple supervisor
/// stages can hold a handle and update state concurrently.
#[derive(Clone)]
pub struct SupervisorStateStore {
    path: PathBuf,
    state: Arc<Mutex<SupervisorState>>,
}

impl SupervisorStateStore {
    /// Construct a store rooted at the default path
    /// (`/var/lib/hedge/supervisor/state.json`).
    pub fn default_path() -> Self {
        Self::with_path(DEFAULT_STATE_PATH)
    }

    /// Construct a store rooted at `path`. Does not touch the
    /// filesystem; call [`SupervisorStateStore::load_or_default`] to
    /// materialise the on-disk content.
    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            state: Arc::new(Mutex::new(SupervisorState::default())),
        }
    }

    /// Borrow the store's path.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Snapshot the current state. Cheap clone of the underlying struct.
    pub fn snapshot(&self) -> SupervisorState {
        self.state.lock().clone()
    }

    /// Run `f` against a mutable reference to the state. Used to apply
    /// a coherent update under one lock acquire/release cycle.
    pub fn with_state<R>(&self, f: impl FnOnce(&mut SupervisorState) -> R) -> R {
        let mut g = self.state.lock();
        f(&mut g)
    }

    /// Load the persisted state from disk. Returns [`SupervisorState::default()`]
    /// when the file does not exist, after logging a structured
    /// `tracing::info!` so an operator can confirm a clean start.
    ///
    /// Other I/O / decode failures propagate.
    pub fn load_or_default(&self) -> Result<SupervisorState, StateError> {
        match fs::read(&self.path) {
            Ok(bytes) => {
                let state: SupervisorState =
                    serde_json::from_slice(&bytes).map_err(|source| StateError::Json {
                        path: self.path.clone(),
                        source,
                    })?;
                if state.version != STATE_VERSION {
                    return Err(StateError::VersionMismatch {
                        path: self.path.clone(),
                        found: state.version,
                        expected: STATE_VERSION,
                    });
                }
                tracing::info!(
                    path = %self.path.display(),
                    ws_sources = state.ws_attempts.len(),
                    "supervisor: loaded last-known-healthy state",
                );
                *self.state.lock() = state.clone();
                Ok(state)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                tracing::info!(
                    path = %self.path.display(),
                    "supervisor: no existing state file, starting fresh",
                );
                let fresh = SupervisorState::default();
                *self.state.lock() = fresh.clone();
                Ok(fresh)
            }
            Err(source) => Err(StateError::Io {
                path: self.path.clone(),
                source,
            }),
        }
    }

    /// Persist the current state to disk via a temp-file + rename.
    pub fn save(&self) -> Result<(), StateError> {
        let snapshot = self.snapshot();
        save_to_path(&self.path, &snapshot)
    }

    /// Apply `f` and persist the resulting state in one call.
    pub fn update_and_save<R>(
        &self,
        f: impl FnOnce(&mut SupervisorState) -> R,
    ) -> Result<R, StateError> {
        let result = self.with_state(|s| {
            let r = f(s);
            s.touch(hedge_core::now_ns());
            r
        });
        self.save()?;
        Ok(result)
    }
}

/// Atomic write helper. Public so binaries that prefer to
/// snapshot+save explicitly (no in-memory store) can use it.
pub fn save_to_path(path: &Path, state: &SupervisorState) -> Result<(), StateError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|source| StateError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
    }

    let tmp_path = with_tmp_suffix(path);
    let bytes = serde_json::to_vec_pretty(state).map_err(|source| StateError::Json {
        path: path.to_path_buf(),
        source,
    })?;
    fs::write(&tmp_path, bytes).map_err(|source| StateError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    fs::rename(&tmp_path, path).map_err(|source| StateError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

/// Compute the sibling temp-file path used by [`save_to_path`].
fn with_tmp_suffix(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn load_or_default_returns_default_when_file_absent() {
        let dir = tempdir().unwrap();
        let store = SupervisorStateStore::with_path(dir.path().join("state.json"));
        let s = store.load_or_default().unwrap();
        assert_eq!(s, SupervisorState::default());
    }

    #[test]
    fn save_then_load_round_trips_state() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let store = SupervisorStateStore::with_path(&path);

        store
            .update_and_save(|s| {
                s.record_ws_attempt("nse_l1");
                s.record_ws_attempt("nse_l1");
                s.record_failover(BrokerId::Zerodha, BrokerId::Dhan, 42);
                s.record_ollama_swap("mistral:7b");
                s.set_redis_degraded(true);
                s.record_mitigation("news", "throttle");
            })
            .unwrap();

        // Fresh store reading the same path sees the persisted state.
        let store2 = SupervisorStateStore::with_path(&path);
        let loaded = store2.load_or_default().unwrap();

        assert_eq!(loaded.ws_attempts.get("nse_l1"), Some(&2));
        assert_eq!(loaded.active_broker, Some(BrokerId::Dhan));
        assert_eq!(
            loaded.last_failover,
            Some(BrokerFailoverRecord {
                from: BrokerId::Zerodha,
                to: BrokerId::Dhan,
                ts_ns: 42,
            })
        );
        assert_eq!(loaded.active_ollama_model.as_deref(), Some("mistral:7b"));
        assert!(loaded.redis_degraded);
        assert_eq!(
            loaded.active_mitigations.get("news"),
            Some(&"throttle".into())
        );
        assert!(loaded.updated_ts_ns > 0);
    }

    #[test]
    fn version_mismatch_is_loud() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        // Hand-write a state file with an unexpected version.
        fs::write(&path, br#"{"version":999,"ws_attempts":{}}"#).unwrap();

        let store = SupervisorStateStore::with_path(&path);
        let err = store.load_or_default().unwrap_err();
        match err {
            StateError::VersionMismatch { found, expected, .. } => {
                assert_eq!(found, 999);
                assert_eq!(expected, STATE_VERSION);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn json_decode_errors_are_propagated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        fs::write(&path, b"{garbage").unwrap();

        let store = SupervisorStateStore::with_path(&path);
        let err = store.load_or_default().unwrap_err();
        match err {
            StateError::Json { .. } => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn ws_counter_helpers_are_correct() {
        let mut s = SupervisorState::default();
        assert_eq!(s.record_ws_attempt("a"), 1);
        assert_eq!(s.record_ws_attempt("a"), 2);
        assert_eq!(s.record_ws_attempt("b"), 1);
        s.reset_ws_attempt("a");
        assert!(!s.ws_attempts.contains_key("a"));
        assert_eq!(s.ws_attempts.get("b"), Some(&1));
    }

    #[test]
    fn save_creates_missing_parent_dirs() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a/b/c/state.json");
        let store = SupervisorStateStore::with_path(&nested);
        store.update_and_save(|s| s.set_redis_degraded(true)).unwrap();
        assert!(nested.exists());
        let s = SupervisorStateStore::with_path(&nested)
            .load_or_default()
            .unwrap();
        assert!(s.redis_degraded);
    }

    #[test]
    fn save_to_path_is_atomic_via_rename() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let s = SupervisorState::default();
        save_to_path(&path, &s).unwrap();
        // The temp file should be gone after the rename.
        let tmp = with_tmp_suffix(&path);
        assert!(!tmp.exists(), "temp file should not linger");
    }
}
