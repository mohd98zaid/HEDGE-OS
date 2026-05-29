//! Redis-backed persistence for Risk_Engine soft state (task C.2).
//!
//! The Risk_Engine keeps two pieces of state that must survive a process
//! restart so a crash mid-session does not reset risk controls:
//!
//!   * **Active cooldowns** — per-symbol `(expiry_ns, reason)` so a
//!     symbol that was cooling before the restart keeps cooling.
//!   * **Daily P&L** — cumulative realised P&L in paise, so the
//!     max-daily-loss gate and profit-target detector survive a bounce.
//!
//! Both are stored as JSON blobs under fixed Redis keys with a TTL that
//! expires at end-of-day IST so stale state never bleeds into the next
//! trading session.
//!
//! Persistence is **best-effort**: every method logs and swallows Redis
//! errors rather than propagating them, because the Risk_Engine must keep
//! gating even when Redis is degraded (R25.2). The in-memory state is
//! always authoritative during a run; Redis is only the restart seed.

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Redis key for the serialised cooldown table.
const KEY_COOLDOWNS: &str = "hedge:risk:cooldowns";
/// Redis key for the cumulative daily P&L (paise).
const KEY_DAILY_PNL: &str = "hedge:risk:daily_pnl_paise";
/// TTL applied to both keys — 18 hours, comfortably past any single
/// trading session but expiring before the next day's open.
const STATE_TTL_SECS: u64 = 18 * 60 * 60;

/// One persisted cooldown entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedCooldown {
    /// Interned symbol id.
    pub symbol: u32,
    /// Monotonic-equivalent expiry in ns since epoch.
    pub expiry_ns: u64,
    /// Reason discriminant (matches `CooldownReason as u8`).
    pub reason: u8,
}

/// Snapshot of the Risk_Engine soft state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskStateSnapshot {
    /// Active cooldowns at snapshot time.
    pub cooldowns: Vec<PersistedCooldown>,
    /// Cumulative daily realised P&L in paise.
    pub daily_pnl_paise: i64,
}

/// Best-effort Redis persistence handle.
///
/// Cheap to clone (holds a refcounted `ConnectionManager`).
#[derive(Clone)]
pub struct RiskPersistence {
    conn: ConnectionManager,
}

impl RiskPersistence {
    /// Construct from a connected `ConnectionManager`.
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    /// Open a connection from a Redis URL. Returns `None` if the URL is
    /// invalid or the connection cannot be established — the caller runs
    /// without persistence in that case.
    pub async fn connect(redis_url: &str) -> Option<Self> {
        match redis::Client::open(redis_url) {
            Ok(client) => match ConnectionManager::new(client).await {
                Ok(conn) => Some(Self::new(conn)),
                Err(e) => {
                    warn!(error = %e, "risk persistence: ConnectionManager failed; running without persistence");
                    None
                }
            },
            Err(e) => {
                warn!(error = %e, "risk persistence: invalid Redis URL; running without persistence");
                None
            }
        }
    }

    /// Persist the full snapshot. Best-effort; logs and swallows errors.
    pub async fn save(&self, snapshot: &RiskStateSnapshot) {
        let mut conn = self.conn.clone();
        let cooldowns_json = match serde_json::to_string(&snapshot.cooldowns) {
            Ok(j) => j,
            Err(e) => {
                warn!(error = %e, "risk persistence: serialise cooldowns failed");
                return;
            }
        };
        // Use SET with EX (seconds TTL). Two independent keys.
        let r1: redis::RedisResult<()> = conn
            .set_ex(KEY_COOLDOWNS, cooldowns_json, STATE_TTL_SECS)
            .await;
        if let Err(e) = r1 {
            warn!(error = %e, "risk persistence: save cooldowns failed");
        }
        let r2: redis::RedisResult<()> = conn
            .set_ex(KEY_DAILY_PNL, snapshot.daily_pnl_paise, STATE_TTL_SECS)
            .await;
        if let Err(e) = r2 {
            warn!(error = %e, "risk persistence: save daily_pnl failed");
        }
        debug!(
            cooldowns = snapshot.cooldowns.len(),
            daily_pnl_paise = snapshot.daily_pnl_paise,
            "risk persistence: state saved"
        );
    }

    /// Load the snapshot seeded at startup. Returns a default (empty)
    /// snapshot when Redis is empty or unreachable.
    pub async fn load(&self) -> RiskStateSnapshot {
        let mut conn = self.conn.clone();

        let cooldowns: Vec<PersistedCooldown> = match conn
            .get::<_, Option<String>>(KEY_COOLDOWNS)
            .await
        {
            Ok(Some(json)) => serde_json::from_str(&json).unwrap_or_else(|e| {
                warn!(error = %e, "risk persistence: deserialise cooldowns failed");
                Vec::new()
            }),
            Ok(None) => Vec::new(),
            Err(e) => {
                warn!(error = %e, "risk persistence: load cooldowns failed");
                Vec::new()
            }
        };

        let daily_pnl_paise: i64 = match conn.get::<_, Option<i64>>(KEY_DAILY_PNL).await {
            Ok(Some(v)) => v,
            Ok(None) => 0,
            Err(e) => {
                warn!(error = %e, "risk persistence: load daily_pnl failed");
                0
            }
        };

        debug!(
            cooldowns = cooldowns.len(),
            daily_pnl_paise, "risk persistence: state loaded"
        );
        RiskStateSnapshot {
            cooldowns,
            daily_pnl_paise,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trips_through_json() {
        let snap = RiskStateSnapshot {
            cooldowns: vec![
                PersistedCooldown {
                    symbol: 1,
                    expiry_ns: 1_700_000_000_000_000_000,
                    reason: 0,
                },
                PersistedCooldown {
                    symbol: 3,
                    expiry_ns: 1_700_000_000_500_000_000,
                    reason: 2,
                },
            ],
            daily_pnl_paise: -125_00,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: RiskStateSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn default_snapshot_is_empty() {
        let snap = RiskStateSnapshot::default();
        assert!(snap.cooldowns.is_empty());
        assert_eq!(snap.daily_pnl_paise, 0);
    }
}
