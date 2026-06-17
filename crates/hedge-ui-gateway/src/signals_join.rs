//! `/signals` channel: join `sig.emitted` with `ai.rank.*` by `correlation_id`.
//!
//! The `/signals` UI channel surfaces a *joined* view of two NATS streams:
//!
//! 1. `sig.emitted` — Hot_Path-emitted strategy signals from the
//!    Signal_Engine.
//! 2. `ai.rank.<correlation_id_hex>` — Warm_AI_Pipeline ranking output
//!    keyed on the same `correlation_id`.
//!
//! The cockpit's "AI Confidence Scores" panel (R20.3) needs both halves
//! to render a single ranked-signal row, so the gateway maintains a
//! per-`correlation_id` join buffer with a bounded TTL and forwards a
//! merged JSON payload only when both halves are present (or on a
//! `signal-only` flush after the TTL elapses, to avoid hiding signals
//! when the ranking pipeline is degraded).
//!
//! ### AI_Shadow_Mode filtering (R23.2)
//!
//! The `ai.rank.*` payload carries a `source` field naming the ranking
//! component. When that component is currently shadowed (per the
//! gateway's [`AiShadowFilter`] surface, populated from
//! `config.ai.shadow_components` and any runtime `ai.gov.action` events
//! placing components into `shadow_mode`), the rank half is *dropped*
//! before joining. The signal half is still forwarded, with a
//! `shadowed_sources` annotation so the UI can render an "AI ranking
//! suppressed" indicator. This satisfies the design's invariant that
//! shadowed AI outputs MUST NOT influence the displayed ranking
//! (R23.2, R24.3).

use std::time::Duration;

use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Rank-source filter implementing AI_Shadow_Mode filtering (R23.2).
///
/// The set of currently-shadowed components is cheap to read (parking_lot
/// `RwLock<HashSet<String>>`). The gateway wires updates from two sources:
///
/// * **Static** — `config.ai.shadow_components` from `HedgeConfig` at
///   process start.
/// * **Dynamic** — `ai.gov.action` events on NATS with `action ==
///   "shadow_mode"` (a future task; the surface here accepts the toggles
///   today).
#[derive(Debug, Default)]
pub struct AiShadowFilter {
    components: RwLock<HashSet<String>>,
}

impl AiShadowFilter {
    /// Construct from an iterator of component names.
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<I, S>(iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: HashSet<String> = iter.into_iter().map(Into::into).collect();
        Self { components: RwLock::new(set) }
    }

    /// `true` when the named component is currently shadowed.
    pub fn is_shadowed(&self, component: &str) -> bool {
        self.components.read().contains(component)
    }

    /// Replace the shadowed-component set. Used when the gateway reloads
    /// configuration on SIGHUP (non-Hot_Path processes are reloadable
    /// per R32.5).
    pub fn replace<I, S>(&self, iter: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut g = self.components.write();
        g.clear();
        g.extend(iter.into_iter().map(Into::into));
    }

    /// Mark a component as shadowed (idempotent).
    pub fn shadow(&self, component: impl Into<String>) {
        self.components.write().insert(component.into());
    }

    /// Unshadow a component (idempotent).
    pub fn unshadow(&self, component: &str) {
        self.components.write().remove(component);
    }

    /// Snapshot the current shadowed-component set (for tests/observability).
    pub fn snapshot(&self) -> Vec<String> {
        let mut v: Vec<String> = self.components.read().iter().cloned().collect();
        v.sort();
        v
    }
}

/// Outcome of feeding one half of the join into [`SignalsJoiner`].
#[derive(Debug, Clone, PartialEq)]
pub enum JoinOutcome {
    /// Both halves are now present — forward the merged payload to
    /// `/signals` subscribers.
    Joined(Value),
    /// Only one half has arrived; buffered and waiting for the other.
    /// The boolean is `true` when *all* matched ranking sources were
    /// dropped because they are shadowed.
    Pending {
        /// `true` when every rank seen so far was filtered out by
        /// `AI_Shadow_Mode`.
        all_ranks_shadowed: bool,
    },
    /// The signal half arrived after its TTL had expired without a
    /// matching rank — emit the signal alone (with a `shadowed_sources`
    /// annotation if relevant) so the cockpit still sees it.
    SignalOnly(Value),
}

/// Per-`correlation_id` join state.
#[derive(Debug, Clone)]
struct PendingJoin {
    /// Signal half. `None` until `sig.emitted` arrives.
    signal: Option<Value>,
    /// Rank halves. Multiple AI components may rank the same signal;
    /// non-shadowed ones are kept and merged.
    ranks: Vec<Value>,
    /// AI components dropped because they are shadowed. The merged
    /// payload carries this list as `shadowed_sources` so the UI can
    /// render a hint.
    shadowed_sources: Vec<String>,
    /// Wall-clock instant when this join was created. Used to drive the
    /// signal-only flush.
    inserted_at: std::time::Instant,
}

/// Bounded join buffer for `sig.emitted` × `ai.rank.*`.
///
/// `correlation_id_hex` is the join key. The buffer is bounded in two
/// dimensions:
///
/// * **TTL** — entries older than `ttl` are pruned on each `feed_*` call.
///   When pruning, a signal-only entry surfaces as
///   [`JoinOutcome::SignalOnly`] so the cockpit still sees the signal.
/// * **Capacity** — once the join map reaches `capacity` entries, the
///   oldest entry is evicted regardless of TTL. This is a backstop
///   against a runaway producer.
pub struct SignalsJoiner {
    inner: parking_lot::Mutex<JoinerInner>,
    ttl: Duration,
    capacity: usize,
    shadow: Arc<AiShadowFilter>,
}

struct JoinerInner {
    map: HashMap<String, PendingJoin>,
    /// Insertion order for FIFO eviction; one entry per `map` key.
    order: std::collections::VecDeque<String>,
}

impl SignalsJoiner {
    /// Construct a joiner with `ttl` and `capacity` bounds and a shared
    /// shadow filter.
    pub fn new(ttl: Duration, capacity: usize, shadow: Arc<AiShadowFilter>) -> Self {
        Self {
            inner: parking_lot::Mutex::new(JoinerInner {
                map: HashMap::new(),
                order: std::collections::VecDeque::new(),
            }),
            ttl,
            capacity,
            shadow,
        }
    }

    /// Feed a `sig.emitted` payload.
    ///
    /// `correlation_id_hex` MUST match the canonical `ai.rank.<hex>`
    /// subject suffix used by the Warm_AI_Pipeline.
    pub fn feed_signal(&self, correlation_id_hex: &str, signal: Value) -> JoinOutcome {
        let mut inner = self.inner.lock();
        self.prune_expired(&mut inner);

        if !inner.map.contains_key(correlation_id_hex) {
            inner.map.insert(
                correlation_id_hex.to_owned(),
                PendingJoin {
                    signal: None,
                    ranks: Vec::new(),
                    shadowed_sources: Vec::new(),
                    inserted_at: std::time::Instant::now(),
                },
            );
            inner.order.push_back(correlation_id_hex.to_owned());
        }
        let entry = inner.map.get_mut(correlation_id_hex).expect("just inserted");
        entry.signal = Some(signal);

        let outcome = decide(entry);
        // If joined, remove the entry; pending stays.
        if matches!(outcome, JoinOutcome::Joined(_) | JoinOutcome::SignalOnly(_)) {
            inner.map.remove(correlation_id_hex);
            order_remove(&mut inner.order, correlation_id_hex);
        }
        self.enforce_capacity(&mut inner);
        outcome
    }

    /// Feed an `ai.rank.*` payload.
    ///
    /// `correlation_id_hex` is the suffix of the `ai.rank.<hex>` subject.
    /// The payload's `source` field is consulted against the shadow
    /// filter; shadowed sources are dropped (R23.2).
    pub fn feed_rank(&self, correlation_id_hex: &str, rank: Value) -> JoinOutcome {
        let source = rank.get("source").and_then(Value::as_str).unwrap_or("").to_owned();
        let is_shadowed = !source.is_empty() && self.shadow.is_shadowed(&source);

        let mut inner = self.inner.lock();
        self.prune_expired(&mut inner);

        if !inner.map.contains_key(correlation_id_hex) {
            inner.map.insert(
                correlation_id_hex.to_owned(),
                PendingJoin {
                    signal: None,
                    ranks: Vec::new(),
                    shadowed_sources: Vec::new(),
                    inserted_at: std::time::Instant::now(),
                },
            );
            inner.order.push_back(correlation_id_hex.to_owned());
        }
        let entry = inner.map.get_mut(correlation_id_hex).expect("just inserted");

        if is_shadowed {
            entry.shadowed_sources.push(source);
        } else {
            entry.ranks.push(rank);
        }

        let outcome = decide(entry);
        if matches!(outcome, JoinOutcome::Joined(_) | JoinOutcome::SignalOnly(_)) {
            inner.map.remove(correlation_id_hex);
            order_remove(&mut inner.order, correlation_id_hex);
        }
        self.enforce_capacity(&mut inner);
        outcome
    }

    /// Force-flush every entry whose TTL has elapsed. Returns the
    /// signal-only payloads that should be forwarded to subscribers.
    /// Called by the gateway on a periodic tick.
    pub fn flush_expired(&self) -> Vec<Value> {
        let mut inner = self.inner.lock();
        let mut out = Vec::new();
        let now = std::time::Instant::now();
        let ttl = self.ttl;
        let keys: Vec<String> = inner
            .map
            .iter()
            .filter(|(_, v)| now.saturating_duration_since(v.inserted_at) > ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            if let Some(entry) = inner.map.remove(&k) {
                order_remove(&mut inner.order, &k);
                if let Some(sig) = entry.signal {
                    out.push(merge_payload(&k, sig, entry.ranks, entry.shadowed_sources));
                }
            }
        }
        out
    }

    /// Inspect the current join-buffer size. Used by tests and metrics.
    pub fn pending_len(&self) -> usize {
        self.inner.lock().map.len()
    }

    fn prune_expired(&self, inner: &mut JoinerInner) {
        let now = std::time::Instant::now();
        let ttl = self.ttl;
        // Evict entries older than `ttl` that have *neither* a signal nor
        // an unshadowed rank — they cannot ever produce a useful joined
        // payload and would dangle forever. Entries with a signal-only or
        // rank-only state are left alone; the gateway's periodic
        // `flush_expired` call surfaces signal-only payloads to the UI
        // and drops orphaned ranks.
        let stale: Vec<String> = inner
            .map
            .iter()
            .filter(|(_, v)| {
                now.saturating_duration_since(v.inserted_at) > ttl
                    && v.signal.is_none()
                    && v.ranks.is_empty()
            })
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale {
            inner.map.remove(&k);
            order_remove(&mut inner.order, &k);
        }
    }

    fn enforce_capacity(&self, inner: &mut JoinerInner) {
        while inner.map.len() > self.capacity {
            if let Some(oldest) = inner.order.pop_front() {
                inner.map.remove(&oldest);
            } else {
                break;
            }
        }
    }
}

fn order_remove(order: &mut std::collections::VecDeque<String>, k: &str) {
    if let Some(pos) = order.iter().position(|x| x == k) {
        order.remove(pos);
    }
}

fn decide(entry: &PendingJoin) -> JoinOutcome {
    let has_signal = entry.signal.is_some();
    let has_rank = !entry.ranks.is_empty();
    let all_shadowed = !entry.shadowed_sources.is_empty() && entry.ranks.is_empty();

    if has_signal && has_rank {
        let signal = entry.signal.clone().unwrap();
        JoinOutcome::Joined(merge_payload(
            "",
            signal,
            entry.ranks.clone(),
            entry.shadowed_sources.clone(),
        ))
    } else {
        JoinOutcome::Pending { all_ranks_shadowed: all_shadowed }
    }
}

fn merge_payload(
    correlation_id_hex: &str,
    signal: Value,
    ranks: Vec<Value>,
    shadowed_sources: Vec<String>,
) -> Value {
    let mut out = json!({
        "signal": signal,
        "ranks": ranks,
    });
    if !correlation_id_hex.is_empty() {
        out["correlation_id"] = Value::String(correlation_id_hex.to_owned());
    }
    if !shadowed_sources.is_empty() {
        out["shadowed_sources"] = Value::Array(
            shadowed_sources
                .into_iter()
                .map(Value::String)
                .collect(),
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shadow_with(components: &[&str]) -> Arc<AiShadowFilter> {
        Arc::new(AiShadowFilter::from_iter(components.iter().copied()))
    }

    #[test]
    fn shadow_filter_lookup_round_trips() {
        let f = AiShadowFilter::from_iter(["news", "psych"]);
        assert!(f.is_shadowed("news"));
        assert!(f.is_shadowed("psych"));
        assert!(!f.is_shadowed("ranking"));
        assert_eq!(f.snapshot(), vec!["news".to_owned(), "psych".to_owned()]);
    }

    #[test]
    fn shadow_filter_replace_clears_old_set() {
        let f = AiShadowFilter::from_iter(["a"]);
        f.replace(["b", "c"]);
        assert!(!f.is_shadowed("a"));
        assert!(f.is_shadowed("b"));
        assert!(f.is_shadowed("c"));
    }

    #[test]
    fn signal_then_rank_yields_joined_payload() {
        let j = SignalsJoiner::new(Duration::from_secs(2), 1024, shadow_with(&[]));
        let cid = "01HJABC";
        let sig = json!({"strategy": "vwap_pullback", "confidence": 0.7});
        let rank = json!({"source": "ranking", "score": 0.82});

        let outcome = j.feed_signal(cid, sig.clone());
        assert!(matches!(outcome, JoinOutcome::Pending { all_ranks_shadowed: false }));

        let outcome = j.feed_rank(cid, rank.clone());
        match outcome {
            JoinOutcome::Joined(payload) => {
                assert_eq!(payload["signal"], sig);
                assert_eq!(payload["ranks"][0], rank);
            }
            other => panic!("expected Joined, got {:?}", other),
        }
        assert_eq!(j.pending_len(), 0);
    }

    #[test]
    fn rank_then_signal_yields_joined_payload() {
        let j = SignalsJoiner::new(Duration::from_secs(2), 1024, shadow_with(&[]));
        let cid = "01HJABC";
        let sig = json!({"strategy": "vwap_pullback"});
        let rank = json!({"source": "ranking", "score": 0.82});

        let outcome = j.feed_rank(cid, rank.clone());
        assert!(matches!(outcome, JoinOutcome::Pending { all_ranks_shadowed: false }));

        let outcome = j.feed_signal(cid, sig.clone());
        match outcome {
            JoinOutcome::Joined(payload) => {
                assert_eq!(payload["signal"], sig);
                assert_eq!(payload["ranks"][0], rank);
            }
            other => panic!("expected Joined, got {:?}", other),
        }
    }

    #[test]
    fn shadowed_rank_is_dropped_and_signal_only_emitted_after_ttl() {
        // ranking is shadowed → the rank half is dropped, only the
        // signal flushes after TTL with shadowed_sources annotation.
        let j = SignalsJoiner::new(
            Duration::from_millis(10),
            1024,
            shadow_with(&["ranking"]),
        );
        let cid = "01HJSHADOW";
        let sig = json!({"strategy": "vwap_pullback"});
        let rank = json!({"source": "ranking", "score": 0.99});

        let _ = j.feed_rank(cid, rank);
        let outcome = j.feed_signal(cid, sig.clone());
        // Nothing to join — signal stays pending.
        assert!(matches!(
            outcome,
            JoinOutcome::Pending { all_ranks_shadowed: true }
        ));

        std::thread::sleep(Duration::from_millis(20));
        let flushed = j.flush_expired();
        assert_eq!(flushed.len(), 1);
        let p = &flushed[0];
        assert_eq!(p["signal"], sig);
        assert_eq!(p["ranks"], json!([]));
        assert_eq!(p["shadowed_sources"], json!(["ranking"]));
    }

    #[test]
    fn signal_alone_flushes_after_ttl_with_no_ranks() {
        let j = SignalsJoiner::new(Duration::from_millis(10), 1024, shadow_with(&[]));
        let cid = "01HJSOLO";
        let sig = json!({"strategy": "obr"});
        let _ = j.feed_signal(cid, sig.clone());

        std::thread::sleep(Duration::from_millis(20));
        let flushed = j.flush_expired();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0]["signal"], sig);
        assert_eq!(flushed[0]["ranks"], json!([]));
    }

    #[test]
    fn capacity_evicts_oldest_pending() {
        let j = SignalsJoiner::new(Duration::from_secs(60), 2, shadow_with(&[]));
        let _ = j.feed_signal("a", json!({}));
        let _ = j.feed_signal("b", json!({}));
        let _ = j.feed_signal("c", json!({}));
        // a should have been evicted when c arrived.
        assert_eq!(j.pending_len(), 2);
    }

    #[test]
    fn non_shadowed_rank_and_shadowed_rank_keep_only_non_shadowed() {
        let j = SignalsJoiner::new(Duration::from_secs(2), 1024, shadow_with(&["news"]));
        let cid = "01HJMIX";
        let _ = j.feed_rank(cid, json!({"source": "ranking", "score": 0.5}));
        let _ = j.feed_rank(cid, json!({"source": "news", "score": 0.9}));
        let outcome = j.feed_signal(cid, json!({"strategy": "obr"}));
        match outcome {
            JoinOutcome::Joined(payload) => {
                assert_eq!(payload["ranks"].as_array().unwrap().len(), 1);
                assert_eq!(payload["ranks"][0]["source"], "ranking");
                assert_eq!(payload["shadowed_sources"], json!(["news"]));
            }
            other => panic!("expected Joined, got {:?}", other),
        }
    }
}
