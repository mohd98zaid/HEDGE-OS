//! Per-connection subscription tracker.
//!
//! Each WebSocket connection maintains a small subscription set: the
//! curated UI channels it has joined plus, optionally, a per-channel
//! topic filter (e.g. symbol IDs the client is currently watching on
//! `/market`). The tracker is intentionally simple — connection counts
//! are bounded by the operator deploying the cockpit (a single trader
//! per Mumbai VPS in design § Architecture § Deployment Topology) so a
//! `HashMap` is plenty.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

use crate::protocol::Channel;

/// Per-connection subscription set.
#[derive(Default)]
pub struct Subscriptions {
    inner: Mutex<HashMap<Channel, HashSet<String>>>,
}

impl Subscriptions {
    /// Construct an empty subscription set.
    pub fn new() -> Self {
        Self { inner: Mutex::new(HashMap::new()) }
    }

    /// Subscribe to `channel`, optionally with a topic filter.
    ///
    /// An empty `topics` slice means "all topics on this channel". If the
    /// client subsequently calls `subscribe` again with topics on the
    /// same channel, the topic set is **replaced**, not unioned, so the
    /// cockpit can update its symbol selection cleanly.
    pub fn subscribe(&self, channel: Channel, topics: &[String]) {
        let mut g = self.inner.lock();
        let set: HashSet<String> = topics.iter().cloned().collect();
        g.insert(channel, set);
    }

    /// Unsubscribe from `channel`.
    pub fn unsubscribe(&self, channel: Channel) {
        self.inner.lock().remove(&channel);
    }

    /// `true` when the connection is subscribed to `channel`.
    pub fn is_subscribed(&self, channel: Channel) -> bool {
        self.inner.lock().contains_key(&channel)
    }

    /// `true` when the connection wants to receive an event on `channel`
    /// whose topic suffix is `topic`.
    ///
    /// If the client subscribed without a topic filter (empty set), every
    /// topic on that channel is accepted. Otherwise only events whose
    /// topic exactly matches one of the configured filters pass through.
    pub fn accepts(&self, channel: Channel, topic: &str) -> bool {
        let g = self.inner.lock();
        match g.get(&channel) {
            None => false,
            Some(set) if set.is_empty() => true,
            Some(set) => set.contains(topic),
        }
    }

    /// Snapshot the current subscription set for tests.
    pub fn snapshot(&self) -> Vec<(Channel, Vec<String>)> {
        let g = self.inner.lock();
        let mut out: Vec<_> = g
            .iter()
            .map(|(c, s)| {
                let mut v: Vec<String> = s.iter().cloned().collect();
                v.sort();
                (*c, v)
            })
            .collect();
        out.sort_by_key(|(c, _)| *c as u8);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_then_check() {
        let s = Subscriptions::new();
        s.subscribe(Channel::Market, &["RELIANCE".into()]);
        assert!(s.is_subscribed(Channel::Market));
        assert!(!s.is_subscribed(Channel::Risk));
    }

    #[test]
    fn unsubscribe_clears_channel() {
        let s = Subscriptions::new();
        s.subscribe(Channel::Risk, &[]);
        assert!(s.is_subscribed(Channel::Risk));
        s.unsubscribe(Channel::Risk);
        assert!(!s.is_subscribed(Channel::Risk));
    }

    #[test]
    fn accepts_all_topics_when_filter_empty() {
        let s = Subscriptions::new();
        s.subscribe(Channel::Market, &[]);
        assert!(s.accepts(Channel::Market, "anything"));
        assert!(s.accepts(Channel::Market, "another"));
    }

    #[test]
    fn accepts_only_matching_topics_when_filter_set() {
        let s = Subscriptions::new();
        s.subscribe(Channel::Market, &["RELIANCE".into(), "NIFTY".into()]);
        assert!(s.accepts(Channel::Market, "RELIANCE"));
        assert!(s.accepts(Channel::Market, "NIFTY"));
        assert!(!s.accepts(Channel::Market, "TCS"));
    }

    #[test]
    fn accepts_returns_false_for_unsubscribed_channels() {
        let s = Subscriptions::new();
        assert!(!s.accepts(Channel::Risk, "anything"));
    }

    #[test]
    fn resubscribe_replaces_topic_filter() {
        let s = Subscriptions::new();
        s.subscribe(Channel::Market, &["A".into()]);
        s.subscribe(Channel::Market, &["B".into()]);
        assert!(!s.accepts(Channel::Market, "A"));
        assert!(s.accepts(Channel::Market, "B"));
    }
}
