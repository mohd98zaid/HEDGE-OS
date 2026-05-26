//! NATS subject ↔ UI channel routing.
//!
//! For every UI channel exposed by the gateway, this module enumerates the
//! NATS subject *patterns* the gateway subscribes to (using NATS wildcards
//! `*` and `>`), and the inverse mapping that classifies a delivered NATS
//! subject back to the UI channel(s) that should receive it.
//!
//! The mapping mirrors the design's
//! [Data Models § WebSocket Channels (UI Gateway)](../../.kiro/specs/project-hedge/design.md)
//! table verbatim:
//!
//! | Channel    | Subjects                                                     |
//! |------------|--------------------------------------------------------------|
//! | `market`   | `md.tick.*`, `md.book.*`, `md.oi.*`, `md.connection.*`, `md.breadth.sector`, `md.breadth.volatility` |
//! | `orderflow`| `of.event.*`, `of.heatmap.*`                                  |
//! | `signals`  | `sig.emitted`, `ai.rank.*` (joined; see `signals_join.rs`)    |
//! | `risk`     | `risk.decision.approved`, `risk.decision.rejected`, `risk.killswitch.activated`, `risk.target.reached`, `risk.cooldown.*`, `pos.update.*`, `pos.risk_state` |
//! | `exec`     | `exec.order.*`, `exec.fill.*`, `exec.broker.failover`, `exec.trade.closed` |
//! | `news`     | `ai.news.impact.*`                                            |
//! | `psych`    | `ai.psych.stability`, `ai.psych.intervention`                 |
//! | `alerts`   | aggregated UI-formatted alerts (see `alerts.rs`)              |
//! | `replay`   | `ops.action.replay`, replay control plane (see `replay.rs`)   |
//! | `latency`  | `obs.latency.*`, `obs.budget.breach.*`                        |
//! | `control`  | client → server intents only (no NATS subscriptions)          |
//!
//! The `signals` channel is special: `ai.rank.*` outputs from shadowed AI
//! components are filtered out per `AI_Shadow_Mode` (R23.2). The actual
//! join + filter logic lives in [`crate::signals_join`].

use crate::protocol::Channel;

/// NATS subject patterns the gateway subscribes to per UI channel.
///
/// Returned patterns use NATS wildcards (`*` matches one segment, `>`
/// matches the remainder). The empty slice for `Channel::Control` and
/// `Channel::Alerts` is intentional — those are populated from internal
/// sources, not from a direct NATS subscription. (`Alerts` is fed by a
/// merged in-process stream; `Control` is client → server only.)
pub fn nats_patterns(channel: Channel) -> &'static [&'static str] {
    match channel {
        Channel::Market => &[
            "md.tick.>",
            "md.book.>",
            "md.oi.>",
            "md.connection.>",
            "md.breadth.sector",
            "md.breadth.volatility",
        ],
        Channel::Orderflow => &["of.event.>", "of.heatmap.>"],
        Channel::Signals => &["sig.emitted", "ai.rank.>"],
        Channel::Risk => &[
            "risk.decision.approved",
            "risk.decision.rejected",
            "risk.killswitch.activated",
            "risk.target.reached",
            "risk.cooldown.>",
            "pos.update.>",
            "pos.risk_state",
        ],
        Channel::Exec => &[
            "exec.order.>",
            "exec.fill.>",
            "exec.broker.failover",
            "exec.trade.closed",
        ],
        Channel::News => &["ai.news.impact.>"],
        Channel::Psych => &["ai.psych.stability", "ai.psych.intervention"],
        Channel::Alerts => &[],
        Channel::Replay => &["ops.action.replay"],
        Channel::Latency => &["obs.latency.>", "obs.budget.breach.>"],
        Channel::Control => &[],
    }
}

/// Classify a delivered NATS subject string into the UI channel(s) it
/// belongs to.
///
/// Returns up to two channels because `risk` and `pos.*` overlap (the
/// design groups `pos.update.*` and `pos.risk_state` under `risk`), and
/// `signals` consumes both `sig.emitted` and `ai.rank.*`.
pub fn classify_subject(subject: &str) -> ChannelMatch {
    if subject == "md.breadth.sector" || subject == "md.breadth.volatility" {
        return ChannelMatch::one(Channel::Market);
    }

    let head = match subject.split('.').next() {
        Some(h) => h,
        None => return ChannelMatch::none(),
    };

    match head {
        "md" => ChannelMatch::one(Channel::Market),
        "of" => ChannelMatch::one(Channel::Orderflow),
        "sig" => ChannelMatch::one(Channel::Signals),
        "ai" => {
            // `ai.rank.*`           → signals
            // `ai.news.impact.*`    → news
            // `ai.psych.*`          → psych
            // anything else under `ai.*` is not surfaced to the UI gateway
            // directly (e.g. `ai.gov.action` is consumed via /alerts).
            let mut parts = subject.split('.');
            let _ai = parts.next();
            match parts.next() {
                Some("rank") => ChannelMatch::one(Channel::Signals),
                Some("news") => {
                    if parts.next() == Some("impact") {
                        ChannelMatch::one(Channel::News)
                    } else {
                        ChannelMatch::none()
                    }
                }
                Some("psych") => ChannelMatch::one(Channel::Psych),
                _ => ChannelMatch::none(),
            }
        }
        "risk" => ChannelMatch::one(Channel::Risk),
        "pos" => ChannelMatch::one(Channel::Risk),
        "exec" => ChannelMatch::one(Channel::Exec),
        "obs" => {
            // Only `obs.latency.*` and `obs.budget.breach.*` flow to the
            // UI Latency Dashboard.
            let mut parts = subject.split('.');
            let _obs = parts.next();
            match parts.next() {
                Some("latency") => ChannelMatch::one(Channel::Latency),
                Some("budget") => {
                    if parts.next() == Some("breach") {
                        ChannelMatch::one(Channel::Latency)
                    } else {
                        ChannelMatch::none()
                    }
                }
                _ => ChannelMatch::none(),
            }
        }
        "ops" => {
            let mut parts = subject.split('.');
            let _ops = parts.next();
            match parts.next() {
                Some("action") => {
                    if parts.next() == Some("replay") {
                        ChannelMatch::one(Channel::Replay)
                    } else {
                        ChannelMatch::none()
                    }
                }
                _ => ChannelMatch::none(),
            }
        }
        _ => ChannelMatch::none(),
    }
}

/// Up-to-two-element classification result.
///
/// We avoid a `Vec` allocation per delivered NATS message because the
/// gateway sees one classification per published event and we want the
/// dispatch hot path to stay alloc-free.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ChannelMatch {
    inner: [Option<Channel>; 2],
}

impl ChannelMatch {
    /// Empty match (subject is not surfaced on any UI channel).
    pub const fn none() -> Self {
        Self { inner: [None, None] }
    }

    /// Match into a single channel.
    pub const fn one(c: Channel) -> Self {
        Self { inner: [Some(c), None] }
    }

    /// Match into two channels (e.g. a future event published on
    /// overlapping subjects).
    #[allow(dead_code)]
    pub const fn two(a: Channel, b: Channel) -> Self {
        Self { inner: [Some(a), Some(b)] }
    }

    /// Iterate over the matched channels.
    pub fn iter(&self) -> impl Iterator<Item = Channel> + '_ {
        self.inner.iter().filter_map(|c| *c)
    }

    /// `true` when no channel matches.
    pub fn is_empty(&self) -> bool {
        self.inner[0].is_none()
    }

    /// `true` when the given channel matches.
    pub fn contains(&self, c: Channel) -> bool {
        self.inner.iter().any(|x| *x == Some(c))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_channel_has_a_pattern_set_or_is_internal() {
        for ch in Channel::ALL {
            let patterns = nats_patterns(ch);
            match ch {
                Channel::Alerts | Channel::Control => {
                    assert!(patterns.is_empty(), "{:?} must not subscribe directly", ch);
                }
                _ => {
                    assert!(!patterns.is_empty(), "{:?} must subscribe to at least one subject", ch);
                }
            }
        }
    }

    #[test]
    fn classify_md_subjects() {
        assert_eq!(classify_subject("md.tick.42"), ChannelMatch::one(Channel::Market));
        assert_eq!(classify_subject("md.book.7"), ChannelMatch::one(Channel::Market));
        assert_eq!(classify_subject("md.oi.11"), ChannelMatch::one(Channel::Market));
        assert_eq!(classify_subject("md.connection.nse_l1"), ChannelMatch::one(Channel::Market));
        assert_eq!(classify_subject("md.breadth.sector"), ChannelMatch::one(Channel::Market));
        assert_eq!(classify_subject("md.breadth.volatility"), ChannelMatch::one(Channel::Market));
    }

    #[test]
    fn classify_of_subjects() {
        assert_eq!(classify_subject("of.event.42"), ChannelMatch::one(Channel::Orderflow));
        assert_eq!(classify_subject("of.heatmap.42"), ChannelMatch::one(Channel::Orderflow));
    }

    #[test]
    fn classify_signals_subjects() {
        assert_eq!(classify_subject("sig.emitted"), ChannelMatch::one(Channel::Signals));
        assert_eq!(classify_subject("ai.rank.abc123"), ChannelMatch::one(Channel::Signals));
    }

    #[test]
    fn classify_risk_pos_subjects() {
        assert_eq!(classify_subject("risk.decision.approved"), ChannelMatch::one(Channel::Risk));
        assert_eq!(classify_subject("risk.decision.rejected"), ChannelMatch::one(Channel::Risk));
        assert_eq!(classify_subject("risk.killswitch.activated"), ChannelMatch::one(Channel::Risk));
        assert_eq!(classify_subject("risk.target.reached"), ChannelMatch::one(Channel::Risk));
        assert_eq!(classify_subject("risk.cooldown.42"), ChannelMatch::one(Channel::Risk));
        assert_eq!(classify_subject("pos.update.42"), ChannelMatch::one(Channel::Risk));
        assert_eq!(classify_subject("pos.risk_state"), ChannelMatch::one(Channel::Risk));
    }

    #[test]
    fn classify_exec_subjects() {
        assert_eq!(classify_subject("exec.order.submitted"), ChannelMatch::one(Channel::Exec));
        assert_eq!(classify_subject("exec.order.filled"), ChannelMatch::one(Channel::Exec));
        assert_eq!(classify_subject("exec.fill.42"), ChannelMatch::one(Channel::Exec));
        assert_eq!(classify_subject("exec.broker.failover"), ChannelMatch::one(Channel::Exec));
        assert_eq!(classify_subject("exec.trade.closed"), ChannelMatch::one(Channel::Exec));
    }

    #[test]
    fn classify_news_psych_subjects() {
        assert_eq!(classify_subject("ai.news.impact.42"), ChannelMatch::one(Channel::News));
        assert_eq!(classify_subject("ai.psych.stability"), ChannelMatch::one(Channel::Psych));
        assert_eq!(classify_subject("ai.psych.intervention"), ChannelMatch::one(Channel::Psych));
    }

    #[test]
    fn classify_obs_subjects_route_to_latency() {
        assert_eq!(classify_subject("obs.latency.tick_ingest"), ChannelMatch::one(Channel::Latency));
        assert_eq!(classify_subject("obs.budget.breach.risk_check"), ChannelMatch::one(Channel::Latency));
        assert_eq!(classify_subject("obs.error.market_data"), ChannelMatch::none());
    }

    #[test]
    fn classify_unknown_subjects_yields_none() {
        assert_eq!(classify_subject("ufo.tick.1"), ChannelMatch::none());
        assert_eq!(classify_subject(""), ChannelMatch::none());
        assert_eq!(classify_subject("ai.gov.action"), ChannelMatch::none());
    }

    #[test]
    fn channel_match_iter_yields_only_present_channels() {
        let m = ChannelMatch::one(Channel::Market);
        let xs: Vec<_> = m.iter().collect();
        assert_eq!(xs, vec![Channel::Market]);

        let m = ChannelMatch::none();
        let xs: Vec<_> = m.iter().collect();
        assert!(xs.is_empty());
    }
}
