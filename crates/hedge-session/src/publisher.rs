//! NATS-backed [`OpsEventPublisher`] for the `ops.warmode.<phase>`
//! subjects and [`SessionEventPublisher`] for the `ops.session.<phase>`
//! subjects.
//!
//! Both publishers wire their typed event onto the well-known subjects
//! from `hedge-bus` using a [`JsonCodec`]. The codec choice matches the
//! design's "JSON for `ops.*`" rule (design § Data Models § Wire Codec).
//!
//! The two controllers use these publishers exclusively in production;
//! tests substitute in-memory implementations (see the unit tests in
//! `controller.rs` and `session_controller.rs`).

use async_trait::async_trait;
use hedge_bus::{
    BusError, JsonCodec, NatsClient, NatsPublisher, Subject, OPS_SESSION_END, OPS_SESSION_START,
    OPS_WARMODE_END, OPS_WARMODE_START,
};

use crate::controller::OpsEventPublisher;
use crate::event::{WarModeEvent, WarModePhase};
use crate::session_controller::SessionEventPublisher;
use crate::session_event::{SessionEvent, SessionPhase};

/// Production publisher that emits `ops.warmode.start` and
/// `ops.warmode.end` over NATS.
///
/// Holds two long-lived [`NatsPublisher`] instances — one per subject —
/// constructed once at startup. `async_nats::Client` is internally
/// refcounted, so per-event publish is a cheap call without re-resolving
/// the codec or re-cloning the client.
pub struct NatsOpsEventPublisher {
    start_pub: NatsPublisher<WarModeEvent, JsonCodec<WarModeEvent>>,
    end_pub: NatsPublisher<WarModeEvent, JsonCodec<WarModeEvent>>,
}

impl NatsOpsEventPublisher {
    /// Construct a publisher from a connected [`NatsClient`].
    ///
    /// Uses the workspace's well-known constants
    /// [`OPS_WARMODE_START`] and [`OPS_WARMODE_END`] from `hedge-bus`
    /// so the subject names cannot drift from the design's canonical
    /// "NATS Subject Naming Convention" table.
    pub fn new(client: &NatsClient) -> Self {
        let start_subject: Subject<WarModeEvent> = Subject::new(OPS_WARMODE_START);
        let end_subject: Subject<WarModeEvent> = Subject::new(OPS_WARMODE_END);
        Self {
            start_pub: client.publisher(start_subject, JsonCodec::<WarModeEvent>::new()),
            end_pub: client.publisher(end_subject, JsonCodec::<WarModeEvent>::new()),
        }
    }
}

#[async_trait]
impl OpsEventPublisher for NatsOpsEventPublisher {
    async fn publish_warmode(&self, event: &WarModeEvent) -> Result<(), BusError> {
        match event.phase {
            WarModePhase::Start => self.start_pub.publish(event).await,
            WarModePhase::End => self.end_pub.publish(event).await,
        }
    }
}

/// Production publisher that emits `ops.session.start` and
/// `ops.session.end` over NATS.
///
/// Mirrors [`NatsOpsEventPublisher`] but is bound to the
/// Trading_Session subjects and [`SessionEvent`] payload. Both
/// publishers can share a single [`NatsClient`] connection — the client
/// is internally refcounted by `async_nats` and supports concurrent
/// publishers.
pub struct NatsSessionEventPublisher {
    start_pub: NatsPublisher<SessionEvent, JsonCodec<SessionEvent>>,
    end_pub: NatsPublisher<SessionEvent, JsonCodec<SessionEvent>>,
}

impl NatsSessionEventPublisher {
    /// Construct a publisher from a connected [`NatsClient`].
    ///
    /// Uses the workspace's well-known constants
    /// [`OPS_SESSION_START`] and [`OPS_SESSION_END`] from `hedge-bus`
    /// so the subject names cannot drift from the design's canonical
    /// "NATS Subject Naming Convention" table.
    pub fn new(client: &NatsClient) -> Self {
        let start_subject: Subject<SessionEvent> = Subject::new(OPS_SESSION_START);
        let end_subject: Subject<SessionEvent> = Subject::new(OPS_SESSION_END);
        Self {
            start_pub: client.publisher(start_subject, JsonCodec::<SessionEvent>::new()),
            end_pub: client.publisher(end_subject, JsonCodec::<SessionEvent>::new()),
        }
    }
}

#[async_trait]
impl SessionEventPublisher for NatsSessionEventPublisher {
    async fn publish_session(&self, event: &SessionEvent) -> Result<(), BusError> {
        match event.phase {
            SessionPhase::Start => self.start_pub.publish(event).await,
            SessionPhase::End => self.end_pub.publish(event).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The publisher types name exactly the four `hedge-bus` subjects
    /// the design declares. This guards against accidental subject
    /// drift — the canonical constants live in `hedge-bus::subject`
    /// and the publishers must wire to them, not to fresh string
    /// literals.
    #[test]
    fn well_known_subjects_match_design() {
        assert_eq!(OPS_WARMODE_START, "ops.warmode.start");
        assert_eq!(OPS_WARMODE_END, "ops.warmode.end");
        assert_eq!(OPS_SESSION_START, "ops.session.start");
        assert_eq!(OPS_SESSION_END, "ops.session.end");
    }
}
