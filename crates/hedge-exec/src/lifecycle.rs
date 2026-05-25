//! `OrderLifecycleTracker` — the per-order FSM (R6.3, Property 9).
//!
//! ## State diagram
//!
//! ```text
//!                ┌────────────┐
//!                │    New     │
//!                └──────┬─────┘
//!                       │ submit
//!                       ▼
//!                ┌────────────┐
//!                │ Submitted  │
//!                └─┬───┬──┬───┘
//!                  │   │  │
//!     partial_fill │   │  │ reject
//!                  ▼   │  └──────┐
//!         ┌────────────┴──┐      │
//!         │ PartiallyFilled│     │
//!         └──┬─────────────┘     │
//!            │ fill (cumulative) │
//!            ▼                   ▼
//!         ┌────────────┐   ┌──────────┐
//!         │   Filled   │   │ Rejected │
//!         └────────────┘   └──────────┘
//!                       (Cancelled is reachable from Submitted
//!                        and PartiallyFilled.)
//! ```
//!
//! Terminal states: `Filled`, `Cancelled`, `Rejected`.
//!
//! ## Discipline
//!
//! * Every transition is validated against the legal edge set
//!   ([`is_legal_transition`]). An invalid attempt produces
//!   [`ExecError::InvalidFsmTransition`] and the FSM does not change
//!   state.
//! * [`OrderLifecycleTracker::transition`] is the single mutator. Each
//!   successful transition produces exactly one [`LifecycleEvent`] for
//!   the network layer to publish on
//!   `exec.order.<state>` (R6.3 + Property 9).
//! * The tracker enforces the cumulative-fill invariant:
//!   `filled_qty` is monotonically non-decreasing and never exceeds the
//!   originally requested quantity.
//! * Once a terminal state is reached, no further transitions are
//!   permitted.

use hedge_core::{BrokerId, CorrelationId};
use hedge_schemas::order_state::OrderLifecycleState;

use crate::error::ExecError;

/// One published-event the tracker asks the network layer to fan out
/// after a successful transition. Subject is `exec.order.<state>`
/// (e.g. `exec.order.Submitted`, `exec.order.Filled`). The state name
/// is the [`OrderLifecycleState::as_str`] canonical form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvent {
    /// Order correlation id.
    pub correlation_id: CorrelationId,
    /// Broker the order was routed to.
    pub broker: BrokerId,
    /// New state — the trailing segment of the `exec.order.<state>` subject.
    pub state: OrderLifecycleState,
    /// Cumulative filled quantity at the moment of this event.
    pub filled_qty: u64,
    /// Volume-weighted average fill price in paise.
    pub avg_fill_paise: i64,
    /// Optional broker-side order id (populated once `Submitted` succeeds).
    pub broker_order_id: Option<String>,
    /// Wall-clock timestamp in nanoseconds.
    pub ts_ns: u64,
}

/// FSM-tracked order. Owned and mutated by the engine; one tracker per
/// in-flight order. `Clone` is exposed so tests and the engine can take
/// snapshots cheaply.
#[derive(Debug, Clone)]
pub struct OrderLifecycleTracker {
    correlation_id: CorrelationId,
    broker: BrokerId,
    state: OrderLifecycleState,
    requested_qty: u64,
    filled_qty: u64,
    avg_fill_paise: i64,
    broker_order_id: Option<String>,
}

impl OrderLifecycleTracker {
    /// Construct a fresh tracker in the `New` state.
    pub fn new(correlation_id: CorrelationId, broker: BrokerId, requested_qty: u64) -> Self {
        Self {
            correlation_id,
            broker,
            state: OrderLifecycleState::New,
            requested_qty,
            filled_qty: 0,
            avg_fill_paise: 0,
            broker_order_id: None,
        }
    }

    /// Borrow the current state.
    #[inline]
    pub fn state(&self) -> OrderLifecycleState {
        self.state
    }

    /// Borrow the cumulative filled quantity.
    #[inline]
    pub fn filled_qty(&self) -> u64 {
        self.filled_qty
    }

    /// Borrow the volume-weighted average fill price.
    #[inline]
    pub fn avg_fill_paise(&self) -> i64 {
        self.avg_fill_paise
    }

    /// Borrow the originally requested quantity.
    #[inline]
    pub fn requested_qty(&self) -> u64 {
        self.requested_qty
    }

    /// Borrow the correlation id.
    #[inline]
    pub fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }

    /// Borrow the broker id.
    #[inline]
    pub fn broker(&self) -> BrokerId {
        self.broker
    }

    /// Borrow the broker-side order id, if assigned.
    #[inline]
    pub fn broker_order_id(&self) -> Option<&str> {
        self.broker_order_id.as_deref()
    }

    /// Returns `true` when the FSM has reached a terminal state.
    #[inline]
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Stamp the broker-side order id. The tracker keeps it as an
    /// optional value so callers can omit it for synthetic-fill flows.
    pub fn set_broker_order_id(&mut self, id: impl Into<String>) {
        self.broker_order_id = Some(id.into());
    }

    /// Move to `Submitted`. Only legal from `New`. Returns the lifecycle
    /// event the caller should publish on `exec.order.Submitted`.
    pub fn submit(
        &mut self,
        broker_order_id: Option<String>,
        ts_ns: u64,
    ) -> Result<LifecycleEvent, ExecError> {
        self.transition_no_fill(OrderLifecycleState::Submitted, ts_ns, broker_order_id)
    }

    /// Apply a partial fill. Cumulatively non-decreasing in `filled_qty`.
    /// Transitions to `PartiallyFilled` (or `Filled` once cumulative
    /// reaches the requested quantity).
    pub fn partial_fill(
        &mut self,
        cum_filled_qty: u64,
        cum_avg_fill_paise: i64,
        ts_ns: u64,
    ) -> Result<LifecycleEvent, ExecError> {
        // Reject regressions in cumulative quantity.
        if cum_filled_qty < self.filled_qty {
            return Err(ExecError::Internal(format!(
                "fill regression: {} -> {}",
                self.filled_qty, cum_filled_qty
            )));
        }
        if cum_filled_qty > self.requested_qty {
            return Err(ExecError::Internal(format!(
                "fill overflow: {} > requested {}",
                cum_filled_qty, self.requested_qty
            )));
        }

        let target = if cum_filled_qty >= self.requested_qty && self.requested_qty > 0 {
            OrderLifecycleState::Filled
        } else {
            OrderLifecycleState::PartiallyFilled
        };

        if !is_legal_transition(self.state, target) {
            return Err(ExecError::InvalidFsmTransition {
                from: self.state,
                to: target,
            });
        }

        self.filled_qty = cum_filled_qty;
        self.avg_fill_paise = cum_avg_fill_paise;
        self.state = target;
        Ok(self.event(ts_ns))
    }

    /// Move to `Cancelled`. Legal from `Submitted` or `PartiallyFilled`.
    pub fn cancel(&mut self, ts_ns: u64) -> Result<LifecycleEvent, ExecError> {
        self.transition_no_fill(OrderLifecycleState::Cancelled, ts_ns, None)
    }

    /// Move to `Rejected`. Legal only from `Submitted` (the broker
    /// rejected before any fill).
    pub fn reject(&mut self, ts_ns: u64) -> Result<LifecycleEvent, ExecError> {
        self.transition_no_fill(OrderLifecycleState::Rejected, ts_ns, None)
    }

    /// Generic transition entry point. Validates the edge against
    /// [`is_legal_transition`] and updates state. Returns the
    /// `LifecycleEvent` the caller publishes.
    pub fn transition(
        &mut self,
        target: OrderLifecycleState,
        ts_ns: u64,
    ) -> Result<LifecycleEvent, ExecError> {
        self.transition_no_fill(target, ts_ns, None)
    }

    fn transition_no_fill(
        &mut self,
        target: OrderLifecycleState,
        ts_ns: u64,
        broker_order_id: Option<String>,
    ) -> Result<LifecycleEvent, ExecError> {
        if !is_legal_transition(self.state, target) {
            return Err(ExecError::InvalidFsmTransition {
                from: self.state,
                to: target,
            });
        }
        if let Some(id) = broker_order_id {
            self.broker_order_id = Some(id);
        }
        self.state = target;
        Ok(self.event(ts_ns))
    }

    fn event(&self, ts_ns: u64) -> LifecycleEvent {
        LifecycleEvent {
            correlation_id: self.correlation_id,
            broker: self.broker,
            state: self.state,
            filled_qty: self.filled_qty,
            avg_fill_paise: self.avg_fill_paise,
            broker_order_id: self.broker_order_id.clone(),
            ts_ns,
        }
    }
}

/// Returns `true` when `from -> to` is a legal FSM edge.
///
/// The transition graph is fixed at compile time and corresponds to
/// the diagram at the top of this module. Invariant: terminal states
/// have no outgoing edges.
#[inline]
pub fn is_legal_transition(from: OrderLifecycleState, to: OrderLifecycleState) -> bool {
    use OrderLifecycleState::*;
    match (from, to) {
        // From New
        (New, Submitted) => true,
        // From Submitted
        (Submitted, PartiallyFilled) => true,
        (Submitted, Filled) => true,
        (Submitted, Cancelled) => true,
        (Submitted, Rejected) => true,
        // From PartiallyFilled
        (PartiallyFilled, PartiallyFilled) => true, // additional partials
        (PartiallyFilled, Filled) => true,
        (PartiallyFilled, Cancelled) => true,
        // Everything else is illegal (in particular: New is the only
        // valid starting point; terminal states have no outgoing edges).
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(n: u128) -> CorrelationId {
        CorrelationId(n)
    }

    /// New -> Submitted -> Filled is the canonical happy path.
    #[test]
    fn happy_path_full_fill() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Zerodha, 10);
        let e = t.submit(Some("BR-1".into()), 100).unwrap();
        assert_eq!(e.state, OrderLifecycleState::Submitted);
        assert_eq!(e.broker_order_id.as_deref(), Some("BR-1"));

        let e = t.partial_fill(10, 100_00, 200).unwrap();
        assert_eq!(e.state, OrderLifecycleState::Filled);
        assert_eq!(e.filled_qty, 10);
        assert_eq!(e.avg_fill_paise, 100_00);
        assert!(t.is_terminal());
    }

    /// New -> Submitted -> PartiallyFilled -> Filled walks the partial fill edge.
    #[test]
    fn partial_then_filled() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Zerodha, 10);
        t.submit(None, 100).unwrap();

        let e = t.partial_fill(4, 99_50, 200).unwrap();
        assert_eq!(e.state, OrderLifecycleState::PartiallyFilled);

        let e = t.partial_fill(10, 100_00, 300).unwrap();
        assert_eq!(e.state, OrderLifecycleState::Filled);
        assert!(t.is_terminal());
    }

    /// Submitted -> Rejected.
    #[test]
    fn submitted_to_rejected() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Dhan, 10);
        t.submit(None, 100).unwrap();
        let e = t.reject(200).unwrap();
        assert_eq!(e.state, OrderLifecycleState::Rejected);
        assert!(t.is_terminal());
    }

    /// Submitted -> Cancelled, and PartiallyFilled -> Cancelled.
    #[test]
    fn cancel_paths() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Dhan, 10);
        t.submit(None, 100).unwrap();
        t.cancel(200).unwrap();
        assert!(t.is_terminal());

        let mut t = OrderLifecycleTracker::new(cid(2), BrokerId::Dhan, 10);
        t.submit(None, 100).unwrap();
        t.partial_fill(3, 99_00, 150).unwrap();
        let e = t.cancel(200).unwrap();
        assert_eq!(e.state, OrderLifecycleState::Cancelled);
        assert_eq!(e.filled_qty, 3, "cancelled state preserves accumulated fills");
    }

    /// Illegal: New -> Filled directly is rejected.
    #[test]
    fn rejects_new_to_filled_directly() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Zerodha, 10);
        let err = t.partial_fill(10, 100_00, 100).unwrap_err();
        match err {
            ExecError::InvalidFsmTransition { from, to } => {
                assert_eq!(from, OrderLifecycleState::New);
                assert_eq!(to, OrderLifecycleState::Filled);
            }
            other => panic!("expected InvalidFsmTransition, got {:?}", other),
        }
    }

    /// Illegal: terminal states reject all further transitions.
    #[test]
    fn terminal_states_reject_all_transitions() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Zerodha, 10);
        t.submit(None, 100).unwrap();
        t.partial_fill(10, 100_00, 200).unwrap();
        assert!(t.is_terminal());
        assert!(t.cancel(300).is_err(), "Filled is terminal");
        assert!(t.reject(300).is_err(), "Filled is terminal");
        assert!(t.partial_fill(10, 100_00, 300).is_err());
    }

    /// Cumulative-fill regression is rejected.
    #[test]
    fn rejects_fill_regression() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Zerodha, 10);
        t.submit(None, 100).unwrap();
        t.partial_fill(7, 100_00, 200).unwrap();
        let err = t.partial_fill(5, 100_00, 300).unwrap_err();
        match err {
            ExecError::Internal(msg) => assert!(msg.contains("regression"), "{}", msg),
            other => panic!("expected Internal, got {:?}", other),
        }
    }

    /// Cumulative-fill overflow (> requested) is rejected.
    #[test]
    fn rejects_fill_overflow() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Zerodha, 10);
        t.submit(None, 100).unwrap();
        let err = t.partial_fill(11, 100_00, 200).unwrap_err();
        match err {
            ExecError::Internal(msg) => assert!(msg.contains("overflow"), "{}", msg),
            other => panic!("expected Internal, got {:?}", other),
        }
    }

    /// `is_legal_transition` agrees with the documented edge set.
    #[test]
    fn legal_transitions_match_design() {
        use OrderLifecycleState::*;
        // Legal:
        let legal = [
            (New, Submitted),
            (Submitted, PartiallyFilled),
            (Submitted, Filled),
            (Submitted, Cancelled),
            (Submitted, Rejected),
            (PartiallyFilled, PartiallyFilled),
            (PartiallyFilled, Filled),
            (PartiallyFilled, Cancelled),
        ];
        for (f, t) in legal {
            assert!(is_legal_transition(f, t), "{:?} -> {:?} should be legal", f, t);
        }
        // Illegal sample:
        let illegal = [
            (New, PartiallyFilled),
            (New, Filled),
            (New, Rejected),
            (New, Cancelled),
            (Filled, Submitted),
            (Cancelled, Submitted),
            (Rejected, Submitted),
            (PartiallyFilled, Rejected),
            (PartiallyFilled, New),
        ];
        for (f, t) in illegal {
            assert!(!is_legal_transition(f, t), "{:?} -> {:?} should be illegal", f, t);
        }
    }

    /// Property 9 implication: every transition produces a single
    /// LifecycleEvent whose `state` matches the new state.
    #[test]
    fn transition_event_matches_new_state() {
        let mut t = OrderLifecycleTracker::new(cid(1), BrokerId::Zerodha, 10);
        let e = t.submit(None, 100).unwrap();
        assert_eq!(e.state, t.state());
        let e = t.partial_fill(5, 99_00, 200).unwrap();
        assert_eq!(e.state, t.state());
        let e = t.partial_fill(10, 100_00, 300).unwrap();
        assert_eq!(e.state, t.state());
    }
}
