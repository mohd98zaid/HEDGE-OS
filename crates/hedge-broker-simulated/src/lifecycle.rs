//! Per-order lifecycle for the simulated broker.
//!
//! Tracks one in-memory record per simulated order. The record stores
//! the original intent, the running fill total, the volume-weighted
//! average fill price, and the current FSM state. Lifecycle transitions
//! are deterministic given the input sequence (R22.4, Property 12).

use hedge_broker_api::{BrokerError, OrderIntent, OrderModification, OrderStatus};
use hedge_core::{CorrelationId, Qty};
use hedge_schemas::order_state::OrderLifecycleState;

use crate::orderbook::BookLevel;

/// One simulated order's full lifecycle record.
#[derive(Clone, Debug)]
pub struct OrderRecord {
    /// Broker-assigned id (deterministic; `"SIM-<n>"`).
    pub broker_order_id: String,
    /// Correlation id of the originating intent.
    pub correlation_id: CorrelationId,
    /// Original (or last-modified) intent.
    pub intent: OrderIntent,
    /// Cumulative filled quantity.
    pub filled_qty: u64,
    /// Volume-weighted average fill price in paise.
    pub avg_fill_paise: i64,
    /// Current FSM state.
    pub state: OrderLifecycleState,
    /// Monotonic ts_ns of the most recent transition (recorded by the
    /// caller via `hedge_core::now_ns()`).
    pub last_ts_ns: u64,
    /// History of fill levels in order of arrival; useful in tests and
    /// for the optional emit-fill stream.
    pub fills: Vec<BookLevel>,
}

impl OrderRecord {
    /// Construct a fresh record in `New` state.
    pub fn new(
        broker_order_id: String,
        intent: OrderIntent,
        ts_ns: u64,
    ) -> Self {
        let correlation_id = intent.correlation_id;
        Self {
            broker_order_id,
            correlation_id,
            intent,
            filled_qty: 0,
            avg_fill_paise: 0,
            state: OrderLifecycleState::New,
            last_ts_ns: ts_ns,
            fills: Vec::new(),
        }
    }

    /// Apply a batch of fills against this order, updating the running
    /// total, volume-weighted avg fill, and FSM state.
    ///
    /// The transition rules are:
    ///
    /// * `total_filled == 0`            → state stays `Submitted` (or
    ///                                    transitions to `Submitted` if it
    ///                                    was `New`).
    /// * `0 < total_filled < requested` → `PartiallyFilled`.
    /// * `total_filled == requested`    → `Filled` (terminal).
    pub fn apply_fills(&mut self, fills: &[BookLevel], ts_ns: u64) {
        self.last_ts_ns = ts_ns;
        if fills.is_empty() {
            // No-op fill batch — Submitted transition still allowed.
            if matches!(self.state, OrderLifecycleState::New) {
                self.state = OrderLifecycleState::Submitted;
            }
            return;
        }

        // Update vwap using the running totals approach so we never lose
        // precision on long fill chains.
        let mut prior_qty = self.filled_qty as i128;
        let mut prior_notional =
            (self.avg_fill_paise as i128).saturating_mul(prior_qty);
        for f in fills {
            let q = f.qty as i128;
            prior_notional =
                prior_notional.saturating_add((f.price_paise as i128).saturating_mul(q));
            prior_qty = prior_qty.saturating_add(q);
            self.fills.push(*f);
        }
        let new_qty = if prior_qty > u64::MAX as i128 {
            u64::MAX
        } else {
            prior_qty as u64
        };
        self.filled_qty = new_qty;
        let new_avg = if new_qty == 0 {
            0
        } else {
            let v = prior_notional / prior_qty;
            if v > i64::MAX as i128 {
                i64::MAX
            } else if v < i64::MIN as i128 {
                i64::MIN
            } else {
                v as i64
            }
        };
        self.avg_fill_paise = new_avg;

        let requested = self.intent.quantity.raw();
        self.state = if self.filled_qty >= requested {
            OrderLifecycleState::Filled
        } else if self.filled_qty == 0 {
            OrderLifecycleState::Submitted
        } else {
            OrderLifecycleState::PartiallyFilled
        };
    }

    /// Apply a modification. The simulated broker accepts only quantity
    /// or limit-price changes on non-terminal orders. Terminal orders
    /// produce [`BrokerError::Rejected`].
    pub fn apply_modification(
        &mut self,
        m: &OrderModification,
        ts_ns: u64,
    ) -> Result<(), BrokerError> {
        if self.state.is_terminal() {
            return Err(BrokerError::Rejected(format!(
                "cannot modify terminal order in state {:?}",
                self.state
            )));
        }
        if let Some(q) = m.new_quantity {
            // Refuse to set quantity below already-filled.
            if q.raw() < self.filled_qty {
                return Err(BrokerError::Rejected(format!(
                    "new quantity {} is below already-filled {}",
                    q.raw(),
                    self.filled_qty
                )));
            }
            self.intent.quantity = q;
        }
        if let Some(limit) = m.new_limit_paise {
            self.intent.limit_paise = limit;
        }
        self.last_ts_ns = ts_ns;
        Ok(())
    }

    /// Cancel the order. Must currently be in a non-terminal state.
    pub fn apply_cancel(&mut self, ts_ns: u64) -> Result<(), BrokerError> {
        if self.state.is_terminal() {
            return Err(BrokerError::Rejected(format!(
                "cannot cancel terminal order in state {:?}",
                self.state
            )));
        }
        self.state = OrderLifecycleState::Cancelled;
        self.last_ts_ns = ts_ns;
        Ok(())
    }

    /// Project to the public [`OrderStatus`] read model.
    pub fn to_status(&self) -> OrderStatus {
        OrderStatus {
            broker_order_id: self.broker_order_id.clone(),
            state: self.state,
            filled_qty: Qty::new(self.filled_qty),
            avg_fill_paise: self.avg_fill_paise,
            broker_ts_ns: Some(self.last_ts_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_broker_api::{Exchange, OrderType};
    use hedge_core::Side;

    fn make_intent(qty: u64) -> OrderIntent {
        OrderIntent {
            correlation_id: CorrelationId::new(),
            symbol_raw: 1,
            side: Side::Buy,
            quantity: Qty::new(qty),
            order_type: OrderType::Limit,
            limit_paise: 100_00,
            exchange: Exchange::Nse,
        }
    }

    #[test]
    fn new_record_starts_in_new_state() {
        let r = OrderRecord::new("SIM-1".into(), make_intent(10), 1);
        assert_eq!(r.state, OrderLifecycleState::New);
        assert_eq!(r.filled_qty, 0);
        assert_eq!(r.avg_fill_paise, 0);
    }

    #[test]
    fn empty_fill_batch_transitions_new_to_submitted() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(10), 1);
        r.apply_fills(&[], 2);
        assert_eq!(r.state, OrderLifecycleState::Submitted);
        assert_eq!(r.filled_qty, 0);
    }

    #[test]
    fn partial_fill_transitions_to_partially_filled() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(10), 1);
        r.apply_fills(&[BookLevel::new(100_00, 4)], 2);
        assert_eq!(r.state, OrderLifecycleState::PartiallyFilled);
        assert_eq!(r.filled_qty, 4);
        assert_eq!(r.avg_fill_paise, 100_00);
    }

    #[test]
    fn full_fill_transitions_to_filled() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(10), 1);
        r.apply_fills(
            &[BookLevel::new(100_00, 4), BookLevel::new(101_00, 6)],
            2,
        );
        assert_eq!(r.state, OrderLifecycleState::Filled);
        assert_eq!(r.filled_qty, 10);
        // (100_00*4 + 101_00*6)/10 = (40000 + 60600)/10 = 100600/10 = 10060
        assert_eq!(r.avg_fill_paise, 10060);
    }

    #[test]
    fn fills_continue_to_update_vwap_correctly() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(20), 1);
        r.apply_fills(&[BookLevel::new(100_00, 5)], 2);
        r.apply_fills(&[BookLevel::new(102_00, 5)], 3);
        // 5*100_00 + 5*102_00 = 50000 + 51000 = 101000; /10 = 10100
        assert_eq!(r.avg_fill_paise, 10100);
        assert_eq!(r.filled_qty, 10);
        assert_eq!(r.state, OrderLifecycleState::PartiallyFilled);
    }

    #[test]
    fn modify_quantity_below_filled_is_rejected() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(10), 1);
        r.apply_fills(&[BookLevel::new(100_00, 7)], 2);
        let m = OrderModification {
            broker_order_id: "SIM-1".into(),
            new_quantity: Some(Qty::new(5)),
            new_limit_paise: None,
        };
        let err = r.apply_modification(&m, 3).unwrap_err();
        match err {
            BrokerError::Rejected(_) => {}
            _ => panic!("wrong error variant: {err:?}"),
        }
    }

    #[test]
    fn modify_terminal_order_rejected() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(5), 1);
        r.apply_fills(&[BookLevel::new(100_00, 5)], 2);
        let m = OrderModification {
            broker_order_id: "SIM-1".into(),
            new_quantity: Some(Qty::new(7)),
            new_limit_paise: None,
        };
        let err = r.apply_modification(&m, 3).unwrap_err();
        match err {
            BrokerError::Rejected(_) => {}
            _ => panic!("wrong error variant"),
        }
    }

    #[test]
    fn cancel_non_terminal_succeeds() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(10), 1);
        r.apply_fills(&[BookLevel::new(100_00, 4)], 2);
        r.apply_cancel(3).unwrap();
        assert_eq!(r.state, OrderLifecycleState::Cancelled);
    }

    #[test]
    fn cancel_terminal_rejected() {
        let mut r = OrderRecord::new("SIM-1".into(), make_intent(5), 1);
        r.apply_fills(&[BookLevel::new(100_00, 5)], 2);
        let err = r.apply_cancel(3).unwrap_err();
        match err {
            BrokerError::Rejected(_) => {}
            _ => panic!("wrong error variant"),
        }
    }
}
