//! `PositionEngine` — the in-process registry that owns every live
//! [`Position`] (R8.1, R8.2, R8.3, R8.4, R8.5).
//!
//! ## Wiring
//!
//! At runtime the engine subscribes to two event streams:
//!
//! * `hedge.hot.fills` (Redis Streams consumer-group `position_engine`) —
//!   ordered fills produced by the Execution_Engine. Every entry must be
//!   processed within 5 ms (R8.2). Per-fill processing is the
//!   [`PositionEngine::on_fill`] entry point.
//! * `md.tick.<symbol>` (NATS) — per-symbol tick stream. Only ticks for
//!   symbols with an open position update unrealised PnL (R8.3). Per-tick
//!   processing is [`PositionEngine::on_tick`].
//!
//! After every state mutation the engine publishes:
//!
//! * `pos.update.<symbol>` — the updated [`Position`]. Throttled to ≤ 10
//!   updates per second per symbol (per task 16.1 spec) so a tick storm on
//!   a held symbol does not flood the bus.
//! * `pos.risk_state` — the aggregate [`TraderRiskState`]. Emitted on every
//!   `on_fill` (because exposure / margin always change) and on every
//!   `on_tick` for held symbols when the recomputed state differs from
//!   the previously published state.
//!
//! The engine is fully in-process: callers wire the network bindings and
//! call into the engine. This separation keeps the engine unit-testable
//! without standing up Redis or NATS.

use std::sync::Arc;

use dashmap::DashMap;
use hedge_core::{Px, Side, SymbolId};
use parking_lot::Mutex;
use tracing::instrument;

use crate::pnl::unrealized_pnl_paise;
use crate::position::{Position, StrategyAllocation};
use crate::risk_state::{aggregate_state, TraderRiskState};

/// Default per-symbol minimum interval between successive `pos.update.<sym>`
/// emissions, expressed in nanoseconds. 100 ms = 10 updates / second.
pub const DEFAULT_POS_UPDATE_THROTTLE_NS: u64 = 100_000_000;

/// One published event the engine asks the network layer to fan out.
///
/// Returning a typed event from `on_fill` / `on_tick` lets the engine stay
/// transport-agnostic: tests assert on the events and the binary in
/// `bin/main.rs` translates them into NATS / Redis publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionEvent {
    /// Per-symbol position snapshot. Subject: `pos.update.<symbol>`.
    PositionUpdate {
        /// Symbol the snapshot belongs to.
        symbol: SymbolId,
        /// Snapshot of the position at the moment of emission.
        snapshot: Box<PositionSnapshot>,
    },
    /// Aggregate trader risk state. Subject: `pos.risk_state`.
    RiskState(TraderRiskState),
}

/// Plain-data snapshot of a [`Position`] used inside [`PositionEvent`] so
/// downstream consumers do not need to hold the engine's internal locks.
///
/// Fields mirror [`Position`] but the `strategy_allocations` field is a
/// regular `Vec` for ergonomic deserialisation in the UI gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionSnapshot {
    /// Symbol the snapshot belongs to.
    pub symbol: SymbolId,
    /// Signed quantity.
    pub quantity: i64,
    /// Volume-weighted average entry price.
    pub avg_entry_px: Px,
    /// Cumulative realised PnL in paise.
    pub realized_pnl_paise: i64,
    /// Cached unrealised PnL at `last_mark_px` in paise.
    pub unrealized_pnl_paise: i64,
    /// Last seen mark price.
    pub last_mark_px: Px,
    /// Per-strategy attribution.
    pub strategy_allocations: Vec<StrategyAllocation>,
}

impl PositionSnapshot {
    fn from_position(p: &Position) -> Self {
        Self {
            symbol: p.symbol,
            quantity: p.quantity,
            avg_entry_px: p.avg_entry_px,
            realized_pnl_paise: p.realized_pnl_paise,
            unrealized_pnl_paise: p.unrealized_pnl_paise,
            last_mark_px: p.last_mark_px,
            strategy_allocations: p.strategy_allocations.iter().copied().collect(),
        }
    }
}

/// In-process registry holding one [`Position`] per [`SymbolId`].
///
/// All mutating entry points are thread-safe: positions are sharded by
/// `DashMap` and each one is wrapped in a [`parking_lot::Mutex`]. The
/// aggregate `TraderRiskState` is kept behind a separate mutex because
/// recomputing it walks every position.
///
/// Cloning the engine is cheap — the internals are `Arc`-wrapped.
#[derive(Clone)]
pub struct PositionEngine {
    inner: Arc<PositionEngineInner>,
}

struct PositionEngineInner {
    positions: DashMap<SymbolId, Mutex<Position>>,
    /// Aggregate state and previous-emit timestamps.
    state: Mutex<EngineState>,
    /// Configured base capital in paise.
    base_capital_paise: i64,
    /// Per-symbol throttle interval in nanoseconds for `pos.update.<sym>`
    /// emissions.
    pos_update_throttle_ns: u64,
}

#[derive(Debug)]
struct EngineState {
    aggregate: TraderRiskState,
    last_emit_ns: std::collections::HashMap<SymbolId, u64>,
    /// Last published aggregate so on-tick paths can suppress duplicate
    /// emissions when nothing changed (avoids per-tick storms).
    last_published_aggregate: Option<TraderRiskState>,
}

impl PositionEngine {
    /// Construct a new engine seeded with the given base capital.
    ///
    /// `base_capital_paise = capital.base_inr × 100`. R32.1 default is
    /// ₹20,000 → 2,000,000 paise; tests pass smaller values for clarity.
    pub fn new(base_capital_paise: i64) -> Self {
        Self::with_throttle(base_capital_paise, DEFAULT_POS_UPDATE_THROTTLE_NS)
    }

    /// Construct an engine with a custom per-symbol throttle interval.
    /// Useful for tests that want to assert throttle behaviour
    /// deterministically without timing constants.
    pub fn with_throttle(base_capital_paise: i64, pos_update_throttle_ns: u64) -> Self {
        Self {
            inner: Arc::new(PositionEngineInner {
                positions: DashMap::new(),
                state: Mutex::new(EngineState {
                    aggregate: TraderRiskState::fresh(base_capital_paise),
                    last_emit_ns: std::collections::HashMap::new(),
                    last_published_aggregate: None,
                }),
                base_capital_paise,
                pos_update_throttle_ns,
            }),
        }
    }

    /// Process a single fill (R8.2). Returns the events the caller should
    /// publish to NATS.
    ///
    /// `now_ns` is supplied by the caller (the binary uses
    /// [`hedge_core::now_ns`]) so tests can drive the throttle clock
    /// deterministically.
    ///
    /// The published-event vector is bounded to two: a `PositionUpdate`
    /// for the symbol (if not throttled) and a `RiskState`. A fill always
    /// shifts exposure / margin, so `RiskState` always emits.
    #[instrument(
        level = "trace",
        skip(self),
        fields(
            position.symbol = symbol.raw(),
            position.side = ?side,
            position.fill_qty = fill_qty,
            position.fill_paise = fill_px.to_paise(),
            now_ns
        )
    )]
    pub fn on_fill(
        &self,
        symbol: SymbolId,
        side: Side,
        fill_qty: u64,
        fill_px: Px,
        now_ns: u64,
    ) -> Vec<PositionEvent> {
        // Step 1: mutate the position.
        let snapshot = {
            let entry = self
                .inner
                .positions
                .entry(symbol)
                .or_insert_with(|| Mutex::new(Position::flat(symbol)));
            let mut guard = entry.lock();
            guard.apply_fill(side, fill_qty, fill_px);
            PositionSnapshot::from_position(&guard)
        };

        // Step 2: walk every position to recompute aggregate state.
        let new_state = self.recompute_aggregate();

        // Step 3: stamp the per-symbol emission timestamp; fills always
        // emit (R8.2 mandates a 5 ms recompute on every fill — a throttle
        // here would make the property untestable). We also record the
        // newly-published aggregate so the next on_tick can suppress an
        // identical re-emission.
        {
            let mut state = self.inner.state.lock();
            state.last_emit_ns.insert(symbol, now_ns);
            state.last_published_aggregate = Some(new_state);
        }

        let mut out = Vec::with_capacity(2);
        out.push(PositionEvent::PositionUpdate {
            symbol,
            snapshot: Box::new(snapshot),
        });
        out.push(PositionEvent::RiskState(new_state));
        out
    }

    /// Process a single market tick (R8.3). Returns the events the caller
    /// should publish.
    ///
    /// * If the symbol has no open position, returns an empty vector
    ///   (R8.3: "for held symbols").
    /// * If a position exists, updates `last_mark_px` and recomputes
    ///   `unrealized_pnl_paise`.
    /// * Emits `pos.update.<sym>` only if at least
    ///   `pos_update_throttle_ns` has elapsed since the last emission for
    ///   this symbol (≤ 10/s default).
    /// * Emits `pos.risk_state` only if the recomputed aggregate differs
    ///   from the previously published one.
    #[instrument(
        level = "trace",
        skip(self),
        fields(position.symbol = symbol.raw(), position.mark_paise = mark_px.to_paise(), now_ns)
    )]
    pub fn on_tick(&self, symbol: SymbolId, mark_px: Px, now_ns: u64) -> Vec<PositionEvent> {
        // Step 1: bail out fast if no position is held on this symbol.
        let snapshot = match self.inner.positions.get(&symbol) {
            None => return Vec::new(),
            Some(entry) => {
                let mut guard = entry.lock();
                if guard.quantity == 0 {
                    // Cached flat position — drop the mark and return
                    // without emitting.
                    guard.last_mark_px = mark_px;
                    guard.unrealized_pnl_paise = 0;
                    return Vec::new();
                }
                guard.apply_mark(mark_px);
                PositionSnapshot::from_position(&guard)
            }
        };

        // Step 2: throttle the per-symbol position emission.
        let mut events = Vec::with_capacity(2);
        let mut state = self.inner.state.lock();
        let last_emit = state.last_emit_ns.get(&symbol).copied().unwrap_or(0);
        let elapsed = now_ns.saturating_sub(last_emit);
        if elapsed >= self.inner.pos_update_throttle_ns {
            state.last_emit_ns.insert(symbol, now_ns);
            events.push(PositionEvent::PositionUpdate {
                symbol,
                snapshot: Box::new(snapshot),
            });
        }
        // Drop the lock before walking positions to avoid holding it across
        // the recompute.
        drop(state);

        // Step 3: recompute aggregate; emit only on change.
        let new_state = self.recompute_aggregate();
        let mut state = self.inner.state.lock();
        let should_emit_aggregate = state
            .last_published_aggregate
            .map(|prev| prev != new_state)
            .unwrap_or(true);
        if should_emit_aggregate {
            state.last_published_aggregate = Some(new_state);
            events.push(PositionEvent::RiskState(new_state));
        }

        events
    }

    /// Returns a snapshot of the current aggregate state without mutating
    /// or emitting.
    pub fn snapshot_risk_state(&self) -> TraderRiskState {
        self.inner.state.lock().aggregate
    }

    /// Read-only access to the position for `symbol`, if any.
    /// Returns a clone so the caller does not hold any internal lock.
    pub fn position_of(&self, symbol: SymbolId) -> Option<Position> {
        self.inner
            .positions
            .get(&symbol)
            .map(|entry| entry.lock().clone())
    }

    /// Per-strategy capital allocations across every symbol. Implements the
    /// "per-strategy capital allocation" accessor required by R8.4.
    ///
    /// Returns an owned `Vec<(SymbolId, StrategyAllocation)>` so the caller
    /// holds no internal locks.
    pub fn strategy_allocations(&self) -> Vec<(SymbolId, StrategyAllocation)> {
        let mut out = Vec::new();
        for entry in self.inner.positions.iter() {
            let guard = entry.value().lock();
            for alloc in guard.strategy_allocations.iter() {
                out.push((guard.symbol, *alloc));
            }
        }
        out
    }

    /// Set a position's per-strategy allocations. Used by the binary's
    /// orchestration layer when the Risk_Engine hands down a sized
    /// approval; tests use it to exercise the accessor.
    pub fn set_strategy_allocations(
        &self,
        symbol: SymbolId,
        allocs: smallvec::SmallVec<[StrategyAllocation; 4]>,
    ) {
        let entry = self
            .inner
            .positions
            .entry(symbol)
            .or_insert_with(|| Mutex::new(Position::flat(symbol)));
        entry.lock().set_strategy_allocations(allocs);
    }

    /// Number of tracked symbols (held + previously held + flat).
    pub fn tracked_symbols(&self) -> usize {
        self.inner.positions.len()
    }

    /// Borrow the configured base capital in paise.
    pub fn base_capital_paise(&self) -> i64 {
        self.inner.base_capital_paise
    }

    fn recompute_aggregate(&self) -> TraderRiskState {
        let positions: Vec<Position> = self
            .inner
            .positions
            .iter()
            .map(|entry| entry.value().lock().clone())
            .collect();
        let prev_peak = self.inner.state.lock().aggregate.peak_equity_paise;
        let new_state =
            aggregate_state(positions.iter(), self.inner.base_capital_paise, prev_peak);
        self.inner.state.lock().aggregate = new_state;
        new_state
    }
}

/// Helper: compute the unrealised PnL a tick at `mark_px` would produce on a
/// position currently at `(qty, avg)`. Exposed for callers that need the
/// projection without applying it.
#[inline]
pub fn project_unrealized(qty: i64, avg_entry_px: Px, mark_px: Px) -> i64 {
    unrealized_pnl_paise(qty, avg_entry_px, mark_px)
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    fn px(paise: i64) -> Px {
        Px::from_paise(paise)
    }

    fn sym(id: u32) -> SymbolId {
        SymbolId::new(id)
    }

    const BASE_CAPITAL: i64 = 20_000 * 100;

    /// Fills always emit two events: PositionUpdate and RiskState. Verifies
    /// the position state and the aggregate state.
    #[test]
    fn on_fill_emits_position_update_and_risk_state() {
        let engine = PositionEngine::new(BASE_CAPITAL);
        let events = engine.on_fill(sym(1), Side::Buy, 10, px(100_00), 0);
        assert_eq!(events.len(), 2);
        match &events[0] {
            PositionEvent::PositionUpdate { symbol, snapshot } => {
                assert_eq!(*symbol, sym(1));
                assert_eq!(snapshot.quantity, 10);
                assert_eq!(snapshot.avg_entry_px, px(100_00));
                assert_eq!(snapshot.realized_pnl_paise, 0);
            }
            other => panic!("unexpected first event: {other:?}"),
        }
        match &events[1] {
            PositionEvent::RiskState(s) => {
                assert_eq!(s.aggregate_exposure_paise, 10 * 100_00);
            }
            other => panic!("unexpected second event: {other:?}"),
        }
    }

    /// On a held symbol, a tick within the throttle window does NOT emit a
    /// PositionUpdate; the second tick after `throttle_ns` does.
    #[test]
    fn on_tick_throttles_position_update_emissions() {
        let engine = PositionEngine::with_throttle(BASE_CAPITAL, 100_000_000); // 100 ms
        // Open position at t=0.
        engine.on_fill(sym(1), Side::Buy, 10, px(100_00), 0);

        // Tick at t=10ms (< 100ms): no PositionUpdate.
        let evts = engine.on_tick(sym(1), px(101_00), 10_000_000);
        assert!(
            !evts
                .iter()
                .any(|e| matches!(e, PositionEvent::PositionUpdate { .. })),
            "expected no PositionUpdate within throttle window"
        );

        // Tick at t=200ms (> 100ms after fill at t=0): PositionUpdate emits.
        let evts = engine.on_tick(sym(1), px(102_00), 200_000_000);
        assert!(
            evts.iter()
                .any(|e| matches!(e, PositionEvent::PositionUpdate { .. })),
            "expected PositionUpdate after throttle elapsed"
        );
    }

    /// Tick on a symbol with no open position yields no events.
    #[test]
    fn on_tick_unheld_symbol_emits_nothing() {
        let engine = PositionEngine::new(BASE_CAPITAL);
        let evts = engine.on_tick(sym(99), px(100_00), 0);
        assert!(evts.is_empty());
    }

    /// Tick on a flat (formerly held) symbol still emits nothing for the
    /// position update; the engine retains the flat record but suppresses
    /// the event because there is no exposure.
    #[test]
    fn on_tick_flat_symbol_emits_nothing() {
        let engine = PositionEngine::new(BASE_CAPITAL);
        // Open and close.
        engine.on_fill(sym(1), Side::Buy, 10, px(100_00), 0);
        engine.on_fill(sym(1), Side::Sell, 10, px(100_00), 1);
        let evts = engine.on_tick(sym(1), px(105_00), 1_000_000_000);
        assert!(evts.is_empty(), "expected no events for flat symbol, got {:?}", evts);
    }

    /// Tick on a held symbol emits a fresh RiskState only when the
    /// aggregate state actually changed. A second identical tick does not
    /// re-emit RiskState.
    #[test]
    fn on_tick_does_not_emit_duplicate_risk_state() {
        let engine = PositionEngine::with_throttle(BASE_CAPITAL, 0);
        engine.on_fill(sym(1), Side::Buy, 10, px(100_00), 0);

        let first = engine.on_tick(sym(1), px(110_00), 1_000_000_000);
        assert!(first.iter().any(|e| matches!(e, PositionEvent::RiskState(_))));

        let second = engine.on_tick(sym(1), px(110_00), 2_000_000_000);
        assert!(
            !second
                .iter()
                .any(|e| matches!(e, PositionEvent::RiskState(_))),
            "duplicate RiskState emitted on identical tick: {:?}",
            second
        );
    }

    /// Multi-symbol exposure aggregates correctly across fills.
    #[test]
    fn aggregate_state_sums_across_symbols() {
        let engine = PositionEngine::new(BASE_CAPITAL);
        engine.on_fill(sym(1), Side::Buy, 10, px(100_00), 0);
        engine.on_fill(sym(2), Side::Sell, 5, px(200_00), 1);
        let s = engine.snapshot_risk_state();
        assert_eq!(s.aggregate_exposure_paise, 10 * 100_00 + 5 * 200_00);
    }

    /// `position_of` returns a snapshot, not a live reference, and is `None`
    /// for unknown symbols.
    #[test]
    fn position_of_returns_clone_or_none() {
        let engine = PositionEngine::new(BASE_CAPITAL);
        assert!(engine.position_of(sym(1)).is_none());

        engine.on_fill(sym(1), Side::Buy, 7, px(50_00), 0);
        let p = engine.position_of(sym(1)).unwrap();
        assert_eq!(p.quantity, 7);
        assert_eq!(p.avg_entry_px, px(50_00));
    }

    /// R8.4: per-strategy allocations are exposed via the engine.
    #[test]
    fn strategy_allocations_accessor() {
        let engine = PositionEngine::new(BASE_CAPITAL);
        engine.on_fill(sym(1), Side::Buy, 10, px(100_00), 0);

        let mut allocs: SmallVec<[StrategyAllocation; 4]> = SmallVec::new();
        allocs.push(StrategyAllocation {
            strategy_id: 2,
            quantity: 10,
            allocated_capital_inr: 1_000,
        });
        engine.set_strategy_allocations(sym(1), allocs);

        let out = engine.strategy_allocations();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, sym(1));
        assert_eq!(out[0].1.strategy_id, 2);
    }

    /// First tick after a fill always emits a PositionUpdate because the
    /// fill set the per-symbol last_emit_ns and the throttle has not yet
    /// elapsed — verify this expectation explicitly so the throttle policy
    /// is documented.
    #[test]
    fn first_tick_after_fill_is_throttled() {
        let engine = PositionEngine::with_throttle(BASE_CAPITAL, 100_000_000);
        engine.on_fill(sym(1), Side::Buy, 10, px(100_00), 0);
        let evts = engine.on_tick(sym(1), px(101_00), 50_000_000);
        // Throttle blocks the position update; RiskState may still emit
        // because unrealised PnL changed.
        assert!(!evts
            .iter()
            .any(|e| matches!(e, PositionEvent::PositionUpdate { .. })));
    }
}
