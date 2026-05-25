//! Orderflow event variants and detection logic.
//!
//! Implements the `OrderflowEvent` enum from `design.md § Components §
//! Orderflow_Engine` and the four detection patterns required by the task:
//!
//! * **Spoofing** (R2.3): a large limit order whose top-of-book qty is
//!   greater than `5×` the rolling median of recent top quotes, that
//!   disappears within `500 ms` without any fill recorded against it.
//! * **Absorption** (R2.2): an aggressive trade that fills at a level whose
//!   visible qty is *less than* the executed volume — i.e. hidden liquidity
//!   absorbed the aggressor.
//! * **Liquidity Gap** (R2.2): a price gap of more than `3` ticks between
//!   the top-of-book level and the next visible level on either side.
//! * **Hidden Liquidity** (R2.2): trade volume at a price that exceeds the
//!   displayed volume at that level on the opposite side of the book.
//!
//! ### No-allocation discipline (R2.6)
//!
//! Every detector keeps its working state in inline storage:
//!
//! * The spoofing detector uses a fixed-size [`ArrayVec<PendingOrder, 32>`]
//!   pending-orders ring (capped at 32 simultaneously-watched candidates);
//!   when full, the oldest pending order is overwritten in place rather
//!   than spilling onto the heap.
//! * The rolling-median computation runs over a [`RingWindow<u64, N>`]
//!   sized at compile time.
//!
//! Events are accumulated into `BoundedEvents<OrderflowEvent, 4>` and
//! emitted on `of.event.<sym>` per book/tick update.

use hedge_core::{ArrayVec, Px, RingWindow, Side};
use hedge_schemas::{BookLevel, Tick};
use serde::{Deserialize, Serialize};

use crate::book::LiveBook;

/// Discriminated event type for the four orderflow patterns.
///
/// Wire form is JSON (the `of.event.<sym>` subject uses the JSON codec —
/// see `hedge_bus::JsonCodec`). Field names are stable.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrderflowEvent {
    /// Gap of more than [`LIQUIDITY_GAP_TICKS`] between top-of-book and the
    /// next visible level on `side`.
    LiquidityGap {
        /// Side of the book where the gap was detected.
        side: Side,
        /// Top-of-book level that introduced the gap.
        level: Px,
        /// Visible quantity at the top-of-book level.
        size: u64,
    },
    /// Aggressive order at `level` consumed more volume than was visible at
    /// that level — hidden liquidity absorbed the aggressor.
    Absorption {
        /// Side of the book that absorbed the aggressor (opposite of the
        /// aggressor's side).
        side: Side,
        /// Price level at which absorption occurred.
        level: Px,
        /// Volume traded.
        size: u64,
    },
    /// Trade volume at `level` exceeds the displayed volume at that level.
    HiddenLiquidity {
        /// Side that displayed less volume than was traded.
        side: Side,
        /// Price level where the discrepancy was observed.
        level: Px,
    },
    /// A large quote that disappeared without filling — likely spoofing.
    Spoofing {
        /// Side of the book the spoof was placed on.
        side: Side,
        /// Price level the spoof was placed at.
        level: Px,
        /// Evidence score in `[0.0, 1.0]`. `1.0` = strong signal.
        evidence_score: f32,
    },
}

/// Liquidity-gap threshold: gap > 3 ticks (R2 design § Components).
pub const LIQUIDITY_GAP_TICKS: i64 = 3;

/// Default tick size in paise. Most NSE equities trade at 5 paise / 1 paise
/// ticks; we use 1 paise as the conservative default so the gap threshold
/// fires only on genuinely large discontinuities. Per-symbol overrides flow
/// through [`Detector::with_tick_size`].
pub const DEFAULT_TICK_SIZE_PAISE: i64 = 1;

/// Spoof candidate: top-of-book quote large enough to warrant watching.
const SPOOF_QTY_MULTIPLIER: u64 = 5;

/// Time window within which a candidate must disappear to count as a spoof.
pub const SPOOF_WINDOW_NS: u64 = 500_000_000; // 500 ms

/// Number of historical top-of-book quote sizes used for the rolling median
/// that drives the spoofing detector. 64 fits comfortably in inline storage
/// and provides ~30 s of history on a 500 ms book cadence.
pub const SPOOF_MEDIAN_WINDOW: usize = 64;

/// Maximum number of pending spoof candidates we track at any one time.
/// Going over this cap is exceedingly rare in practice; on overflow the
/// oldest pending entry is overwritten (FIFO).
pub const SPOOF_PENDING_CAPACITY: usize = 32;

/// One pending spoof candidate awaiting either a fill or a cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingOrder {
    side: Side,
    /// Price in paise.
    price_paise: i64,
    /// Visible quantity at placement.
    qty: u64,
    /// Wall-clock equivalent timestamp (ns since process epoch) at which the
    /// candidate was first observed.
    placed_ts_ns: u64,
    /// Cumulative qty that traded through this level since placement. Used
    /// to discriminate "vanished" from "filled".
    filled_qty: u64,
}

/// Stateful detector for the four orderflow patterns.
///
/// One detector instance per symbol. The detector holds:
///
/// * A [`RingWindow`] of recent top-of-book qty values for the rolling
///   median used by the spoofing test.
/// * An [`ArrayVec`] of pending spoof candidates.
/// * The most recent best bid / ask qty and price for the absorption /
///   hidden-liquidity tests.
///
/// The struct is `Default` so the engine can `entry().or_default()`-style
/// instantiate it. It is intentionally **not** `Clone` because the inner
/// [`RingWindow`] does not implement `Clone`; cloning a live detector would
/// violate the inline-storage discipline anyway.
#[derive(Debug)]
pub struct Detector {
    tick_size_paise: i64,
    qty_history: RingWindow<u64, SPOOF_MEDIAN_WINDOW>,
    pending: ArrayVec<PendingOrder, SPOOF_PENDING_CAPACITY>,
    last_top_bid: Option<BookLevel>,
    last_top_ask: Option<BookLevel>,
}

impl Default for Detector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector {
    /// Construct a detector with the default tick size.
    #[inline]
    pub fn new() -> Self {
        Self::with_tick_size(DEFAULT_TICK_SIZE_PAISE)
    }

    /// Construct a detector with a per-symbol tick-size override.
    pub fn with_tick_size(tick_size_paise: i64) -> Self {
        Self {
            tick_size_paise: tick_size_paise.max(1),
            qty_history: RingWindow::new(),
            pending: ArrayVec::new(),
            last_top_bid: None,
            last_top_ask: None,
        }
    }

    /// Tick size in paise. Always `>= 1`.
    #[inline]
    pub fn tick_size_paise(&self) -> i64 {
        self.tick_size_paise
    }

    /// Number of pending spoof candidates currently tracked. Exposed for
    /// tests; production code should not depend on this value.
    #[inline]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Median of the rolling top-of-book qty window. Returns `0` when the
    /// window is empty.
    fn rolling_median_qty(&self) -> u64 {
        if self.qty_history.is_empty() {
            return 0;
        }
        // We need k = len/2; quickselect would be O(n) but the inline window
        // is at most 64 elements. A copy-and-sort over an inline buffer is
        // cheap and allocation-free.
        let mut buf: ArrayVec<u64, SPOOF_MEDIAN_WINDOW> = ArrayVec::new();
        for &v in self.qty_history.iter() {
            // Bounded by capacity; safe to ignore overflow result.
            let _ = buf.try_push(v);
        }
        buf.as_mut_slice().sort_unstable();
        let n = buf.len();
        if n == 0 {
            return 0;
        }
        if n % 2 == 0 {
            // Round-down average for even sample counts is fine for a
            // threshold comparison.
            let lo = buf.as_slice()[n / 2 - 1];
            let hi = buf.as_slice()[n / 2];
            ((lo as u128 + hi as u128) / 2) as u64
        } else {
            buf.as_slice()[n / 2]
        }
    }

    /// Drive the detector through the post-update state of `book` at the
    /// monotonic timestamp `now_ns`. Returns the number of new events
    /// pushed into `out`.
    ///
    /// **Allocation discipline**: the function only mutates inline storage
    /// (the rolling window, the pending ArrayVec, and the caller-supplied
    /// `BoundedEvents`). No heap allocation occurs in steady state.
    pub fn observe_book(
        &mut self,
        book: &LiveBook,
        now_ns: u64,
        out: &mut hedge_core::BoundedEvents<OrderflowEvent, 4>,
    ) -> usize {
        let mut emitted = 0;

        // 1. Liquidity-gap detection on both sides.
        if let Some(gap) = detect_liquidity_gap(Side::Buy, book.bid_levels(), self.tick_size_paise)
        {
            if out.try_push(gap).is_ok() {
                emitted += 1;
            }
        }
        if let Some(gap) = detect_liquidity_gap(Side::Sell, book.ask_levels(), self.tick_size_paise)
        {
            if out.try_push(gap).is_ok() {
                emitted += 1;
            }
        }

        // 2. Spoofing detection — quote disappearance window.
        emitted += self.advance_spoofing(book, now_ns, out);

        // 3. Update rolling history and last-top snapshots for next round.
        let top_bid_qty = book.top_bid().map(|l| l.qty).unwrap_or(0);
        let top_ask_qty = book.top_ask().map(|l| l.qty).unwrap_or(0);
        // Track the larger of the two so the median represents "size of the
        // dominant top-of-book quote". Both feed the spoof threshold.
        self.qty_history.push(top_bid_qty.max(top_ask_qty));

        // 4. Register fresh spoof candidates: a new top quote whose qty is
        //    > 5× the rolling median.
        let median = self.rolling_median_qty();
        if median > 0 {
            let threshold = median.saturating_mul(SPOOF_QTY_MULTIPLIER);
            if let Some(top) = book.top_bid() {
                if top.qty > threshold {
                    self.try_register_pending(Side::Buy, top, now_ns);
                }
            }
            if let Some(top) = book.top_ask() {
                if top.qty > threshold {
                    self.try_register_pending(Side::Sell, top, now_ns);
                }
            }
        }

        self.last_top_bid = book.top_bid().copied();
        self.last_top_ask = book.top_ask().copied();
        emitted
    }

    /// Drive the detector through a trade event, optionally emitting
    /// `Absorption` and `HiddenLiquidity` events. Updates pending spoof
    /// candidates' `filled_qty` so they are correctly classified as filled
    /// rather than spoofed.
    ///
    /// `book` should be the most recent live book; the detector reads only
    /// its top-of-book levels at the point of the trade for the absorption
    /// classification.
    pub fn observe_trade(
        &mut self,
        tick: &Tick,
        book: &LiveBook,
        out: &mut hedge_core::BoundedEvents<OrderflowEvent, 4>,
    ) -> usize {
        let mut emitted = 0;
        let trade_px = tick.ltp_paise;
        let trade_qty = tick.ltq;
        if trade_qty == 0 {
            return 0;
        }

        // Aggressor classification from the most recent top-of-book.
        // - price >= ask -> aggressive buyer (lifted the offer);
        // - price <= bid -> aggressive seller (hit the bid).
        let aggressor_side = if let Some(ask) = book.top_ask() {
            if trade_px >= ask.price_paise {
                Some(Side::Buy)
            } else if let Some(bid) = book.top_bid() {
                if trade_px <= bid.price_paise {
                    Some(Side::Sell)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(aggr) = aggressor_side {
            // Absorption: trade qty > visible qty at the level being lifted /
            // hit. The "absorbing" side is the opposite of the aggressor.
            let opposite_top = match aggr {
                Side::Buy => book.top_ask(),
                Side::Sell => book.top_bid(),
            };
            if let Some(level) = opposite_top {
                if trade_qty > level.qty {
                    let evt = OrderflowEvent::Absorption {
                        side: aggr.opposite(),
                        level: Px::from_paise(level.price_paise),
                        size: trade_qty,
                    };
                    if out.try_push(evt).is_ok() {
                        emitted += 1;
                    }
                    // The same condition surfaces hidden liquidity at that
                    // level: more was traded than was displayed. Emit only
                    // once per trade by attributing it to the absorbing
                    // side.
                    let evt2 = OrderflowEvent::HiddenLiquidity {
                        side: aggr.opposite(),
                        level: Px::from_paise(level.price_paise),
                    };
                    if out.try_push(evt2).is_ok() {
                        emitted += 1;
                    }
                }
            }
        }

        // Account this trade against any pending spoof candidate at the
        // same price. If the candidate's filled qty meets/exceeds the
        // visible qty at placement, it is classified as filled rather than
        // a spoof and discarded.
        for pending in self.pending.as_mut_slice().iter_mut() {
            if pending.price_paise == trade_px {
                pending.filled_qty = pending.filled_qty.saturating_add(trade_qty);
            }
        }
        // Compact: remove any pending whose filled_qty has reached its qty.
        // This is an in-place retain; ArrayVec does not provide retain so we
        // implement it manually.
        let mut write = 0usize;
        for read in 0..self.pending.len() {
            let p = self.pending.as_slice()[read];
            if p.filled_qty < p.qty {
                self.pending.as_mut_slice()[write] = p;
                write += 1;
            }
        }
        self.pending.truncate(write);

        emitted
    }

    /// Walk the pending list, emitting Spoofing events for any candidate
    /// that has neither filled nor remained on the book within
    /// `SPOOF_WINDOW_NS`. Returns the number of events pushed into `out`.
    fn advance_spoofing(
        &mut self,
        book: &LiveBook,
        now_ns: u64,
        out: &mut hedge_core::BoundedEvents<OrderflowEvent, 4>,
    ) -> usize {
        let mut emitted = 0;
        let mut write = 0usize;
        for read in 0..self.pending.len() {
            let p = self.pending.as_slice()[read];
            let age = now_ns.saturating_sub(p.placed_ts_ns);
            // Did the candidate "disappear"? Check whether the current top of
            // its side either is at a different price or has a much smaller
            // qty than at placement. Partial qty fills against this level
            // are accumulated separately via observe_trade.
            let still_present = match p.side {
                Side::Buy => book
                    .top_bid()
                    .map(|l| l.price_paise == p.price_paise && l.qty >= p.qty / 2)
                    .unwrap_or(false),
                Side::Sell => book
                    .top_ask()
                    .map(|l| l.price_paise == p.price_paise && l.qty >= p.qty / 2)
                    .unwrap_or(false),
            };

            if !still_present && age <= SPOOF_WINDOW_NS && p.filled_qty < p.qty / 4 {
                // Vanished within the window without meaningful fill: a
                // spoof. Evidence score is the ratio of unfilled qty to the
                // size at placement, capped at 1.0.
                let unfilled = p.qty.saturating_sub(p.filled_qty) as f32;
                let total = p.qty.max(1) as f32;
                let evidence = (unfilled / total).clamp(0.0, 1.0);
                let evt = OrderflowEvent::Spoofing {
                    side: p.side,
                    level: Px::from_paise(p.price_paise),
                    evidence_score: evidence,
                };
                if out.try_push(evt).is_ok() {
                    emitted += 1;
                }
                // Drop from pending — handled implicitly by skipping the
                // write step.
                continue;
            }

            if age > SPOOF_WINDOW_NS {
                // Aged out without disappearing: not a spoof, just a large
                // resting order. Drop from pending without emitting.
                continue;
            }

            // Otherwise keep it in pending.
            self.pending.as_mut_slice()[write] = p;
            write += 1;
        }
        self.pending.truncate(write);
        emitted
    }

    fn try_register_pending(&mut self, side: Side, top: &BookLevel, now_ns: u64) {
        // Skip if we already track a pending order at the same side+price —
        // the candidate is the *same* large quote, not a new one.
        for p in self.pending.as_slice() {
            if p.side == side && p.price_paise == top.price_paise {
                return;
            }
        }
        let entry = PendingOrder {
            side,
            price_paise: top.price_paise,
            qty: top.qty,
            placed_ts_ns: now_ns,
            filled_qty: 0,
        };
        if self.pending.try_push(entry).is_err() {
            // Cap reached: overwrite the oldest pending in place. The pending
            // list is small (≤ 32) so a linear scan for "oldest" is cheap.
            let mut oldest_idx = 0usize;
            let mut oldest_ts = u64::MAX;
            for (i, p) in self.pending.as_slice().iter().enumerate() {
                if p.placed_ts_ns < oldest_ts {
                    oldest_ts = p.placed_ts_ns;
                    oldest_idx = i;
                }
            }
            self.pending.as_mut_slice()[oldest_idx] = entry;
        }
    }
}

/// Detect a liquidity gap on `levels`. The slice is assumed to be sorted
/// best-first; a gap is reported when the price difference between the top
/// and the next level exceeds [`LIQUIDITY_GAP_TICKS`] × `tick_size`.
fn detect_liquidity_gap(
    side: Side,
    levels: &[BookLevel],
    tick_size_paise: i64,
) -> Option<OrderflowEvent> {
    if levels.len() < 2 {
        return None;
    }
    let top = levels[0];
    let next = levels[1];
    let gap_paise = (top.price_paise - next.price_paise).abs();
    let threshold_paise = LIQUIDITY_GAP_TICKS.saturating_mul(tick_size_paise);
    if gap_paise > threshold_paise {
        Some(OrderflowEvent::LiquidityGap {
            side,
            level: Px::from_paise(top.price_paise),
            size: top.qty,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_core::BoundedEvents;
    use hedge_schemas::OrderBook;

    fn lvl(price_paise: i64, qty: u64) -> BookLevel {
        BookLevel {
            price_paise,
            qty,
            orders: 1,
        }
    }

    fn make_book(ts_ns: u64, bids: &[BookLevel], asks: &[BookLevel]) -> OrderBook {
        OrderBook {
            correlation_id: [0u8; 16],
            symbol: 1,
            exchange: 0,
            bid_levels: bids.to_vec(),
            ask_levels: asks.to_vec(),
            ts_ns,
        }
    }

    fn make_tick(price_paise: i64, qty: u64) -> Tick {
        Tick {
            correlation_id: [0u8; 16],
            symbol: 1,
            exchange: 0,
            ltp_paise: price_paise,
            bid_paise: 0,
            ask_paise: 0,
            ltq: qty,
            total_buy_qty: 0,
            total_sell_qty: 0,
            ts_exchange_ns: 0,
            ts_recv_ns: 0,
        }
    }

    #[test]
    fn liquidity_gap_emitted_when_gap_exceeds_three_ticks() {
        // Tick size 1 paise; gap of 5 paise > 3 ticks.
        let mut book = LiveBook::new();
        book.apply(&make_book(
            1,
            &[lvl(10000, 10), lvl(9994, 10)], // 6-paise gap on bid side
            &[lvl(10100, 10), lvl(10101, 10)], // 1-paise gap on ask side -> no event
        ));

        let mut det = Detector::new();
        let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, 1, &mut out);

        assert_eq!(out.len(), 1, "should emit exactly one gap event");
        match out.as_slice()[0] {
            OrderflowEvent::LiquidityGap { side, level, size } => {
                assert_eq!(side, Side::Buy);
                assert_eq!(level, Px::from_paise(10000));
                assert_eq!(size, 10);
            }
            other => panic!("expected LiquidityGap, got {:?}", other),
        }
    }

    #[test]
    fn liquidity_gap_not_emitted_when_gap_within_threshold() {
        let mut book = LiveBook::new();
        // 3-paise gap = exactly the threshold; the rule fires only when
        // strictly greater than 3 ticks.
        book.apply(&make_book(
            1,
            &[lvl(10000, 10), lvl(9997, 10)],
            &[lvl(10100, 10), lvl(10103, 10)],
        ));
        let mut det = Detector::new();
        let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, 1, &mut out);
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn absorption_emitted_when_trade_volume_exceeds_visible_at_top_of_opposite_side() {
        let mut book = LiveBook::new();
        // top ask qty 5; an aggressive buyer trades 20 -> absorption.
        book.apply(&make_book(1, &[lvl(9900, 100)], &[lvl(10000, 5)]));
        let mut det = Detector::new();

        // Prime the detector's last-top snapshots with one observe_book call.
        let mut tmp: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, 1, &mut tmp);

        let tick = make_tick(10000, 20); // price >= ask, qty > visible
        let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_trade(&tick, &book, &mut out);

        let has_absorption = out
            .as_slice()
            .iter()
            .any(|e| matches!(e, OrderflowEvent::Absorption { .. }));
        let has_hidden = out
            .as_slice()
            .iter()
            .any(|e| matches!(e, OrderflowEvent::HiddenLiquidity { .. }));
        assert!(has_absorption, "expected Absorption, got {:?}", out);
        assert!(has_hidden, "expected HiddenLiquidity, got {:?}", out);
    }

    #[test]
    fn no_absorption_when_trade_volume_within_visible() {
        let mut book = LiveBook::new();
        book.apply(&make_book(1, &[lvl(9900, 100)], &[lvl(10000, 50)]));
        let mut det = Detector::new();
        let mut tmp: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, 1, &mut tmp);

        let tick = make_tick(10000, 5); // price >= ask but qty < visible
        let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_trade(&tick, &book, &mut out);
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn spoofing_emitted_when_large_quote_disappears_within_window() {
        // Build up a baseline: 10 small books so the rolling median is small.
        let mut det = Detector::new();
        let mut book = LiveBook::new();
        for i in 0..30u64 {
            book.apply(&make_book(
                i + 1,
                &[lvl(9900, 10)],
                &[lvl(10000, 10)],
            ));
            let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
            det.observe_book(&book, (i + 1) * 1_000_000, &mut out);
        }

        // Now plant a huge bid (10x baseline) -> registered as pending.
        let huge_ts = 1_000_000_000u64;
        book.apply(&make_book(31, &[lvl(9950, 1000)], &[lvl(10000, 10)]));
        let mut out1: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, huge_ts, &mut out1);
        // Should have at least one pending candidate; gap rule won't fire
        // because the bid side has only one level here.
        assert!(det.pending_len() >= 1, "spoof candidate not registered");

        // Within the spoof window, the huge bid disappears entirely.
        let cancel_ts = huge_ts + 100_000_000; // 100 ms later
        book.apply(&make_book(32, &[lvl(9900, 10)], &[lvl(10000, 10)]));
        let mut out2: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, cancel_ts, &mut out2);

        // Expect exactly one Spoofing event for the disappeared bid.
        let spoofs: Vec<_> = out2
            .as_slice()
            .iter()
            .filter(|e| matches!(e, OrderflowEvent::Spoofing { .. }))
            .collect();
        assert_eq!(spoofs.len(), 1, "got events: {:?}", out2);
        match spoofs[0] {
            OrderflowEvent::Spoofing {
                side,
                level,
                evidence_score,
            } => {
                assert_eq!(*side, Side::Buy);
                assert_eq!(*level, Px::from_paise(9950));
                assert!((0.0..=1.0).contains(evidence_score));
                assert!(*evidence_score > 0.5, "evidence too low: {}", evidence_score);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn no_spoofing_when_large_quote_persists_past_window() {
        let mut det = Detector::new();
        let mut book = LiveBook::new();
        for i in 0..30u64 {
            book.apply(&make_book(i + 1, &[lvl(9900, 10)], &[lvl(10000, 10)]));
            let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
            det.observe_book(&book, (i + 1) * 1_000_000, &mut out);
        }
        let huge_ts = 1_000_000_000u64;
        book.apply(&make_book(31, &[lvl(9950, 1000)], &[lvl(10000, 10)]));
        let mut out1: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, huge_ts, &mut out1);

        // Quote persists through to past the window.
        let later_ts = huge_ts + 600_000_000; // 600 ms — past the 500 ms cap
        let mut out2: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, later_ts, &mut out2);
        let spoofs: Vec<_> = out2
            .as_slice()
            .iter()
            .filter(|e| matches!(e, OrderflowEvent::Spoofing { .. }))
            .collect();
        assert!(spoofs.is_empty(), "should not flag a resting order");
    }

    #[test]
    fn no_spoofing_when_quote_fills() {
        let mut det = Detector::new();
        let mut book = LiveBook::new();
        for i in 0..30u64 {
            book.apply(&make_book(i + 1, &[lvl(9900, 10)], &[lvl(10000, 10)]));
            let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
            det.observe_book(&book, (i + 1) * 1_000_000, &mut out);
        }
        let huge_ts = 1_000_000_000u64;
        book.apply(&make_book(31, &[lvl(9950, 1000)], &[lvl(10000, 10)]));
        let mut out1: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, huge_ts, &mut out1);

        // Trade against the huge bid for full size.
        let tick = make_tick(9950, 1000);
        let mut tout: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_trade(&tick, &book, &mut tout);

        // After observing the trade the candidate should be classified as
        // filled and removed from pending.
        assert_eq!(det.pending_len(), 0);

        // Even if the quote disappears within the window, no spoof is
        // emitted because the candidate was already removed via fill.
        let cancel_ts = huge_ts + 100_000_000;
        book.apply(&make_book(32, &[lvl(9900, 10)], &[lvl(10000, 10)]));
        let mut out2: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
        det.observe_book(&book, cancel_ts, &mut out2);
        let spoofs: Vec<_> = out2
            .as_slice()
            .iter()
            .filter(|e| matches!(e, OrderflowEvent::Spoofing { .. }))
            .collect();
        assert!(spoofs.is_empty());
    }

    #[test]
    fn pending_capacity_does_not_exceed_array_cap() {
        // Try to register far more than 32 pending candidates and confirm
        // the ArrayVec cap is enforced.
        let mut det = Detector::new();
        let mut book = LiveBook::new();
        for i in 0..30u64 {
            book.apply(&make_book(i + 1, &[lvl(9900, 1)], &[lvl(10000, 1)]));
            let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
            det.observe_book(&book, (i + 1) * 1_000_000, &mut out);
        }
        for j in 0..200u64 {
            book.apply(&make_book(
                100 + j,
                &[lvl(9000 + j as i64, 1000)],
                &[lvl(10000, 1)],
            ));
            let mut out: BoundedEvents<OrderflowEvent, 4> = BoundedEvents::new();
            det.observe_book(&book, 1_000_000_000 + j * 10_000_000, &mut out);
        }
        assert!(det.pending_len() <= SPOOF_PENDING_CAPACITY);
    }
}
