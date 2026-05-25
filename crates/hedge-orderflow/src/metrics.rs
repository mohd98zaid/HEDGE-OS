//! Per-symbol orderflow metrics computed from the live book and the trade
//! tape.
//!
//! Implements the [`OrderflowSnapshot`] structure and its dependent
//! aggregations described in `design.md § Components § Orderflow_Engine`.
//!
//! Formulas (R2.1, R2.5):
//!
//! * `bid_ask_imbalance = (top_bid_qty - top_ask_qty) /
//!     (top_bid_qty + top_ask_qty)` clamped to `[-1.0, 1.0]`. When both
//!   sides are zero the value is `0.0`.
//! * `liquidity_pressure = (Σ top5 bid qty - Σ top5 ask qty) /
//!     (Σ top5 bid qty + Σ top5 ask qty)` clamped to `[-1.0, 1.0]`. Same
//!   zero-divisor handling.
//! * `aggressive_buyer_volume`: cumulative volume of trades whose price
//!   was `>= top_ask` at trade time, over the configured rolling window.
//! * `aggressive_seller_volume`: cumulative volume of trades whose price
//!   was `<= top_bid` at trade time, over the configured rolling window.
//! * `rolling_delta`: signed delta `aggressive_buyer_volume -
//!     aggressive_seller_volume` over a configurable rolling window
//!   (default: 30 s, R2 design § Components).

use hedge_core::{BoundedEvents, RingWindow, SymbolId};

use crate::book::LiveBook;
use crate::events::OrderflowEvent;

/// Number of top levels included in the [`liquidity_pressure`] sum.
pub const LIQUIDITY_PRESSURE_DEPTH: usize = 5;

/// Default rolling delta window size in nanoseconds (30 s).
pub const DEFAULT_ROLLING_DELTA_WINDOW_NS: u64 = 30 * 1_000_000_000;

/// Number of trade-bucket slots in the rolling-delta window. Each slot
/// represents a 100 ms time bucket; 300 slots × 100 ms = 30 s.
pub const ROLLING_DELTA_BUCKETS: usize = 300;

/// One bucket of aggressor volumes covering a fixed time slice.
#[derive(Debug, Clone, Copy, Default)]
struct DeltaBucket {
    bucket_start_ns: u64,
    buyer_volume: u64,
    seller_volume: u64,
}

/// Rolling window of aggressor volumes.
///
/// The window is keyed off monotonic timestamps and uses a fixed-size
/// [`RingWindow`] under the hood (no allocation in steady state, R2.6).
/// Samples older than `window_ns` at observation time are dropped from
/// the running totals via the "outgoing minus incoming" trick — the
/// running totals are recomputed only over the active buckets, which is
/// O(N) over a small fixed N (`ROLLING_DELTA_BUCKETS`).
///
/// Not `Clone` (the underlying [`RingWindow`] is not `Clone`).
#[derive(Debug)]
pub struct RollingDelta {
    window_ns: u64,
    bucket_size_ns: u64,
    buckets: RingWindow<DeltaBucket, ROLLING_DELTA_BUCKETS>,
    /// Latest open bucket. Contributions accumulate here until time advances
    /// past `bucket_start_ns + bucket_size_ns`, at which point the bucket is
    /// pushed into `buckets` and a new open bucket is created.
    open: DeltaBucket,
    open_initialised: bool,
}

impl Default for RollingDelta {
    fn default() -> Self {
        Self::with_window(DEFAULT_ROLLING_DELTA_WINDOW_NS)
    }
}

impl RollingDelta {
    /// Construct a rolling-delta tracker with the given window size in
    /// nanoseconds. The window is divided into [`ROLLING_DELTA_BUCKETS`]
    /// equal-sized buckets.
    pub fn with_window(window_ns: u64) -> Self {
        let bucket_size_ns = (window_ns / ROLLING_DELTA_BUCKETS as u64).max(1);
        Self {
            window_ns,
            bucket_size_ns,
            buckets: RingWindow::new(),
            open: DeltaBucket::default(),
            open_initialised: false,
        }
    }

    /// Record an aggressor trade at `now_ns`.
    pub fn record(&mut self, now_ns: u64, buyer_volume: u64, seller_volume: u64) {
        let bucket_start = (now_ns / self.bucket_size_ns) * self.bucket_size_ns;
        if !self.open_initialised {
            self.open = DeltaBucket {
                bucket_start_ns: bucket_start,
                buyer_volume,
                seller_volume,
            };
            self.open_initialised = true;
            return;
        }
        if bucket_start == self.open.bucket_start_ns {
            self.open.buyer_volume = self.open.buyer_volume.saturating_add(buyer_volume);
            self.open.seller_volume = self.open.seller_volume.saturating_add(seller_volume);
        } else {
            // Roll the open bucket into history and start fresh.
            self.buckets.push(self.open);
            self.open = DeltaBucket {
                bucket_start_ns: bucket_start,
                buyer_volume,
                seller_volume,
            };
        }
    }

    /// Aggressive buyer volume over the active window at `now_ns`.
    pub fn buyer_volume(&self, now_ns: u64) -> u64 {
        let cutoff = now_ns.saturating_sub(self.window_ns);
        let mut total: u128 = 0;
        for b in self.buckets.iter() {
            if b.bucket_start_ns >= cutoff {
                total = total.saturating_add(b.buyer_volume as u128);
            }
        }
        if self.open_initialised && self.open.bucket_start_ns >= cutoff {
            total = total.saturating_add(self.open.buyer_volume as u128);
        }
        total.min(u64::MAX as u128) as u64
    }

    /// Aggressive seller volume over the active window at `now_ns`.
    pub fn seller_volume(&self, now_ns: u64) -> u64 {
        let cutoff = now_ns.saturating_sub(self.window_ns);
        let mut total: u128 = 0;
        for b in self.buckets.iter() {
            if b.bucket_start_ns >= cutoff {
                total = total.saturating_add(b.seller_volume as u128);
            }
        }
        if self.open_initialised && self.open.bucket_start_ns >= cutoff {
            total = total.saturating_add(self.open.seller_volume as u128);
        }
        total.min(u64::MAX as u128) as u64
    }

    /// Signed cumulative delta over the active window. Saturates at the
    /// `i64` bounds so a malicious input cannot panic the engine.
    pub fn signed_delta(&self, now_ns: u64) -> i64 {
        let buyer = self.buyer_volume(now_ns) as i128;
        let seller = self.seller_volume(now_ns) as i128;
        let d = buyer - seller;
        d.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }
}

/// Per-tick / per-book snapshot of the orderflow metrics for a single
/// symbol. Carries up to four detector-emitted events (R2.6).
///
/// In-process structure consumed by the engine. Individual
/// [`OrderflowEvent`] payloads are published on `of.event.<sym>` (JSON
/// codec) one event per NATS message; the snapshot itself stays in-process
/// because [`BoundedEvents`] is intentionally not `Serialize` / `Clone`.
#[derive(Debug)]
pub struct OrderflowSnapshot {
    /// Symbol the snapshot describes.
    pub symbol: SymbolId,
    /// `(top_bid_qty - top_ask_qty) / (top_bid_qty + top_ask_qty)`
    /// clamped to `[-1.0, 1.0]`.
    pub bid_ask_imbalance: f32,
    /// Cumulative volume of aggressive buyer trades over the rolling
    /// window.
    pub aggressive_buyer_volume: u64,
    /// Cumulative volume of aggressive seller trades over the rolling
    /// window.
    pub aggressive_seller_volume: u64,
    /// Signed cumulative delta over the rolling window
    /// (`aggressive_buyer_volume - aggressive_seller_volume`).
    pub rolling_delta: i64,
    /// Top-5 liquidity-pressure score, clamped to `[-1.0, 1.0]`.
    pub liquidity_pressure: f32,
    /// Detector-emitted events, capped at four (R2.6 inline storage).
    pub events: BoundedEvents<OrderflowEvent, 4>,
}

impl OrderflowSnapshot {
    /// Build an empty snapshot for the given symbol. All numeric fields are
    /// zero / `0.0` and the events buffer is empty.
    #[inline]
    pub fn empty(symbol: SymbolId) -> Self {
        Self {
            symbol,
            bid_ask_imbalance: 0.0,
            aggressive_buyer_volume: 0,
            aggressive_seller_volume: 0,
            rolling_delta: 0,
            liquidity_pressure: 0.0,
            events: BoundedEvents::new(),
        }
    }
}

/// Compute `bid_ask_imbalance` for the current top of book.
///
/// Returns `0.0` when both sides are zero. The clamp at `[-1.0, 1.0]` is
/// mathematically guaranteed by the formula but is still applied so a
/// malformed book (e.g. deserialised garbage with negative qty interpreted
/// as a large unsigned number) cannot escape the contract.
#[inline]
pub fn bid_ask_imbalance(book: &LiveBook) -> f32 {
    let bid_qty = book.top_bid().map(|l| l.qty).unwrap_or(0) as f64;
    let ask_qty = book.top_ask().map(|l| l.qty).unwrap_or(0) as f64;
    let total = bid_qty + ask_qty;
    if total <= 0.0 {
        return 0.0;
    }
    let v = (bid_qty - ask_qty) / total;
    (v as f32).clamp(-1.0, 1.0)
}

/// Compute `liquidity_pressure` over the top
/// [`LIQUIDITY_PRESSURE_DEPTH`] levels of each side.
///
/// The formula in the task description has the same algebraic form as
/// `bid_ask_imbalance` but takes summed-depth qty rather than the single
/// top quote: `(Σ_5 bid - Σ_5 ask) / (Σ_5 bid + Σ_5 ask)`. Returns `0.0`
/// when both sums are zero. Result is clamped to `[-1.0, 1.0]`.
#[inline]
pub fn liquidity_pressure(book: &LiveBook) -> f32 {
    let bid_sum: u128 = book
        .bid_levels()
        .iter()
        .take(LIQUIDITY_PRESSURE_DEPTH)
        .map(|l| l.qty as u128)
        .sum();
    let ask_sum: u128 = book
        .ask_levels()
        .iter()
        .take(LIQUIDITY_PRESSURE_DEPTH)
        .map(|l| l.qty as u128)
        .sum();
    let total = bid_sum + ask_sum;
    if total == 0 {
        return 0.0;
    }
    // `as f64` is safe because each side caps at u64::MAX × 5 ≈ 9.2e19,
    // well within f64's 53-bit mantissa range for the relevant magnitudes
    // (book qty rarely exceeds 10^9).
    let num = bid_sum as f64 - ask_sum as f64;
    let den = total as f64;
    let v = num / den;
    (v as f32).clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_schemas::{BookLevel, OrderBook};

    fn lvl(price_paise: i64, qty: u64) -> BookLevel {
        BookLevel {
            price_paise,
            qty,
            orders: 1,
        }
    }

    fn book(bids: &[BookLevel], asks: &[BookLevel]) -> LiveBook {
        let mut b = LiveBook::new();
        b.apply(&OrderBook {
            correlation_id: [0u8; 16],
            symbol: 1,
            exchange: 0,
            bid_levels: bids.to_vec(),
            ask_levels: asks.to_vec(),
            ts_ns: 1,
        });
        b
    }

    #[test]
    fn imbalance_is_zero_for_equal_top_qty() {
        let b = book(&[lvl(100, 50)], &[lvl(101, 50)]);
        assert_eq!(bid_ask_imbalance(&b), 0.0);
    }

    #[test]
    fn imbalance_is_one_when_only_bid_present() {
        let b = book(&[lvl(100, 100)], &[]);
        assert_eq!(bid_ask_imbalance(&b), 1.0);
    }

    #[test]
    fn imbalance_is_minus_one_when_only_ask_present() {
        let b = book(&[], &[lvl(101, 100)]);
        assert_eq!(bid_ask_imbalance(&b), -1.0);
    }

    #[test]
    fn imbalance_is_in_range_for_arbitrary_input() {
        let b = book(&[lvl(100, 7)], &[lvl(101, 13)]);
        let v = bid_ask_imbalance(&b);
        assert!(v >= -1.0 && v <= 1.0, "{v} out of range");
        // (7 - 13) / 20 = -0.3
        assert!((v + 0.3).abs() < 1e-6);
    }

    #[test]
    fn imbalance_zero_for_empty_book() {
        let b = book(&[], &[]);
        assert_eq!(bid_ask_imbalance(&b), 0.0);
    }

    #[test]
    fn liquidity_pressure_zero_for_balanced_top5() {
        let b = book(
            &[
                lvl(100, 10),
                lvl(99, 10),
                lvl(98, 10),
                lvl(97, 10),
                lvl(96, 10),
            ],
            &[
                lvl(101, 10),
                lvl(102, 10),
                lvl(103, 10),
                lvl(104, 10),
                lvl(105, 10),
            ],
        );
        assert!(liquidity_pressure(&b).abs() < 1e-6);
    }

    #[test]
    fn liquidity_pressure_clamped_to_one_for_only_bids() {
        let b = book(&[lvl(100, 10), lvl(99, 10), lvl(98, 10)], &[]);
        assert_eq!(liquidity_pressure(&b), 1.0);
    }

    #[test]
    fn liquidity_pressure_clamped_to_minus_one_for_only_asks() {
        let b = book(&[], &[lvl(101, 10), lvl(102, 10), lvl(103, 10)]);
        assert_eq!(liquidity_pressure(&b), -1.0);
    }

    #[test]
    fn liquidity_pressure_uses_only_top_five_levels() {
        let b = book(
            &[
                lvl(100, 1),
                lvl(99, 1),
                lvl(98, 1),
                lvl(97, 1),
                lvl(96, 1),
                lvl(95, 1_000_000), // beyond depth — should not influence result
            ],
            &[lvl(101, 5)],
        );
        let v = liquidity_pressure(&b);
        // top-5 bid sum = 5, top-5 ask sum = 5 -> 0.0
        assert!(v.abs() < 1e-6, "got {v}");
    }

    #[test]
    fn liquidity_pressure_zero_for_empty_book() {
        let b = book(&[], &[]);
        assert_eq!(liquidity_pressure(&b), 0.0);
    }

    #[test]
    fn rolling_delta_records_and_recovers_volume() {
        let mut rd = RollingDelta::with_window(1_000_000_000); // 1 s window
        rd.record(0, 10, 0);
        rd.record(0, 0, 5);
        assert_eq!(rd.buyer_volume(0), 10);
        assert_eq!(rd.seller_volume(0), 5);
        assert_eq!(rd.signed_delta(0), 5);
    }

    #[test]
    fn rolling_delta_drops_old_buckets() {
        let mut rd = RollingDelta::with_window(1_000_000_000);
        rd.record(0, 10, 0);
        // Advance well past the window so the old bucket falls out.
        rd.record(2_000_000_000, 1, 0);
        let v = rd.buyer_volume(2_000_000_000);
        assert_eq!(v, 1, "old buyer volume should be dropped, got {v}");
    }

    #[test]
    fn rolling_delta_signed_delta_within_i64_bounds() {
        let mut rd = RollingDelta::with_window(1_000_000_000);
        rd.record(0, 10_000, 1_000);
        let d = rd.signed_delta(0);
        assert_eq!(d, 9_000);
    }

    #[test]
    fn empty_snapshot_has_safe_defaults() {
        let s = OrderflowSnapshot::empty(SymbolId::new(7));
        assert_eq!(s.symbol, SymbolId::new(7));
        assert_eq!(s.bid_ask_imbalance, 0.0);
        assert_eq!(s.liquidity_pressure, 0.0);
        assert_eq!(s.aggressive_buyer_volume, 0);
        assert_eq!(s.aggressive_seller_volume, 0);
        assert_eq!(s.rolling_delta, 0);
        assert!(s.events.is_empty());
    }
}
