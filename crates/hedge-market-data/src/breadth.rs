//! Sector and volatility breadth aggregator.
//!
//! Implements R1.7 ("compute and publish sector breadth and volatility
//! breadth metrics on each tick batch") and design § Components §
//! Market_Data_Engine § `BreadthAggregator`.
//!
//! ### Sector breadth
//!
//! Per sector: `(advancers - decliners) / total`, where:
//!
//! * `advancers` = count of symbols whose latest `ltp > prev_close`,
//! * `decliners` = count of symbols whose latest `ltp < prev_close`,
//! * `total` = `advancers + decliners + unchanged`.
//!
//! Symbols whose latest `ltp == prev_close` are counted in `total` but
//! contribute neither to advancers nor decliners. Sectors with no symbols
//! emit a `0.0` breadth (no advancers, no decliners).
//!
//! ### Volatility breadth
//!
//! Rolling **standard deviation of percentage returns** across all tracked
//! symbols over a 1-minute window. The window is implemented as a
//! per-symbol [`RingWindow`] of recent percentage returns; the aggregator
//! computes the population standard deviation across the latest sample
//! from every tracked symbol.
//!
//! ### Batching
//!
//! The aggregator publishes on `md.breadth.sector` and
//! `md.breadth.volatility` whenever **either**:
//!
//! 1. 100 ticks have been ingested since the last publish, **or**
//! 2. 250 ms have elapsed since the last publish,
//!
//! whichever happens first. The thresholds are surfaced as
//! [`BreadthAggregator::with_batch_thresholds`] so tests can exercise
//! the trigger logic without waiting on wall-clock time.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use hedge_core::{RingWindow, SymbolId};
use hedge_schemas::Tick;
use serde::{Deserialize, Serialize};

/// Per-symbol percentage-return window length used by the volatility
/// breadth calculation.
///
/// Set to 60 to roughly approximate a one-minute window when the upstream
/// feed delivers one normalized tick per symbol per second; downstream
/// tests can increase this if they need a longer baseline.
pub const VOLATILITY_WINDOW: usize = 60;

/// Default tick-count batch trigger.
pub const DEFAULT_BATCH_TICKS: u32 = 100;

/// Default elapsed-time batch trigger.
pub const DEFAULT_BATCH_INTERVAL: Duration = Duration::from_millis(250);

/// Sector breadth payload published on `md.breadth.sector` (JSON).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SectorBreadth {
    /// Per-sector breadth in `[-1.0, 1.0]`.
    pub sectors: HashMap<String, f32>,
    /// Monotonic ns timestamp at publish time.
    pub ts_ns: u64,
}

/// Volatility breadth payload published on `md.breadth.volatility` (JSON).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolatilityBreadth {
    /// Population standard deviation of latest pct returns across all
    /// tracked symbols.
    pub stdev: f32,
    /// Number of symbols included in the calculation.
    pub sample_count: u32,
    /// Monotonic ns timestamp at publish time.
    pub ts_ns: u64,
}

/// Result of an [`BreadthAggregator::on_tick`] call. The two payload
/// fields are populated only on a publish boundary; otherwise they are
/// `None`.
#[derive(Debug, Default, Clone)]
pub struct BreadthSnapshot {
    /// Sector breadth payload, populated when a publish boundary fires.
    pub sector: Option<SectorBreadth>,
    /// Volatility breadth payload, populated when a publish boundary fires.
    pub volatility: Option<VolatilityBreadth>,
}

/// Per-symbol state tracked by the aggregator.
struct SymbolState {
    /// Most recent `ltp` in paise.
    last_ltp_paise: i64,
    /// Previous-close in paise (loaded from config / previous-day memory).
    prev_close_paise: i64,
    /// Window of recent percentage returns (last_ltp / prev_ltp - 1).
    /// Used by the volatility-breadth calculation.
    returns: RingWindow<f32, VOLATILITY_WINDOW>,
    /// Previous tick's ltp; used to compute the next pct return.
    prev_ltp_paise: Option<i64>,
}

impl SymbolState {
    fn new(prev_close_paise: i64) -> Self {
        Self {
            last_ltp_paise: 0,
            prev_close_paise,
            returns: RingWindow::new(),
            prev_ltp_paise: None,
        }
    }
}

/// Sector + volatility breadth aggregator.
///
/// Single-threaded by design: each `MarketDataEngine` adapter task feeds
/// one local instance, and the engine publishes the snapshot on the bus.
/// Two adapter tasks therefore each compute their own breadth — that is
/// acceptable because the union of all upstream feeds is the source-of-
/// truth, and downstream consumers care only about the published JSON
/// payload, not the per-task aggregator state.
pub struct BreadthAggregator {
    /// `symbol -> sector` mapping loaded from configuration.
    sectors: HashMap<SymbolId, String>,
    /// Per-symbol state, keyed by SymbolId.
    state: HashMap<SymbolId, SymbolState>,
    /// Per-symbol prev_close map seeded at construction. Persisted so
    /// symbols seen for the first time after construction can still
    /// resolve their prev_close.
    prev_close: HashMap<SymbolId, i64>,
    /// Monotonic tick counter since last publish.
    ticks_since_publish: u32,
    /// Wall-clock anchor for the elapsed-time trigger.
    last_publish: Instant,
    /// Batch trigger thresholds.
    batch_ticks: u32,
    batch_interval: Duration,
}

impl BreadthAggregator {
    /// Construct a new aggregator with the documented default thresholds.
    ///
    /// `sectors` maps each tracked [`SymbolId`] to its sector label
    /// (e.g. `SymbolId(42) -> "Banking"`). Symbols absent from this map
    /// are silently skipped by the sector calculation; volatility breadth
    /// still includes them.
    ///
    /// `prev_close` provides each tracked symbol's previous-day close in
    /// paise. Symbols absent from this map are skipped by the sector
    /// calculation (no comparator).
    pub fn new(
        sectors: HashMap<SymbolId, String>,
        prev_close: HashMap<SymbolId, i64>,
    ) -> Self {
        Self::with_batch_thresholds(
            sectors,
            prev_close,
            DEFAULT_BATCH_TICKS,
            DEFAULT_BATCH_INTERVAL,
        )
    }

    /// Construct an aggregator with custom batch thresholds. Tests use
    /// this entry point so the trigger logic can be exercised
    /// deterministically.
    pub fn with_batch_thresholds(
        sectors: HashMap<SymbolId, String>,
        prev_close: HashMap<SymbolId, i64>,
        batch_ticks: u32,
        batch_interval: Duration,
    ) -> Self {
        Self {
            sectors,
            state: HashMap::new(),
            prev_close,
            ticks_since_publish: 0,
            last_publish: Instant::now(),
            batch_ticks,
            batch_interval,
        }
    }

    /// Number of distinct symbols the aggregator has observed.
    pub fn tracked_symbols(&self) -> usize {
        self.state.len()
    }

    /// Number of ticks accumulated since the last publish boundary.
    pub fn ticks_in_batch(&self) -> u32 {
        self.ticks_since_publish
    }

    fn symbol_state_mut(&mut self, sym: SymbolId) -> &mut SymbolState {
        let prev_close = *self.prev_close.get(&sym).unwrap_or(&0);
        self.state
            .entry(sym)
            .or_insert_with(|| SymbolState::new(prev_close))
    }

    /// Feed one normalized [`Tick`] into the aggregator.
    ///
    /// Returns a [`BreadthSnapshot`] whose payload fields are populated
    /// only when a batch boundary triggers a publish. The engine forwards
    /// the populated payloads onto NATS.
    pub fn on_tick(&mut self, tick: &Tick) -> BreadthSnapshot {
        let sym = SymbolId::new(tick.symbol);
        // Update per-symbol state.
        let s = self.symbol_state_mut(sym);
        if let Some(prev) = s.prev_ltp_paise {
            if prev != 0 {
                let pct = (tick.ltp_paise as f64 - prev as f64) / prev as f64;
                s.returns.push(pct as f32);
            }
        }
        s.last_ltp_paise = tick.ltp_paise;
        s.prev_ltp_paise = Some(tick.ltp_paise);

        self.ticks_since_publish = self.ticks_since_publish.saturating_add(1);

        if self.should_publish() {
            let now_ns = ts_ns();
            let snap = BreadthSnapshot {
                sector: Some(self.compute_sector(now_ns)),
                volatility: Some(self.compute_volatility(now_ns)),
            };
            self.ticks_since_publish = 0;
            self.last_publish = Instant::now();
            snap
        } else {
            BreadthSnapshot::default()
        }
    }

    /// Force a publish snapshot, ignoring the batch thresholds.
    /// Tests use this to compare incremental output against a reference.
    pub fn snapshot(&mut self) -> BreadthSnapshot {
        let now_ns = ts_ns();
        BreadthSnapshot {
            sector: Some(self.compute_sector(now_ns)),
            volatility: Some(self.compute_volatility(now_ns)),
        }
    }

    fn should_publish(&self) -> bool {
        self.ticks_since_publish >= self.batch_ticks
            || self.last_publish.elapsed() >= self.batch_interval
    }

    fn compute_sector(&self, now_ns: u64) -> SectorBreadth {
        // (advancers, decliners, total) per sector.
        let mut counts: HashMap<&str, (u32, u32, u32)> = HashMap::new();
        for (sym, state) in &self.state {
            let Some(sector) = self.sectors.get(sym) else { continue };
            let prev = state.prev_close_paise;
            if prev == 0 {
                // No comparator — exclude from sector calculation.
                continue;
            }
            let entry = counts.entry(sector.as_str()).or_default();
            entry.2 += 1;
            match state.last_ltp_paise.cmp(&prev) {
                std::cmp::Ordering::Greater => entry.0 += 1,
                std::cmp::Ordering::Less => entry.1 += 1,
                _ => {}
            }
        }

        let mut sectors: HashMap<String, f32> = HashMap::with_capacity(counts.len());
        for (name, (adv, dec, total)) in counts {
            let breadth = if total == 0 {
                0.0
            } else {
                (adv as f32 - dec as f32) / total as f32
            };
            sectors.insert(name.to_string(), breadth);
        }
        SectorBreadth { sectors, ts_ns: now_ns }
    }

    fn compute_volatility(&self, now_ns: u64) -> VolatilityBreadth {
        // Pool every percentage return across every tracked symbol's window.
        // The population standard deviation across this pool is the
        // "volatility breadth" metric.
        let mut sum: f64 = 0.0;
        let mut sum_sq: f64 = 0.0;
        let mut n: u64 = 0;
        for state in self.state.values() {
            for v in state.returns.iter() {
                let x = *v as f64;
                sum += x;
                sum_sq += x * x;
                n += 1;
            }
        }
        let stdev = if n >= 2 {
            let mean = sum / n as f64;
            let variance = (sum_sq / n as f64) - (mean * mean);
            variance.max(0.0).sqrt() as f32
        } else {
            0.0
        };
        VolatilityBreadth {
            stdev,
            sample_count: n.min(u32::MAX as u64) as u32,
            ts_ns: now_ns,
        }
    }
}

/// Helper that returns the current [`hedge_core::now_ns`]. Wrapped here so
/// the `chrono` import in the breadth payloads stays the only timestamp
/// dependency in the module.
fn ts_ns() -> u64 {
    hedge_core::now_ns()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn make_tick(symbol: u32, ltp_paise: i64) -> Tick {
        Tick {
            correlation_id: [0u8; 16],
            symbol,
            exchange: 0,
            ltp_paise,
            bid_paise: ltp_paise - 50,
            ask_paise: ltp_paise + 50,
            ltq: 1,
            total_buy_qty: 0,
            total_sell_qty: 0,
            ts_exchange_ns: 0,
            ts_recv_ns: 0,
        }
    }

    fn fixture() -> BreadthAggregator {
        let mut sectors = HashMap::new();
        sectors.insert(SymbolId::new(1), "Banking".to_string());
        sectors.insert(SymbolId::new(2), "Banking".to_string());
        sectors.insert(SymbolId::new(3), "IT".to_string());
        let mut prev_close = HashMap::new();
        prev_close.insert(SymbolId::new(1), 1_000_00); // ₹1000
        prev_close.insert(SymbolId::new(2), 2_000_00);
        prev_close.insert(SymbolId::new(3), 3_000_00);
        BreadthAggregator::with_batch_thresholds(
            sectors,
            prev_close,
            // High thresholds so on_tick never auto-publishes during test
            // setup; tests call `snapshot()` to read the breadth.
            10_000,
            Duration::from_secs(3600),
        )
    }

    #[test]
    fn sector_breadth_one_advancer_one_decliner_yields_zero() {
        let mut agg = fixture();
        // Banking: advancer (1) + decliner (2)
        agg.on_tick(&make_tick(1, 1_010_00));
        agg.on_tick(&make_tick(2, 1_990_00));
        // IT: advancer (3)
        agg.on_tick(&make_tick(3, 3_010_00));
        let snap = agg.snapshot();
        let sector = snap.sector.expect("sector");
        let banking = sector.sectors.get("Banking").copied().unwrap();
        let it = sector.sectors.get("IT").copied().unwrap();
        assert!((banking - 0.0).abs() < f32::EPSILON, "Banking: {}", banking);
        assert!((it - 1.0).abs() < f32::EPSILON, "IT: {}", it);
    }

    #[test]
    fn sector_breadth_unchanged_counts_in_total_only() {
        let mut agg = fixture();
        // Banking: 1 advancer + 1 unchanged ⇒ (1-0)/2 = 0.5
        agg.on_tick(&make_tick(1, 1_010_00));
        agg.on_tick(&make_tick(2, 2_000_00));
        let snap = agg.snapshot();
        let banking = snap
            .sector
            .unwrap()
            .sectors
            .get("Banking")
            .copied()
            .unwrap();
        assert!((banking - 0.5).abs() < f32::EPSILON, "{}", banking);
    }

    #[test]
    fn sector_breadth_excludes_symbols_with_zero_prev_close() {
        // Symbol 99 has no prev_close entry; it must not appear in sector
        // breadth even though it is tracked.
        let mut sectors = HashMap::new();
        sectors.insert(SymbolId::new(99), "Energy".to_string());
        let prev_close = HashMap::new();
        let mut agg = BreadthAggregator::with_batch_thresholds(
            sectors,
            prev_close,
            10_000,
            Duration::from_secs(3600),
        );
        agg.on_tick(&make_tick(99, 100_00));
        let snap = agg.snapshot();
        assert!(
            !snap.sector.unwrap().sectors.contains_key("Energy"),
            "sector with no prev_close must be excluded"
        );
    }

    #[test]
    fn batch_publishes_after_n_ticks() {
        let mut sectors = HashMap::new();
        sectors.insert(SymbolId::new(1), "Banking".to_string());
        let mut prev_close = HashMap::new();
        prev_close.insert(SymbolId::new(1), 1_000_00);
        let mut agg = BreadthAggregator::with_batch_thresholds(
            sectors,
            prev_close,
            // Trigger every 3 ticks.
            3,
            Duration::from_secs(3600),
        );
        let s1 = agg.on_tick(&make_tick(1, 1_001_00));
        assert!(s1.sector.is_none(), "no publish on tick 1");
        let s2 = agg.on_tick(&make_tick(1, 1_002_00));
        assert!(s2.sector.is_none(), "no publish on tick 2");
        let s3 = agg.on_tick(&make_tick(1, 1_003_00));
        assert!(s3.sector.is_some(), "publish on tick 3");
        assert!(s3.volatility.is_some());
        // Counter resets after publish.
        let s4 = agg.on_tick(&make_tick(1, 1_004_00));
        assert!(s4.sector.is_none(), "no publish immediately after batch");
    }

    #[test]
    fn batch_publishes_after_interval_elapses() {
        let mut sectors = HashMap::new();
        sectors.insert(SymbolId::new(1), "Banking".to_string());
        let mut prev_close = HashMap::new();
        prev_close.insert(SymbolId::new(1), 1_000_00);
        let mut agg = BreadthAggregator::with_batch_thresholds(
            sectors,
            prev_close,
            10_000,
            // Trigger after a tiny interval.
            Duration::from_millis(1),
        );
        // Sleep so the elapsed-time trigger fires on the next tick.
        std::thread::sleep(Duration::from_millis(5));
        let snap = agg.on_tick(&make_tick(1, 1_001_00));
        assert!(snap.sector.is_some(), "interval trigger should fire");
    }

    #[test]
    fn volatility_uses_pct_returns_across_symbols() {
        let mut agg = fixture();
        // Tick stream: identical % return per tick — variance should be ~0.
        // Ltp doubling each tick yields a constant 100% return.
        agg.on_tick(&make_tick(1, 100_00));
        agg.on_tick(&make_tick(1, 200_00)); // +100%
        agg.on_tick(&make_tick(1, 400_00)); // +100%
        agg.on_tick(&make_tick(1, 800_00)); // +100%
        let snap = agg.snapshot();
        let vol = snap.volatility.unwrap();
        assert!(vol.stdev.abs() < 1e-3, "constant returns => stdev≈0, got {}", vol.stdev);
        assert!(vol.sample_count >= 3);
    }

    proptest! {
        /// Property: for any random tick stream, the population standard
        /// deviation reported by the aggregator equals an offline
        /// reference computation of the same pooled returns over the
        /// per-symbol [`VOLATILITY_WINDOW`] window.
        #[test]
        fn volatility_breadth_matches_window_reference(
            // Each entry: (symbol_id 1..=4, ltp_paise 100_00..=10_000_00).
            // Bound the count so proptest cases run in milliseconds.
            stream in proptest::collection::vec(
                (1u32..=4u32, 100_00i64..=10_000_00i64),
                10..=200,
            )
        ) {
            let mut sectors = HashMap::new();
            sectors.insert(SymbolId::new(1), "S".to_string());
            sectors.insert(SymbolId::new(2), "S".to_string());
            sectors.insert(SymbolId::new(3), "S".to_string());
            sectors.insert(SymbolId::new(4), "S".to_string());
            let mut prev_close = HashMap::new();
            prev_close.insert(SymbolId::new(1), 1_000_00);
            prev_close.insert(SymbolId::new(2), 1_000_00);
            prev_close.insert(SymbolId::new(3), 1_000_00);
            prev_close.insert(SymbolId::new(4), 1_000_00);
            let mut agg = BreadthAggregator::with_batch_thresholds(
                sectors,
                prev_close,
                u32::MAX,
                Duration::from_secs(3600),
            );

            // Reference: per-symbol return queues bounded to VOLATILITY_WINDOW
            // so the offline pool matches the aggregator's RingWindow aging.
            use std::collections::VecDeque;
            let mut prev_ltp: HashMap<u32, i64> = HashMap::new();
            let mut per_symbol: HashMap<u32, VecDeque<f32>> = HashMap::new();
            for (sym, ltp) in &stream {
                if let Some(prev) = prev_ltp.get(sym) {
                    if *prev != 0 {
                        let r = (*ltp as f64 - *prev as f64) / *prev as f64;
                        let q = per_symbol.entry(*sym).or_default();
                        if q.len() == VOLATILITY_WINDOW {
                            q.pop_front();
                        }
                        q.push_back(r as f32);
                    }
                }
                prev_ltp.insert(*sym, *ltp);
                agg.on_tick(&make_tick(*sym, *ltp));
            }

            let pooled: Vec<f64> = per_symbol
                .values()
                .flat_map(|q| q.iter().map(|x| *x as f64))
                .collect();

            let agg_snap = agg.snapshot();
            let v = agg_snap.volatility.unwrap();

            let ref_stdev = if pooled.len() >= 2 {
                let n = pooled.len() as f64;
                let mean = pooled.iter().sum::<f64>() / n;
                let var = pooled.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
                var.max(0.0).sqrt() as f32
            } else {
                0.0f32
            };
            let diff = (v.stdev - ref_stdev).abs();
            prop_assert!(
                diff < 1e-3,
                "stdev mismatch: agg={} ref={} diff={}",
                v.stdev, ref_stdev, diff,
            );
            prop_assert_eq!(v.sample_count as usize, pooled.len());
        }
    }
}
