//! Per-symbol Feature_Extraction_Engine state.
//!
//! [`FeatureState`] holds **every** rolling buffer needed by the
//! incremental indicator modules under [`crate::incremental`]. There is
//! exactly one `FeatureState` per [`SymbolId`] in the Hot_Path; the
//! `FeatureExtractionEngine` owns them inside
//! `DashMap<SymbolId, parking_lot::Mutex<FeatureState>>` and locks the
//! per-symbol cell on each tick (design § Components § Feature_Extraction_Engine).
//!
//! ## Buffer sizing
//!
//! The constants below are part of the on-wire contract: they decide how
//! many ticks each indicator must observe before [`is_ready`] returns
//! `true`. They MUST match the design's documented windows:
//!
//! | Indicator         | Window | Constant                  |
//! |-------------------|--------|---------------------------|
//! | ATR               | 14     | [`ATR_WINDOW`]            |
//! | EMA fast          | 9      | [`EMA_FAST_PERIOD`]       |
//! | EMA slow          | 21     | [`EMA_SLOW_PERIOD`]       |
//! | EMA slope lookback| 5      | [`EMA_SLOPE_LOOKBACK`]    |
//! | Momentum          | 10     | [`MOMENTUM_WINDOW`]       |
//! | Realized Vol      | 30     | [`VOLATILITY_WINDOW`]     |
//! | Compression       | 20     | [`COMPRESSION_WINDOW`]    |
//! | Liquidity Sweep   | 3      | [`SWEEP_LOOKAHEAD`]       |
//! | Rolling Delta     | 30 s   | [`ROLLING_DELTA_WINDOW_NS`] |
//!
//! ## No-allocation property
//!
//! Every buffer is either a primitive scalar or a
//! [`RingWindow<T, N>`](hedge_core::RingWindow). `RingWindow` stores its
//! `N` slots inline as `[T; N]`, so [`FeatureState::default`] performs no
//! heap allocation and `update_*` operations are O(1) and allocation-free
//! (R3.4, R30.8).
//!
//! ## VWAP cumulative numerator/denominator
//!
//! VWAP is `Σ(price · volume) / Σ(volume)` since session open. Two `i128`
//! accumulators absorb the wraparound headroom even under a full session
//! of `i64::MAX` paise × `u64::MAX` qty pathological inputs; in practice
//! a single trading session never gets close, so the headroom is purely
//! defensive.

use hedge_core::RingWindow;

/// ATR window (R3.1: ATR(14)). Used by [`crate::incremental::atr`].
pub const ATR_WINDOW: usize = 14;

/// EMA fast period (R3.1: EMA fast = 9). Used by [`crate::incremental::ema`].
pub const EMA_FAST_PERIOD: usize = 9;

/// EMA slow period (R3.1: EMA slow = 21). Used by [`crate::incremental::ema`].
pub const EMA_SLOW_PERIOD: usize = 21;

/// EMA trend period (V2: EMA = 5000 for tick data). Used by [`crate::incremental::ema`].
pub const EMA_TREND_PERIOD: usize = 5000;

/// RSI period (scaled for tick data).
pub const RSI_WINDOW: usize = 1400;

/// ADX period (scaled for tick data).
pub const ADX_WINDOW: usize = 1400;

/// Donchian channel period (scaled for tick data).
pub const DONCHIAN_WINDOW: usize = 2000;

/// EMA slope lookback in samples (design: slope over the last 5 EMA samples).
pub const EMA_SLOPE_LOOKBACK: usize = 5;

/// Momentum lookback in ticks (R3.1: momentum over 10 ticks).
pub const MOMENTUM_WINDOW: usize = 10;

/// Realized volatility window in ticks (design: stdev of log returns over 30
/// ticks).
pub const VOLATILITY_WINDOW: usize = 30;

/// Compression-zone evaluation window in ticks (design: `range / atr < 0.5`
/// over 20 ticks).
pub const COMPRESSION_WINDOW: usize = 20;

/// Liquidity-sweep look-ahead in ticks (design: tag new high/low then reverse
/// within 3 ticks).
pub const SWEEP_LOOKAHEAD: usize = 3;

/// Rolling-delta window expressed in nanoseconds (design: 30 s window of
/// signed trade volume). Stored as a const so the rolling-delta module can
/// reference one canonical value.
pub const ROLLING_DELTA_WINDOW_NS: u64 = 30 * 1_000_000_000;

/// Capacity of the rolling-delta tick buffer. The window is **time**-bounded
/// (30 s) but we still need a hard upper bound on storage so the inline
/// `RingWindow` does not allocate. NSE/BSE on a hot symbol prints up to ~5
/// trades / second; 30 s × 5 = 150, and we round up generously to 256 to
/// absorb micro-bursts. Trades older than 30 s are evicted lazily on each
/// update.
pub const ROLLING_DELTA_CAPACITY: usize = 256;

/// One sample stored in the rolling-delta ring: timestamp + signed volume.
///
/// Signed volume convention follows the orderflow engine: `+qty` for buyer-
/// initiated, `-qty` for seller-initiated. The sign is determined by the
/// caller (orderflow) and surfaced through the `Tick`-level `total_buy_qty`
/// vs `total_sell_qty` differential during pre-orderflow operation.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaSample {
    /// Monotonic ns timestamp from `Tick.ts_recv_ns`.
    pub ts_ns: u64,
    /// Signed quantity: `+` buyer-initiated, `-` seller-initiated.
    pub signed_qty: i64,
}

/// Last-tick book snapshot used by liquidity-imbalance and sweep modules.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct LastBook {
    /// Best bid price in paise, or `0` if uninitialised.
    pub bid_paise: i64,
    /// Best ask price in paise, or `0` if uninitialised.
    pub ask_paise: i64,
    /// Total visible buy quantity at the top of book.
    pub total_buy_qty: u64,
    /// Total visible sell quantity at the top of book.
    pub total_sell_qty: u64,
}

/// Per-symbol incremental feature state.
///
/// Every field is either a primitive or a `RingWindow<T, N>`; the entire
/// struct lives inline with no heap pointers. The Hot_Path always borrows
/// `&mut FeatureState`. We do **not** derive `Clone` because
/// `RingWindow<T, N>` does not implement `Clone` — `FeatureState` is
/// owned by the per-symbol `parking_lot::Mutex<FeatureState>` cell and
/// never duplicated.
#[derive(Debug)]
pub struct FeatureState {
    // ---- Identification ---------------------------------------------------
    /// How many ticks have been folded in since `clear_session`.
    pub tick_count: u64,
    /// Latest LTP in paise; `0` until the first tick.
    pub last_ltp_paise: i64,
    /// Previous LTP, used for log-return / momentum delta.
    pub prev_ltp_paise: i64,
    /// Monotonic ns timestamp of the most recent tick processed.
    pub last_ts_ns: u64,

    // ---- VWAP -------------------------------------------------------------
    /// Σ (ltp_paise × ltq), in `i128` so a full session of pathological
    /// inputs never overflows.
    pub vwap_num: i128,
    /// Σ (ltq), `u128` to match the denominator domain.
    pub vwap_den: u128,

    // ---- ATR --------------------------------------------------------------
    /// Last 14 true-range values, in paise.
    pub tr_window: RingWindow<i64, ATR_WINDOW>,
    /// "Previous close" used by ATR's true-range formula. Initialised to
    /// `last_ltp_paise` on the first observation.
    pub prev_close_paise: i64,
    /// Per-tick rolling high used for true-range. Reset whenever the
    /// caller signals a new bar; for tick-driven operation we treat each
    /// tick as a 1-tick "bar" (`high == low == ltp`), which still
    /// produces a meaningful ATR — the design pinpoints ATR(14) without
    /// constraining the bar definition.
    pub bar_high_paise: i64,
    /// Per-tick rolling low; pair with [`bar_high_paise`].
    pub bar_low_paise: i64,

    // ---- EMA fast / slow / trend / slope ----------------------------------------
    /// Last EMA(9) value, in paise. `0` until [`crate::incremental::ema`]
    /// has been seeded.
    pub ema_fast_paise: i64,
    /// Last EMA(21) value, in paise.
    pub ema_slow_paise: i64,
    /// Last EMA(50) value, in paise.
    pub ema_trend_paise: i64,
    /// High-precision f64 accumulator for EMA(9) recurrence.
    /// Avoids cumulative rounding drift from per-step i64 round-trips.
    pub ema_fast_acc: f64,
    /// High-precision f64 accumulator for EMA(21) recurrence.
    pub ema_slow_acc: f64,
    /// High-precision f64 accumulator for EMA(50) recurrence.
    pub ema_trend_acc: f64,
    /// Has EMA fast been seeded with at least one observation?
    pub ema_fast_seeded: bool,
    /// Has EMA slow been seeded with at least one observation?
    pub ema_slow_seeded: bool,
    /// Has EMA trend been seeded with at least one observation?
    pub ema_trend_seeded: bool,
    /// Last 5 EMA(fast) samples for the slope computation.
    pub ema_fast_history: RingWindow<i64, EMA_SLOPE_LOOKBACK>,
    
    // ---- RSI --------------------------------------------------------
    pub rsi_avg_gain: f64,
    pub rsi_avg_loss: f64,
    pub rsi_value: f32,

    // ---- ADX --------------------------------------------------------
    pub adx_smoothed_tr: f64,
    pub adx_smoothed_pdm: f64,
    pub adx_smoothed_ndm: f64,
    pub adx_smoothed_dx: f64,
    pub adx_value: f32,

    // ---- Donchian ---------------------------------------------------
    pub donchian_prices: RingWindow<i64, DONCHIAN_WINDOW>,

    // ---- Momentum --------------------------------------------------------
    /// Last 10 LTP samples for the momentum calculation.
    pub momentum_prices: RingWindow<i64, MOMENTUM_WINDOW>,

    // ---- Realized volatility --------------------------------------------
    /// Last 30 log returns (scaled to f64 to avoid f32 catastrophic
    /// cancellation when prices are large and returns small).
    pub log_returns: RingWindow<f64, VOLATILITY_WINDOW>,

    // ---- Rolling delta ---------------------------------------------------
    /// Last `ROLLING_DELTA_CAPACITY` signed-volume samples within the 30 s
    /// window. Older entries are evicted on each update.
    pub delta_samples: RingWindow<DeltaSample, ROLLING_DELTA_CAPACITY>,
    /// Cached sum of `delta_samples.signed_qty` — refreshed on each update.
    pub rolling_delta_cached: i64,

    // ---- Compression / breakout / sweep ---------------------------------
    /// Last 20 LTP samples for the compression-zone calculation.
    pub compression_prices: RingWindow<i64, COMPRESSION_WINDOW>,
    /// Highest LTP seen in the most recent compression window.
    pub session_high_paise: i64,
    /// Lowest LTP seen in the most recent compression window.
    pub session_low_paise: i64,

    // ---- Sweep -----------------------------------------------------------
    /// Tick index at which a new local high was tagged, or `None`.
    pub last_high_break_idx: Option<u64>,
    /// Tick index at which a new local low was tagged, or `None`.
    pub last_low_break_idx: Option<u64>,
    /// LTP at which the new high break occurred.
    pub last_high_break_paise: i64,
    /// LTP at which the new low break occurred.
    pub last_low_break_paise: i64,
    /// `1.0` if a sweep reversal has been detected within the last
    /// [`SWEEP_LOOKAHEAD`] ticks, otherwise `0.0`. Decays each tick.
    pub sweep_signal: f32,

    // ---- Orderflow / book inputs ----------------------------------------
    /// Last book snapshot (mirrored from the most recent tick's
    /// total_buy_qty / total_sell_qty fields and book updates).
    pub last_book: LastBook,
    /// Cached liquidity imbalance from the last update, in `[-1.0, 1.0]`.
    pub liquidity_imbalance_cached: f32,
    /// Orderflow strength forwarded from the Orderflow_Engine
    /// (`of.event.<sym>` payload). Range `[-1.0, 1.0]`. The engine binary
    /// updates this field whenever a fresh `OrderflowSnapshot` is consumed.
    pub orderflow_strength_cached: f32,
}

impl Default for FeatureState {
    fn default() -> Self {
        Self {
            tick_count: 0,
            last_ltp_paise: 0,
            prev_ltp_paise: 0,
            last_ts_ns: 0,
            vwap_num: 0,
            vwap_den: 0,
            tr_window: RingWindow::new(),
            prev_close_paise: 0,
            bar_high_paise: 0,
            bar_low_paise: 0,
            ema_fast_paise: 0,
            ema_slow_paise: 0,
            ema_trend_paise: 0,
            ema_fast_acc: 0.0,
            ema_slow_acc: 0.0,
            ema_trend_acc: 0.0,
            ema_fast_seeded: false,
            ema_slow_seeded: false,
            ema_trend_seeded: false,
            ema_fast_history: RingWindow::new(),
            rsi_avg_gain: 0.0,
            rsi_avg_loss: 0.0,
            rsi_value: 0.0,
            adx_smoothed_tr: 0.0,
            adx_smoothed_pdm: 0.0,
            adx_smoothed_ndm: 0.0,
            adx_smoothed_dx: 0.0,
            adx_value: 0.0,
            donchian_prices: RingWindow::new(),
            momentum_prices: RingWindow::new(),
            log_returns: RingWindow::new(),
            delta_samples: RingWindow::new(),
            rolling_delta_cached: 0,
            compression_prices: RingWindow::new(),
            session_high_paise: 0,
            session_low_paise: 0,
            last_high_break_idx: None,
            last_low_break_idx: None,
            last_high_break_paise: 0,
            last_low_break_paise: 0,
            sweep_signal: 0.0,
            last_book: LastBook::default(),
            liquidity_imbalance_cached: 0.0,
            orderflow_strength_cached: 0.0,
        }
    }
}

impl FeatureState {
    /// Construct a fresh state. Identical to [`Default::default`].
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset every cumulative field. Called by the engine when an
    /// `ops.session.start` event arrives (R15.3, design § Components §
    /// Feature_Extraction_Engine — VWAP since session start).
    ///
    /// This is the primary entry point exercised by the
    /// `vwap_reset_on_session_boundary_clears_cumulative` test in
    /// [`crate::incremental::vwap`].
    #[inline]
    pub fn clear_session(&mut self) {
        // Use `*self = Default::default()` — `RingWindow::clear` is O(1) and
        // we want every accumulator and every history window to drop back to
        // empty in one call. Profile-guided: this path runs once per
        // session, not per tick.
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_is_quiet() {
        let s = FeatureState::default();
        assert_eq!(s.tick_count, 0);
        assert_eq!(s.last_ltp_paise, 0);
        assert_eq!(s.vwap_num, 0);
        assert_eq!(s.vwap_den, 0);
        assert_eq!(s.tr_window.len(), 0);
        assert_eq!(s.momentum_prices.len(), 0);
        assert_eq!(s.log_returns.len(), 0);
        assert_eq!(s.delta_samples.len(), 0);
        assert_eq!(s.rolling_delta_cached, 0);
        assert_eq!(s.compression_prices.len(), 0);
        assert!(s.last_high_break_idx.is_none());
        assert!(s.last_low_break_idx.is_none());
        assert!(!s.ema_fast_seeded);
        assert!(!s.ema_slow_seeded);
    }

    #[test]
    fn clear_session_resets_every_field() {
        let mut s = FeatureState::default();
        s.tick_count = 42;
        s.last_ltp_paise = 100_50;
        s.vwap_num = 999;
        s.vwap_den = 17;
        s.tr_window.push(1);
        s.momentum_prices.push(2);
        s.log_returns.push(3.0);
        s.delta_samples.push(DeltaSample { ts_ns: 1, signed_qty: 5 });
        s.rolling_delta_cached = 5;
        s.compression_prices.push(7);
        s.last_high_break_idx = Some(11);
        s.ema_fast_seeded = true;
        s.ema_slow_seeded = true;
        s.ema_fast_paise = 12_345;
        s.ema_slow_paise = 67_890;

        s.clear_session();

        assert_eq!(s.tick_count, 0);
        assert_eq!(s.last_ltp_paise, 0);
        assert_eq!(s.vwap_num, 0);
        assert_eq!(s.vwap_den, 0);
        assert_eq!(s.tr_window.len(), 0);
        assert_eq!(s.momentum_prices.len(), 0);
        assert_eq!(s.log_returns.len(), 0);
        assert_eq!(s.delta_samples.len(), 0);
        assert_eq!(s.rolling_delta_cached, 0);
        assert_eq!(s.compression_prices.len(), 0);
        assert!(s.last_high_break_idx.is_none());
        assert!(!s.ema_fast_seeded);
        assert!(!s.ema_slow_seeded);
        assert_eq!(s.ema_fast_paise, 0);
        assert_eq!(s.ema_slow_paise, 0);
    }

    #[test]
    fn window_constants_match_design() {
        // Wire-contract assertions: changing these breaks downstream
        // signal-engine strategies that rely on documented windows.
        assert_eq!(ATR_WINDOW, 14);
        assert_eq!(EMA_FAST_PERIOD, 9);
        assert_eq!(EMA_SLOW_PERIOD, 21);
        assert_eq!(EMA_SLOPE_LOOKBACK, 5);
        assert_eq!(MOMENTUM_WINDOW, 10);
        assert_eq!(VOLATILITY_WINDOW, 30);
        assert_eq!(COMPRESSION_WINDOW, 20);
        assert_eq!(SWEEP_LOOKAHEAD, 3);
        assert_eq!(ROLLING_DELTA_WINDOW_NS, 30_000_000_000);
    }
}
