//! Concrete strategy implementations (R4.1).
//!
//! Each module hosts one strategy and exposes a unit struct that implements
//! [`crate::Strategy`]. The strategies are intentionally allocation-free
//! and pure — they consume the [`hedge_schemas::FeatureSnapshot`] and a
//! [`crate::StrategyContext`], then return `Some(Signal)` when their
//! preconditions fire.
//!
//! ### Common helpers
//!
//! [`util`] contains tiny shared helpers used by every strategy
//! (clamping, side direction → `u8`, building a default `RiskProfile`).
//! Keeping the helpers in one place makes Property 4 (formula
//! equivalence) trivially auditable.

pub mod liquidity_sweep_reversal;
pub mod momentum_breakout;
pub mod options_oi_expansion_breakout;
pub mod orb;
pub mod util;
pub mod volatility_compression_breakout;
pub mod vwap_pullback;

pub use liquidity_sweep_reversal::LiquiditySweepReversal;
pub use momentum_breakout::MomentumBreakout;
pub use options_oi_expansion_breakout::OptionsOiExpansionBreakout;
pub use orb::OpeningRangeBreakout;
pub use volatility_compression_breakout::VolatilityCompressionBreakout;
pub use vwap_pullback::VwapPullback;
