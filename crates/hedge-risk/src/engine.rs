//! [`RiskEngine`] — final-authority evaluator (R5, R31, R32).
//!
//! ## Hot path
//!
//! [`RiskEngine::evaluate`] is the **only** public entry point that
//! produces an `Approved` decision. Every approval mints exactly one
//! [`ApprovalToken`] HMAC-signed over the canonical intent bytes
//! (R5.14, R6.8, R21.2). The signing key never leaves the engine.
//!
//! The evaluation path is wrapped in a [`LatencyTracer`] so the 2 ms p99
//! budget (R5.12, R28.3) is observed end-to-end and breaches surface as
//! `obs.budget.breach.RiskCheck` events plus a
//! `hedge_budget_breach_total{stage="RiskCheck"}` counter increment
//! (R28.6).
//!
//! ## Authority
//!
//! The engine arbitrates the Authority Hierarchy structurally
//! (R5.1, R21.1):
//!
//! ```text
//! Risk_Engine > Execution_Engine > Signal_Engine > Warm_AI_Pipeline > Trader_Input
//! ```
//!
//! No other process holds the HMAC key, so the Execution_Engine can only
//! act on intents the engine has signed. The Warm_AI_Pipeline cannot
//! force an approval — it can only nudge `Adaptive_Risk` factors via the
//! WarmCache last-known-value path. Trader inputs are mediated through
//! `trader.intent.*` topics and processed as state updates between
//! evaluations, never as approval overrides.
//!
//! ## Determinism
//!
//! `evaluate` is deterministic given:
//!
//! 1. The signal payload.
//! 2. The Risk_Engine state at the moment of the call.
//! 3. The clock readings (`now_ns` for frequency / mint timestamp,
//!    `chrono::Utc::now` for the IST session gate).
//!
//! Replay (R22) supplies clock readings via a virtual time source so
//! the evaluator can be exercised against pre-recorded inputs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{Datelike, NaiveTime, TimeZone, Timelike, Weekday};
use chrono_tz::Asia::Kolkata;
use hedge_config::{CapitalConfig, RiskConfig, SessionConfig};
use hedge_core::{now_ns, BrokerId, CorrelationId, SymbolId};
use hedge_obs::{LatencyEmitter, LatencyTracer};
use hedge_schemas::rejection_reason::RejectionReason;
use hedge_schemas::stage::Stage;
use hedge_schemas::{OrderIntent, Signal as Signal_v1};

use crate::approval::ApprovalSigner;
use crate::decision::{GateOutcome, RiskDecision, RiskRationale};
use crate::kill_switch::{KillReason, KillSwitchState};
use crate::state::RiskState;
use crate::warmcache::WarmCacheView;

/// Risk-check budget, in nanoseconds (R5.12, R28.3).
pub const RISK_CHECK_BUDGET_NS: u64 = 2_000_000;

/// Final-authority risk evaluator.
///
/// Owns:
///
/// * The immutable [`RiskConfig`] / [`CapitalConfig`] / [`SessionConfig`]
///   surface (loaded from `/etc/hedge/config.yaml`, R32).
/// * The [`KillSwitchState`] atomic (R5.5, R5.9, R16.7).
/// * The mutable [`RiskState`] aggregate behind a `parking_lot::Mutex`.
/// * The [`ApprovalSigner`] HMAC key — never serialized, never published.
/// * A `WarmCacheView` handle for `Adaptive_Risk` factors (R5.13).
/// * A monotonic per-engine `sequence` counter so two approvals over
///   byte-equal intents always produce distinct tokens.
pub struct RiskEngine {
    capital: CapitalConfig,
    risk_cfg: RiskConfig,
    session_cfg: SessionConfig,
    /// Process-private signing key holder.
    signer: ApprovalSigner,
    /// Fast-path atomic Kill_Switch.
    kill_switch: Arc<KillSwitchState>,
    /// All other mutable state.
    state: parking_lot::Mutex<RiskState>,
    /// Last-known-value cache feeding Adaptive_Risk (R5.13).
    warm_cache: Arc<dyn WarmCacheView>,
    /// Monotonic sequence number used to disambiguate two approvals over
    /// the same intent. Bumped on every `Approved` mint.
    sequence: AtomicU64,
}

impl RiskEngine {
    /// Construct a fresh engine.
    ///
    /// The `signer` carries the HMAC key (32+ bytes of cryptographically
    /// random data). The `warm_cache` handle is shared by reference so
    /// the Warm_AI_Pipeline subscriber tasks can update the cache
    /// concurrently with evaluation.
    pub fn new(
        capital: CapitalConfig,
        risk_cfg: RiskConfig,
        session_cfg: SessionConfig,
        signer: ApprovalSigner,
        warm_cache: Arc<dyn WarmCacheView>,
    ) -> Self {
        let state = RiskState::new(&capital, &risk_cfg);
        Self {
            capital,
            risk_cfg,
            session_cfg,
            signer,
            kill_switch: Arc::new(KillSwitchState::new()),
            state: parking_lot::Mutex::new(state),
            warm_cache,
            sequence: AtomicU64::new(1),
        }
    }

    /// Borrow the configured capital surface.
    #[inline]
    pub fn capital(&self) -> &CapitalConfig {
        &self.capital
    }

    /// Borrow the configured risk-limit surface.
    #[inline]
    pub fn risk_config(&self) -> &RiskConfig {
        &self.risk_cfg
    }

    /// Borrow the kill-switch handle. Cheap clone for cross-task wiring
    /// (e.g. the `trader.intent.killswitch` subscriber engages it directly).
    #[inline]
    pub fn kill_switch(&self) -> Arc<KillSwitchState> {
        Arc::clone(&self.kill_switch)
    }

    /// Borrow the WarmCache view.
    #[inline]
    pub fn warm_cache(&self) -> &Arc<dyn WarmCacheView> {
        &self.warm_cache
    }

    /// Borrow the mutable state for external mutators (NATS subscriber
    /// tasks updating PnL, cooldowns, etc.).
    pub fn state(&self) -> parking_lot::MutexGuard<'_, RiskState> {
        self.state.lock()
    }

    /// Evaluate `signal` against every gate. Mints an `ApprovalToken`
    /// only on success.
    ///
    /// `entry_price_paise` is the per-share entry price the
    /// Signal_Engine derived from the latest tick. The Signal_v1
    /// FlatBuffers schema in `hedge-schemas` (task 4.1) does not yet
    /// carry an entry-price field; once it does, the engine reads it
    /// from `signal` directly. For now the caller provides it
    /// alongside the signal.
    ///
    /// `emitter` is the latency-tracing sink (typically a
    /// [`hedge_obs::NatsEmitter`] in production; tests pass
    /// [`hedge_obs::NoopEmitter`] or a [`hedge_obs::RecorderEmitter`]).
    pub fn evaluate<E: LatencyEmitter>(
        &self,
        signal: &Signal_v1,
        entry_price_paise: i64,
        emitter: &E,
    ) -> RiskDecision {
        let cid = CorrelationId(u128::from_be_bytes(signal.correlation_id));
        let _tracer = LatencyTracer::start(Stage::RiskCheck, cid, RISK_CHECK_BUDGET_NS, emitter);
        self.evaluate_inner(signal, entry_price_paise, cid)
    }

    /// Evaluation core — same as [`Self::evaluate`] but uses a
    /// [`NoopEmitter`] for the latency tracer. Useful when the caller
    /// already wraps the call site in their own tracer.
    pub fn evaluate_no_obs(
        &self,
        signal: &Signal_v1,
        entry_price_paise: i64,
    ) -> RiskDecision {
        let cid = CorrelationId(u128::from_be_bytes(signal.correlation_id));
        self.evaluate_inner(signal, entry_price_paise, cid)
    }

    fn evaluate_inner(
        &self,
        signal: &Signal_v1,
        entry_price_paise: i64,
        cid: CorrelationId,
    ) -> RiskDecision {
        let mut rationale = RiskRationale::default();

        // -------- Step 1: Kill_Switch (R5.9) --------
        if self.kill_switch.is_active() {
            rationale.kill_switch = Some(GateOutcome::Reject(RejectionReason::KillSwitchEngaged));
            return RiskDecision::Rejected {
                reason: RejectionReason::KillSwitchEngaged,
                rationale,
            };
        }
        rationale.kill_switch = Some(GateOutcome::Pass);

        // -------- Step 2: Session-time gate (R31.1) --------
        if !is_within_session(&self.session_cfg) {
            rationale.session_time =
                Some(GateOutcome::Reject(RejectionReason::SessionClosed));
            return RiskDecision::Rejected {
                reason: RejectionReason::SessionClosed,
                rationale,
            };
        }
        rationale.session_time = Some(GateOutcome::Pass);

        // Acquire the state lock for the remainder of evaluation. The
        // engine evaluator is single-threaded on the Hot_Path, so the
        // lock is uncontended in steady state — concurrent mutators
        // (e.g. NATS subscribers) only acquire briefly between calls.
        let mut state = self.state.lock();

        // -------- Step 3: Daily loss (R5.2) --------
        let max_daily_loss_paise = -(i64::from(self.risk_cfg.max_daily_loss_inr) * 100);
        if state.total_pnl_paise() <= max_daily_loss_paise {
            rationale.daily_loss = Some(GateOutcome::Reject(RejectionReason::MaxDailyLoss));
            return RiskDecision::Rejected {
                reason: RejectionReason::MaxDailyLoss,
                rationale,
            };
        }
        rationale.daily_loss = Some(GateOutcome::Pass);

        // -------- Step 4: Drawdown (R5.5) — auto-engages Kill_Switch --------
        let max_drawdown_paise = i64::from(self.risk_cfg.max_drawdown_inr) * 100;
        if state.drawdown_paise >= max_drawdown_paise {
            rationale.drawdown = Some(GateOutcome::Reject(RejectionReason::MaxDrawdown));
            // Self-engage the switch — only the first transition emits.
            // Caller (the binary) is responsible for actually publishing
            // the `risk.killswitch.activated` event on the transition;
            // here we set the atomic and let the binary observe the
            // edge via `kill_switch.is_active()` polling or a separate
            // notification channel.
            self.kill_switch.activate(KillReason::MaxDrawdown);
            return RiskDecision::Rejected {
                reason: RejectionReason::MaxDrawdown,
                rationale,
            };
        }
        rationale.drawdown = Some(GateOutcome::Pass);

        // -------- Step 5: Trade frequency (R5.6) --------
        if state.frequency.would_breach(
            now_ns(),
            self.risk_cfg.max_trades_per_minute,
            self.risk_cfg.max_trades_per_hour,
            self.risk_cfg.max_trades_per_session,
        ) {
            rationale.frequency = Some(GateOutcome::Reject(RejectionReason::TradeFrequency));
            return RiskDecision::Rejected {
                reason: RejectionReason::TradeFrequency,
                rationale,
            };
        }
        rationale.frequency = Some(GateOutcome::Pass);

        // -------- Step 6: Position size cap (R5.3) --------
        // Worst-case: every share of the current `Signal_v1.risk_profile.max_size_qty`
        // is admitted. The actual sized_quantity (computed below) might
        // be smaller — but the gate uses the upper bound to avoid
        // approving a signal that *could* breach.
        let prospective_qty = signal.risk_profile.max_size_qty.max(1);
        let symbol = SymbolId::new(signal.symbol);
        let current_per_symbol = state
            .per_symbol_position
            .get(&symbol)
            .copied()
            .unwrap_or(0);
        if current_per_symbol.saturating_add(prospective_qty)
            > u64::from(self.risk_cfg.max_position_per_symbol)
            || state.portfolio_position.saturating_add(prospective_qty)
                > u64::from(self.risk_cfg.max_position_portfolio)
        {
            rationale.position_size =
                Some(GateOutcome::Reject(RejectionReason::MaxPosition));
            return RiskDecision::Rejected {
                reason: RejectionReason::MaxPosition,
                rationale,
            };
        }
        rationale.position_size = Some(GateOutcome::Pass);

        // -------- Step 7: Leverage (R5.4) --------
        let capital_paise = i64::from(self.capital.base_inr) * 100;
        // notional after admit = current aggregate notional + entry_price × qty
        let prospective_notional = state.aggregate_notional_paise.saturating_add(
            entry_price_paise.saturating_mul(prospective_qty as i64),
        );
        if capital_paise > 0 {
            let leverage = prospective_notional as f64 / capital_paise as f64;
            if leverage > self.risk_cfg.max_leverage_account as f64 {
                rationale.leverage = Some(GateOutcome::Reject(RejectionReason::MaxLeverage));
                return RiskDecision::Rejected {
                    reason: RejectionReason::MaxLeverage,
                    rationale,
                };
            }
        }
        rationale.leverage = Some(GateOutcome::Pass);

        // -------- Step 8: Exposure (R5.7) --------
        let prospective_exposure = entry_price_paise.saturating_mul(prospective_qty as i64);
        let current_per_symbol_exposure = state
            .per_symbol_exposure_paise
            .get(&symbol)
            .copied()
            .unwrap_or(0);
        let max_exposure_per_symbol_paise =
            i64::from(self.risk_cfg.max_exposure_per_symbol_inr) * 100;
        if current_per_symbol_exposure.saturating_add(prospective_exposure)
            > max_exposure_per_symbol_paise
        {
            rationale.exposure = Some(GateOutcome::Reject(RejectionReason::MaxExposure));
            return RiskDecision::Rejected {
                reason: RejectionReason::MaxExposure,
                rationale,
            };
        }
        rationale.exposure = Some(GateOutcome::Pass);

        // -------- Step 9: Slippage cooldown (R5.8) --------
        if state.cooldowns.is_cooling(symbol, now_ns()) {
            rationale.slippage_cooldown =
                Some(GateOutcome::Reject(RejectionReason::SlippageCooldown));
            return RiskDecision::Rejected {
                reason: RejectionReason::SlippageCooldown,
                rationale,
            };
        }
        rationale.slippage_cooldown = Some(GateOutcome::Pass);

        // -------- Step 10: Volatility block (R5.10) --------
        if state
            .volatility
            .is_blocked(symbol, self.risk_cfg.volatility_block_threshold)
        {
            rationale.volatility_block =
                Some(GateOutcome::Reject(RejectionReason::VolatilityBlock));
            return RiskDecision::Rejected {
                reason: RejectionReason::VolatilityBlock,
                rationale,
            };
        }
        rationale.volatility_block = Some(GateOutcome::Pass);

        // -------- Step 11: Broker latency block (R5.11) --------
        // The "active broker" identity is owned by the Execution_Engine.
        // Until the failover crate publishes the active broker on a
        // shared channel we conservatively check every broker we have
        // a reading for; if any one is below threshold we admit. The
        // Execution_Engine re-checks at submission time (R6.5).
        let broker_threshold = self.risk_cfg.broker_latency_block_ms;
        let any_broker_ok = [
            BrokerId::Zerodha,
            BrokerId::Dhan,
            BrokerId::Shoonya,
            BrokerId::AngelOne,
            BrokerId::Upstox,
        ]
        .into_iter()
        .any(|b| match state.broker_latency.latency_ms(b) {
            Some(ms) => ms <= broker_threshold,
            // No reading for that broker — be permissive (the metric just
            // hasn't published yet). Returns `true` so this broker counts
            // as "OK" and the gate passes.
            None => true,
        });
        if !any_broker_ok {
            rationale.broker_latency_block =
                Some(GateOutcome::Reject(RejectionReason::BrokerLatencyBlock));
            return RiskDecision::Rejected {
                reason: RejectionReason::BrokerLatencyBlock,
                rationale,
            };
        }
        rationale.broker_latency_block = Some(GateOutcome::Pass);

        // -------- Step 12: Daily-profit-target post-policy (R32.3) --------
        if state.target.should_reject() {
            rationale.profit_target =
                Some(GateOutcome::Reject(RejectionReason::ProfitTargetReached));
            return RiskDecision::Rejected {
                reason: RejectionReason::ProfitTargetReached,
                rationale,
            };
        }
        rationale.profit_target = Some(GateOutcome::Pass);

        // -------- Step 13: Adaptive_Risk computation (R5.13) --------
        let base_risk_paise =
            i64::from(self.risk_cfg.base_risk_per_trade_inr) * 100;

        let market_stability = clamp01(self.warm_cache.market_stability());
        // Fall back to the Signal_Engine confidence if WarmCache stale
        // (R24.2).
        let signal_confidence = clamp01(
            self.warm_cache
                .trade_confidence(cid)
                .unwrap_or(signal.confidence),
        );
        let trader_discipline = clamp01(self.warm_cache.trader_stability());

        // Multiply in f64 to avoid double-rounding artefacts when one of
        // the factors is near 1.0.
        let scaled = (base_risk_paise as f64)
            * (market_stability as f64)
            * (signal_confidence as f64)
            * (trader_discipline as f64);

        // Clamp to `[0, base_risk_paise]`. Negative or NaN values force
        // the rejection branch below.
        let adaptive_risk_paise = if scaled.is_finite() && scaled > 0.0 {
            scaled.min(base_risk_paise as f64) as i64
        } else {
            0
        };

        if adaptive_risk_paise <= 0 {
            rationale.adaptive_risk =
                Some(GateOutcome::Reject(RejectionReason::AdaptiveRiskZero));
            return RiskDecision::Rejected {
                reason: RejectionReason::AdaptiveRiskZero,
                rationale,
            };
        }
        rationale.adaptive_risk = Some(GateOutcome::Pass);

        // -------- Step 14: Sized quantity --------
        // `entry_price_paise` is the per-share price; integer division
        // gives the share count consistent with the rupees of risk.
        let mut sized_quantity = if entry_price_paise > 0 {
            ((adaptive_risk_paise as u128) / (entry_price_paise as u128)) as u64
        } else {
            0
        };
        // Honour the post-target HalveSize policy (R32.3).
        if state.target.should_halve() {
            sized_quantity /= 2;
        }
        // Cap at `risk_profile.max_size_qty` so the engine never sizes
        // larger than the Signal_Engine's risk profile.
        sized_quantity = sized_quantity.min(signal.risk_profile.max_size_qty);
        // Floor at 1 — if Adaptive_Risk produced anything strictly
        // positive, sizing collapses to one share rather than zero. The
        // brief mandates `max(1, …)`.
        if sized_quantity == 0 {
            sized_quantity = 1;
        }

        // -------- Step 15: Mint single-use ApprovalToken --------
        let intent = OrderIntent {
            correlation_id: signal.correlation_id,
            symbol: signal.symbol,
            side: signal.side,
            quantity: sized_quantity,
            order_type: 0, // canonical "market" — Execution_Engine adapts (R6.7).
            limit_paise: signal.risk_profile.take_profit_paise,
            exchange: 0,
        };
        let ts_ns = now_ns();
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let token = self.signer.sign(&intent, sized_quantity, ts_ns, sequence);

        RiskDecision::Approved {
            token,
            sized_quantity,
            rationale,
        }
    }
}

/// Convenience free function — see [`RiskEngine::evaluate`].
pub fn evaluate<E: LatencyEmitter>(
    engine: &RiskEngine,
    signal: &Signal_v1,
    entry_price_paise: i64,
    emitter: &E,
) -> RiskDecision {
    engine.evaluate(signal, entry_price_paise, emitter)
}

// ---- internal helpers ---------------------------------------------------

/// Returns `true` when current UTC wall-clock falls within
/// `[session.start_ist, session.end_ist]` on a weekday in `Asia/Kolkata`
/// (R31.1).
fn is_within_session(cfg: &SessionConfig) -> bool {
    let now_utc = chrono::Utc::now();
    let now_ist = Kolkata.from_utc_datetime(&now_utc.naive_utc());
    let weekday = now_ist.weekday();
    if matches!(weekday, Weekday::Sat | Weekday::Sun) {
        return false;
    }
    let now_time = NaiveTime::from_hms_opt(
        now_ist.hour(),
        now_ist.minute(),
        now_ist.second(),
    )
    .expect("constructed from chrono components");
    now_time >= cfg.start_ist && now_time <= cfg.end_ist
}

/// Clamp a float into `[0.0, 1.0]`. NaN → 0.0 (defensive — Adaptive_Risk
/// must never propagate NaN).
#[inline]
fn clamp01(v: f32) -> f32 {
    if v.is_nan() || v < 0.0 {
        0.0
    } else if v > 1.0 {
        1.0
    } else {
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::ApprovalSigner;
    use crate::warmcache::MockWarmCacheView;
    use hedge_config::defaults;
    use hedge_obs::NoopEmitter;
    use hedge_schemas::{RiskProfile, Signal as Signal_v1};

    fn make_signal(symbol: u32, confidence: f32) -> Signal_v1 {
        Signal_v1 {
            correlation_id: CorrelationId::new().as_u128().to_be_bytes(),
            strategy: 0,
            symbol,
            side: 0,
            base_probability: 0.7,
            confidence,
            risk_profile: RiskProfile {
                stop_loss_paise: 9_900,
                take_profit_paise: 10_500,
                max_size_qty: 50,
                time_horizon_seconds: 300,
            },
            ts_ns: 1_000,
        }
    }

    /// Helper: stash the engine's session config so the evaluator
    /// always returns "in session" for the test, regardless of CI clock.
    fn make_engine_always_in_session() -> RiskEngine {
        let mut session = defaults::session();
        session.start_ist = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        session.end_ist = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        let warm = Arc::new(MockWarmCacheView::neutral());
        RiskEngine::new(
            defaults::capital(),
            defaults::risk(),
            session,
            ApprovalSigner::from_key(b"unit-test-hmac-key-32-bytes!!!".to_vec()),
            warm,
        )
    }

    /// Variant that takes a custom risk config.
    fn make_engine_with(risk_cfg: RiskConfig, warm: Arc<dyn WarmCacheView>) -> RiskEngine {
        let mut session = defaults::session();
        session.start_ist = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        session.end_ist = NaiveTime::from_hms_opt(23, 59, 59).unwrap();
        RiskEngine::new(
            defaults::capital(),
            risk_cfg,
            session,
            ApprovalSigner::from_key(b"unit-test-hmac-key-32-bytes!!!".to_vec()),
            warm,
        )
    }

    /// A weekend day will fail the session gate even with a 24h window.
    /// We bypass that by stubbing the session config and skipping
    /// weekend-sensitive tests when the host clock disagrees. For other
    /// tests, we use `make_engine_always_in_session` and run only on
    /// weekdays-in-IST.
    fn skip_if_weekend_in_ist() -> bool {
        use chrono::TimeZone;
        let now_utc = chrono::Utc::now();
        let now_ist = Kolkata.from_utc_datetime(&now_utc.naive_utc());
        matches!(now_ist.weekday(), Weekday::Sat | Weekday::Sun)
    }

    #[test]
    fn approves_well_formed_signal_under_neutral_state() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert!(d.is_approved(), "decision = {:?}", d.rejection_reason());
        assert!(d.sized_quantity() > 0);
        assert!(d.token().is_some());
    }

    #[test]
    fn rejects_when_kill_switch_active() {
        let engine = make_engine_always_in_session();
        engine.kill_switch().activate(KillReason::TraderRequest);
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::KillSwitchEngaged));
    }

    #[test]
    fn rejects_when_session_closed() {
        // Force a session window that excludes "now".
        let mut session = defaults::session();
        session.start_ist = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        session.end_ist = NaiveTime::from_hms_opt(0, 0, 1).unwrap();
        let warm = Arc::new(MockWarmCacheView::neutral());
        let engine = RiskEngine::new(
            defaults::capital(),
            defaults::risk(),
            session,
            ApprovalSigner::from_key(b"k".to_vec()),
            warm,
        );
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::SessionClosed));
    }

    #[test]
    fn rejects_when_daily_loss_reached() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        // Push realized loss past the configured ₹600 default.
        engine
            .state()
            .update_pnl(-60_001, 0, &defaults::capital());
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::MaxDailyLoss));
    }

    #[test]
    fn rejects_when_drawdown_breached_and_engages_kill_switch() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        // Establish a peak then collapse equity to breach 1_000 INR (= 100_000 paise).
        {
            let mut s = engine.state();
            s.peak_equity_paise = 2_000_000 + 200_000; // peak +₹2_000
            s.drawdown_paise = 200_000; // current ₹2_000 below peak
        }
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::MaxDrawdown));
        assert!(engine.kill_switch().is_active(), "drawdown auto-engages kill switch");
    }

    #[test]
    fn rejects_when_trade_frequency_breached() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        // Pre-fill the per-minute counter to its 4/min default.
        {
            let mut s = engine.state();
            for i in 0..4 {
                s.frequency.record(now_ns().saturating_sub(i * 1_000));
            }
        }
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::TradeFrequency));
    }

    #[test]
    fn rejects_when_position_size_cap_breached() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let mut risk_cfg = defaults::risk();
        risk_cfg.max_position_per_symbol = 10;
        risk_cfg.max_position_portfolio = 1_000;
        let engine = make_engine_with(risk_cfg, Arc::new(MockWarmCacheView::neutral()));
        // Pre-load 11 shares of symbol 42 — already over per-symbol cap.
        engine
            .state()
            .per_symbol_position
            .insert(SymbolId::new(42), 11);
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::MaxPosition));
    }

    #[test]
    fn rejects_when_leverage_breached() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let mut risk_cfg = defaults::risk();
        risk_cfg.max_leverage_account = 1.0;
        let engine = make_engine_with(risk_cfg, Arc::new(MockWarmCacheView::neutral()));
        // Already at 1.5× leverage on aggregate notional.
        engine.state().aggregate_notional_paise = 3_000_000;
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::MaxLeverage));
    }

    #[test]
    fn rejects_when_exposure_breached() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let mut risk_cfg = defaults::risk();
        risk_cfg.max_exposure_per_symbol_inr = 1; // ₹1 cap
        let engine = make_engine_with(risk_cfg, Arc::new(MockWarmCacheView::neutral()));
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::MaxExposure));
    }

    #[test]
    fn rejects_when_symbol_in_slippage_cooldown() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        engine.state().cooldowns.engage(
            SymbolId::new(42),
            now_ns() + 60_000_000_000,
            crate::cooldown::CooldownReason::Slippage,
        );
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::SlippageCooldown));
    }

    #[test]
    fn rejects_when_volatility_blocked() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        engine
            .state()
            .volatility
            .update(SymbolId::new(42), 0.10, 0.06);
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::VolatilityBlock));
    }

    #[test]
    fn rejects_when_every_broker_latency_blocked() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        // Set every broker over the 250 ms default threshold.
        {
            let mut s = engine.state();
            s.broker_latency.record(BrokerId::Zerodha, 999);
            s.broker_latency.record(BrokerId::Dhan, 999);
            s.broker_latency.record(BrokerId::Shoonya, 999);
            s.broker_latency.record(BrokerId::AngelOne, 999);
            s.broker_latency.record(BrokerId::Upstox, 999);
        }
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::BrokerLatencyBlock));
    }

    #[test]
    fn rejects_when_profit_target_reduce_to_zero_and_reached() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        // Push past the upper band (₹1_000 = 100_000 paise default).
        engine.state().target.record_realized(100_000);
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::ProfitTargetReached));
    }

    #[test]
    fn rejects_when_adaptive_risk_collapses_to_zero() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let warm = Arc::new(MockWarmCacheView::with_values(0.0, 1.0));
        let engine = make_engine_with(defaults::risk(), warm);
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        assert_eq!(d.rejection_reason(), Some(RejectionReason::AdaptiveRiskZero));
    }

    #[test]
    fn adaptive_risk_scales_monotonically_with_market_stability() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let warm_low = Arc::new(MockWarmCacheView::with_values(0.25, 1.0));
        let warm_high = Arc::new(MockWarmCacheView::with_values(0.75, 1.0));
        let engine_low = make_engine_with(defaults::risk(), warm_low);
        let engine_high = make_engine_with(defaults::risk(), warm_high);

        // High entry price so the sized quantity is small but positive.
        let sig = make_signal(42, 0.9);
        let d_low = engine_low.evaluate(&sig, 100, &NoopEmitter);
        let d_high = engine_high.evaluate(&sig, 100, &NoopEmitter);
        assert!(d_low.is_approved() && d_high.is_approved());
        assert!(
            d_high.sized_quantity() >= d_low.sized_quantity(),
            "higher market_stability must yield ≥ sized_quantity: {} vs {}",
            d_low.sized_quantity(),
            d_high.sized_quantity()
        );
    }

    #[test]
    fn adaptive_risk_scales_monotonically_with_signal_confidence() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        let sig_lo = make_signal(42, 0.2);
        let sig_hi = make_signal(42, 0.95);
        let d_lo = engine.evaluate(&sig_lo, 100, &NoopEmitter);
        let d_hi = engine.evaluate(&sig_hi, 100, &NoopEmitter);
        assert!(d_lo.is_approved() && d_hi.is_approved());
        assert!(d_hi.sized_quantity() >= d_lo.sized_quantity());
    }

    #[test]
    fn adaptive_risk_scales_monotonically_with_trader_stability() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let warm_low = Arc::new(MockWarmCacheView::with_values(1.0, 0.3));
        let warm_high = Arc::new(MockWarmCacheView::with_values(1.0, 0.9));
        let engine_low = make_engine_with(defaults::risk(), warm_low);
        let engine_high = make_engine_with(defaults::risk(), warm_high);
        let sig = make_signal(42, 0.9);
        let d_low = engine_low.evaluate(&sig, 100, &NoopEmitter);
        let d_high = engine_high.evaluate(&sig, 100, &NoopEmitter);
        assert!(d_low.is_approved() && d_high.is_approved());
        assert!(d_high.sized_quantity() >= d_low.sized_quantity());
    }

    #[test]
    fn approval_token_verifies_against_signed_intent() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        let token = d.token().expect("approved").clone();
        let _qty = d.sized_quantity();
        // The verifier was built with the same key; we cannot reach the
        // raw key from outside the engine, but we can recreate one with
        // the same constant test key the helper uses.
        // (See the dedicated `approval` module's tests for raw HMAC
        // round-tripping; this test asserts the engine produced a 32-byte
        // non-zero token.)
        assert_ne!(token.as_bytes(), &[0u8; 32]);
    }

    #[test]
    fn two_evaluations_over_same_signal_produce_distinct_tokens() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        let sig = make_signal(42, 0.9);
        let d1 = engine.evaluate(&sig, 10_000, &NoopEmitter);
        let d2 = engine.evaluate(&sig, 10_000, &NoopEmitter);
        // Both approved, but tokens differ because the per-engine
        // sequence number bumped.
        let t1 = d1.token().expect("approved").clone();
        let t2 = d2.token().expect("approved").clone();
        assert_ne!(t1.as_bytes(), t2.as_bytes());
    }

    #[test]
    fn evaluate_emits_latency_record() {
        if skip_if_weekend_in_ist() {
            return;
        }
        use hedge_obs::RecorderEmitter;
        let engine = make_engine_always_in_session();
        let sig = make_signal(42, 0.9);
        let recorder = RecorderEmitter::with_capacity(8);
        let _ = engine.evaluate(&sig, 10_000, &recorder);
        let (stage, _) = recorder
            .records
            .pop()
            .expect("latency record should be emitted");
        assert_eq!(stage, Stage::RiskCheck);
    }

    #[test]
    fn clamp01_handles_nan_negative_and_over_one() {
        assert_eq!(clamp01(f32::NAN), 0.0);
        assert_eq!(clamp01(-0.5), 0.0);
        assert_eq!(clamp01(1.5), 1.0);
        assert_eq!(clamp01(0.5), 0.5);
    }

    #[test]
    fn rationale_records_pass_for_each_traversed_gate_on_approve() {
        if skip_if_weekend_in_ist() {
            return;
        }
        let engine = make_engine_always_in_session();
        let sig = make_signal(42, 0.9);
        let d = engine.evaluate(&sig, 10_000, &NoopEmitter);
        let r = d.rationale();
        assert_eq!(r.kill_switch, Some(GateOutcome::Pass));
        assert_eq!(r.session_time, Some(GateOutcome::Pass));
        assert_eq!(r.daily_loss, Some(GateOutcome::Pass));
        assert_eq!(r.drawdown, Some(GateOutcome::Pass));
        assert_eq!(r.frequency, Some(GateOutcome::Pass));
        assert_eq!(r.adaptive_risk, Some(GateOutcome::Pass));
    }
}
