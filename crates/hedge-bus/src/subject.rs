//! Typed NATS subject names.
//!
//! Mirrors the **NATS Subject Naming Convention** table from the design
//! (`<domain>.<entity>.<action_or_id>`, three segments minimum). Every
//! subject in the design is exposed here as either:
//!
//! * a `pub const &str` with the full subject when the subject has no
//!   parameter (e.g. [`SIG_EMITTED`] = `"sig.emitted"`), or
//! * a parameterised constructor on [`Subject`] when the subject's last
//!   segment is dynamic (e.g. `md.tick.<sym>`).
//!
//! Subjects are wrapped in the [`Subject<T>`] newtype so a `Subject<Tick>`
//! can never be passed where a `Subject<RiskApproval>` is expected. The
//! phantom type parameter is informational at this layer — the actual
//! payload type is owned by the [`Codec`](crate::Codec) used to encode and
//! decode it. Concrete typed wrappers (`Subject<Tick_v1>` etc.) bind to the
//! FlatBuffers types from `hedge-schemas` once task 4.1 lands.

use std::fmt;
use std::marker::PhantomData;

use hedge_core::SymbolId;
use serde::{Deserialize, Serialize};

// ---- Well-known subject constants (no parameter segment) ----------------
//
// Names match the design's NATS Subject Naming Convention table verbatim.
// Subjects with a parameter segment (e.g. `md.tick.<sym>`) are exposed as
// **prefix** constants and the full subject is built via [`Subject::md_tick`]
// and similar constructors.

// md.* — Market_Data_Engine
/// Per-symbol tick subject prefix, formatted as `md.tick.<symbol_id>`.
pub const MD_TICK: &str = "md.tick";
/// Per-symbol orderbook update subject prefix, formatted as `md.book.<symbol_id>`.
pub const MD_BOOK: &str = "md.book";
/// Per-symbol open-interest subject prefix, formatted as `md.oi.<symbol_id>`.
pub const MD_OI: &str = "md.oi";
/// Sector-breadth subject (full subject; no parameter).
pub const MD_BREADTH_SECTOR: &str = "md.breadth.sector";
/// Volatility-breadth subject (full subject; no parameter).
pub const MD_BREADTH_VOL: &str = "md.breadth.volatility";
/// Per-source connection-status subject prefix, formatted as `md.connection.<source>`.
pub const MD_CONNECTION: &str = "md.connection";

// of.* — Orderflow_Engine
/// Per-symbol orderflow event prefix, formatted as `of.event.<symbol_id>`.
pub const OF_EVENT: &str = "of.event";
/// Per-symbol orderflow heatmap prefix, formatted as `of.heatmap.<symbol_id>`.
pub const OF_HEATMAP: &str = "of.heatmap";

// feat.* — Feature_Extraction_Engine
/// Per-symbol feature update prefix, formatted as `feat.update.<symbol_id>`.
pub const FEAT_UPDATE: &str = "feat.update";

// sig.* — Signal_Engine
/// Signal-emitted subject (full subject; no parameter).
pub const SIG_EMITTED: &str = "sig.emitted";

// risk.* — Risk_Engine
/// Risk-approved decision subject (full subject; no parameter).
pub const RISK_DECISION_APPROVED: &str = "risk.decision.approved";
/// Risk-rejected decision subject (full subject; no parameter).
pub const RISK_DECISION_REJECTED: &str = "risk.decision.rejected";
/// Kill-switch activation subject (full subject; no parameter).
pub const RISK_KILLSWITCH_ACTIVATED: &str = "risk.killswitch.activated";
/// Daily-profit-target reached subject (full subject; no parameter).
pub const RISK_TARGET_REACHED: &str = "risk.target.reached";
/// Per-symbol risk cooldown subject prefix, formatted as `risk.cooldown.<symbol_id>`.
pub const RISK_COOLDOWN: &str = "risk.cooldown";

// exec.* — Execution_Engine
/// Per-state order lifecycle subject prefix, formatted as `exec.order.<state>`.
pub const EXEC_ORDER: &str = "exec.order";
/// Per-symbol fill subject prefix, formatted as `exec.fill.<symbol_id>`.
pub const EXEC_FILL: &str = "exec.fill";
/// Broker failover event subject (full subject; no parameter).
pub const EXEC_BROKER_FAILOVER: &str = "exec.broker.failover";
/// Trade closed event subject (full subject; no parameter).
pub const EXEC_TRADE_CLOSED: &str = "exec.trade.closed";

// pos.* — Position_Engine
/// Per-symbol position update subject prefix, formatted as `pos.update.<symbol_id>`.
pub const POS_UPDATE: &str = "pos.update";
/// Aggregate trader risk-state subject (full subject; no parameter).
pub const POS_RISK_STATE: &str = "pos.risk_state";

// ai.* — Warm_AI_Pipeline
/// Per-correlation-id ranking subject prefix, formatted as `ai.rank.<cid>`.
pub const AI_RANK: &str = "ai.rank";
/// Per-symbol news-impact subject prefix, formatted as `ai.news.impact.<symbol_id>`.
pub const AI_NEWS_IMPACT: &str = "ai.news.impact";
/// Regime-change event subject (full subject; no parameter).
pub const AI_REGIME_CHANGED: &str = "ai.regime.changed";
/// Trader-stability score event subject (full subject; no parameter).
pub const AI_PSYCH_STABILITY: &str = "ai.psych.stability";
/// Trader-psychology intervention event subject (full subject; no parameter).
pub const AI_PSYCH_INTERVENTION: &str = "ai.psych.intervention";
/// Per-symbol priority change subject prefix, formatted as `ai.priority.changed.<symbol_id>`.
pub const AI_PRIORITY_CHANGED: &str = "ai.priority.changed";
/// AI trade-journal entry subject (full subject; no parameter).
pub const AI_JOURNAL_ENTRY: &str = "ai.journal.entry";
/// AI governance action subject (full subject; no parameter).
pub const AI_GOV_ACTION: &str = "ai.gov.action";
/// Ollama-degraded service event subject (full subject; no parameter).
pub const AI_OLLAMA_DEGRADED: &str = "ai.ollama.degraded";

// mem.* — Previous_Day_Memory_Engine
/// Per-symbol previous-day memory subject prefix, formatted as `mem.prev_day.<symbol_id>`.
pub const MEM_PREV_DAY: &str = "mem.prev_day";

// trader.* — UI gateway → Risk_Engine
/// Trader kill-switch intent subject (full subject; no parameter).
pub const TRADER_INTENT_KILLSWITCH: &str = "trader.intent.killswitch";
/// Trader strategy-toggle intent subject (full subject; no parameter).
pub const TRADER_INTENT_STRATEGY_TOGGLE: &str = "trader.intent.strategy_toggle";
/// Trader priority-change intent subject (full subject; no parameter).
pub const TRADER_INTENT_PRIORITY: &str = "trader.intent.priority";
/// Trader manual-order intent subject (full subject; no parameter).
pub const TRADER_INTENT_ORDER: &str = "trader.intent.order";
/// Trader trading-mode intent subject (live vs paper). Full subject; no
/// parameter. Payload `{ "live": bool }`. The Execution_Engine consumes
/// this to switch between live broker submission and paper mode.
pub const TRADER_INTENT_TRADING_MODE: &str = "trader.intent.trading_mode";

// ops.* — Session manager / Self_Healing_Supervisor
/// Trading-session start event subject (full subject; no parameter).
pub const OPS_SESSION_START: &str = "ops.session.start";
/// Trading-session end event subject (full subject; no parameter).
pub const OPS_SESSION_END: &str = "ops.session.end";
/// War-mode start event subject (full subject; no parameter).
pub const OPS_WARMODE_START: &str = "ops.warmode.start";
/// War-mode end event subject (full subject; no parameter).
pub const OPS_WARMODE_END: &str = "ops.warmode.end";
/// Per-target ops action subject prefix, formatted as `ops.action.<target>`.
pub const OPS_ACTION: &str = "ops.action";

// obs.* — Observability
/// Per-stage latency record subject prefix, formatted as `obs.latency.<stage>`.
pub const OBS_LATENCY: &str = "obs.latency";
/// Per-stage budget-breach event subject prefix, formatted as `obs.budget.breach.<stage>`.
pub const OBS_BUDGET_BREACH: &str = "obs.budget.breach";
/// Per-source error event subject prefix, formatted as `obs.error.<source>`.
pub const OBS_ERROR: &str = "obs.error";

// ---- Subject domains ----------------------------------------------------

/// Coarse-grained domain label for a subject. Mirrors the leftmost segment
/// of the subject name and the row groups in the design's NATS table. Used
/// by the supervisor and the UI gateway to apply per-domain policies (ACLs,
/// retention, dashboard routing).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SubjectDomain {
    /// `md.*` — market data
    MarketData,
    /// `of.*` — orderflow
    Orderflow,
    /// `feat.*` — feature extraction
    Features,
    /// `sig.*` — signals
    Signals,
    /// `risk.*` — risk decisions
    Risk,
    /// `exec.*` — execution lifecycle and fills
    Exec,
    /// `pos.*` — positions
    Positions,
    /// `ai.*` — Warm_AI_Pipeline
    Ai,
    /// `mem.*` — Memory / previous-day
    Memory,
    /// `trader.*` — UI → Risk_Engine intents
    TraderIntent,
    /// `ops.*` — operational events
    Ops,
    /// `obs.*` — observability events
    Observability,
}

impl SubjectDomain {
    /// The leftmost subject segment for this domain (e.g. `md`, `risk`).
    #[inline]
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::MarketData => "md",
            Self::Orderflow => "of",
            Self::Features => "feat",
            Self::Signals => "sig",
            Self::Risk => "risk",
            Self::Exec => "exec",
            Self::Positions => "pos",
            Self::Ai => "ai",
            Self::Memory => "mem",
            Self::TraderIntent => "trader",
            Self::Ops => "ops",
            Self::Observability => "obs",
        }
    }

    /// Best-effort classification of a subject string into its domain. Returns
    /// `None` if the subject does not match any known domain prefix.
    pub fn classify(subject: &str) -> Option<Self> {
        let head = subject.split('.').next()?;
        Some(match head {
            "md" => Self::MarketData,
            "of" => Self::Orderflow,
            "feat" => Self::Features,
            "sig" => Self::Signals,
            "risk" => Self::Risk,
            "exec" => Self::Exec,
            "pos" => Self::Positions,
            "ai" => Self::Ai,
            "mem" => Self::Memory,
            "trader" => Self::TraderIntent,
            "ops" => Self::Ops,
            "obs" => Self::Observability,
            _ => return None,
        })
    }
}

// ---- Subject<T> ---------------------------------------------------------

/// Typed NATS subject name.
///
/// The phantom parameter `T` is informational at this layer — concrete
/// payload types live in `hedge-schemas` (FlatBuffers) and the JSON
/// `ai.*`/`mem.*` schema bindings. Wrapping the subject in a generic newtype
/// lets call sites express:
///
/// ```ignore
/// fn publish_tick(pub: &NatsPublisher<Tick>, s: &Subject<Tick>, t: Tick) -> ...
/// ```
///
/// and rely on the compiler to reject `publish_tick(pub_for_ticks, &book_subject, ...)`.
pub struct Subject<T> {
    name: String,
    // `PhantomData<fn() -> T>` makes `Subject<T>` covariant in `T` and
    // `Send + Sync` regardless of `T`, which matches our intent: the typed
    // payload is a phantom marker, never actually held.
    _marker: PhantomData<fn() -> T>,
}

// Manual `Clone`/`Debug`/`PartialEq`/`Eq`/`Hash` impls so the bounds depend
// only on the subject name, not on `T`. `#[derive(Clone)]` would force a
// spurious `T: Clone` bound even though `PhantomData<fn() -> T>` is `Clone`
// regardless of `T`.
impl<T> Clone for Subject<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
}

// Manual `Debug`/`PartialEq`/`Eq`/`Hash` impls so the bound is on the subject
// name only, not on `T` (which the wire payload type rarely implements
// uniformly).

impl<T> fmt::Debug for Subject<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Subject").field(&self.name).finish()
    }
}

impl<T> fmt::Display for Subject<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

impl<T> PartialEq for Subject<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl<T> Eq for Subject<T> {}

impl<T> std::hash::Hash for Subject<T> {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state)
    }
}

impl<T> Subject<T> {
    /// Construct a typed subject from any string-like value.
    ///
    /// This is the only escape hatch and is the constructor every parameterised
    /// helper below funnels through. It performs no validation beyond the
    /// caller's input — the design's three-segment-minimum rule is enforced
    /// at the helper level.
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }

    /// Borrow the subject as a `&str`. Used by the NATS client to publish.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.name
    }

    /// Consume the typed subject, returning the owned `String`.
    #[inline]
    pub fn into_string(self) -> String {
        self.name
    }

    /// Coerce the phantom payload type. Useful at API boundaries that take a
    /// `Subject<Bytes>` (raw transport) but where the caller already knows
    /// the logical payload type.
    #[inline]
    pub fn cast<U>(self) -> Subject<U> {
        Subject {
            name: self.name,
            _marker: PhantomData,
        }
    }

    /// The [`SubjectDomain`] this subject belongs to, if recognisable.
    #[inline]
    pub fn domain(&self) -> Option<SubjectDomain> {
        SubjectDomain::classify(&self.name)
    }
}

// ---- Parameterised constructors -----------------------------------------
//
// Helpers for every subject in the design that has a `<sym>`/`<state>`/`<source>`
// parameter segment. Each helper builds the full subject string and wraps it
// in a `Subject<T>` so the caller picks the payload type at the call site.

/// Subject helpers. Free functions live here as a thin namespace so call
/// sites can write `subjects::md_tick(symbol)` for readability.
pub mod subjects {
    use super::*;

    // ---- md.* ----

    /// `md.tick.<symbol_id>` — per-symbol tick stream.
    #[inline]
    pub fn md_tick<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", MD_TICK, sym.raw()))
    }

    /// `md.book.<symbol_id>` — per-symbol L2 orderbook stream.
    #[inline]
    pub fn md_book<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", MD_BOOK, sym.raw()))
    }

    /// `md.oi.<symbol_id>` — per-symbol open-interest stream.
    #[inline]
    pub fn md_oi<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", MD_OI, sym.raw()))
    }

    /// `md.connection.<source>` — connection-status events for a named feed.
    /// `source` is a free-form identifier such as `"nse_l1"` or `"bse_l2"`.
    #[inline]
    pub fn md_connection<T>(source: &str) -> Subject<T> {
        Subject::new(format!("{}.{}", MD_CONNECTION, source))
    }

    // ---- of.* ----

    /// `of.event.<symbol_id>` — orderflow event stream.
    #[inline]
    pub fn of_event<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", OF_EVENT, sym.raw()))
    }

    /// `of.heatmap.<symbol_id>` — orderflow heatmap deltas.
    #[inline]
    pub fn of_heatmap<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", OF_HEATMAP, sym.raw()))
    }

    // ---- feat.* ----

    /// `feat.update.<symbol_id>` — per-symbol feature update.
    #[inline]
    pub fn feat_update<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", FEAT_UPDATE, sym.raw()))
    }

    // ---- risk.* ----

    /// `risk.cooldown.<symbol_id>` — per-symbol cooldown event.
    #[inline]
    pub fn risk_cooldown<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", RISK_COOLDOWN, sym.raw()))
    }

    // ---- exec.* ----

    /// `exec.order.<state>` — per-FSM-state order lifecycle event. `state`
    /// must be one of `"submitted"`, `"partial"`, `"filled"`, `"cancelled"`,
    /// `"rejected"`. Validation is deferred to the publisher.
    #[inline]
    pub fn exec_order<T>(state: &str) -> Subject<T> {
        Subject::new(format!("{}.{}", EXEC_ORDER, state))
    }

    /// `exec.fill.<symbol_id>` — per-symbol fill stream.
    #[inline]
    pub fn exec_fill<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", EXEC_FILL, sym.raw()))
    }

    // ---- pos.* ----

    /// `pos.update.<symbol_id>` — per-symbol position update.
    #[inline]
    pub fn pos_update<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", POS_UPDATE, sym.raw()))
    }

    // ---- ai.* ----

    /// `ai.rank.<correlation_id_hex>` — per-correlation ranking response.
    /// The caller passes the canonical hex form of [`hedge_core::CorrelationId`].
    #[inline]
    pub fn ai_rank<T>(correlation_id_hex: &str) -> Subject<T> {
        Subject::new(format!("{}.{}", AI_RANK, correlation_id_hex))
    }

    /// `ai.news.impact.<symbol_id>` — per-symbol news-impact event.
    #[inline]
    pub fn ai_news_impact<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", AI_NEWS_IMPACT, sym.raw()))
    }

    /// `ai.priority.changed.<symbol_id>` — per-symbol priority change.
    #[inline]
    pub fn ai_priority_changed<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", AI_PRIORITY_CHANGED, sym.raw()))
    }

    // ---- mem.* ----

    /// `mem.prev_day.<symbol_id>` — per-symbol previous-day memory record.
    #[inline]
    pub fn mem_prev_day<T>(sym: SymbolId) -> Subject<T> {
        Subject::new(format!("{}.{}", MEM_PREV_DAY, sym.raw()))
    }

    // ---- ops.* ----

    /// `ops.action.<target>` — operational action targeting a named component
    /// (e.g. `"signal_engine"`, `"risk_engine"`).
    #[inline]
    pub fn ops_action<T>(target: &str) -> Subject<T> {
        Subject::new(format!("{}.{}", OPS_ACTION, target))
    }

    // ---- obs.* ----

    /// `obs.latency.<stage>` — per-stage latency record subject. `stage`
    /// values mirror the stages from the design's Latency Budget Allocation
    /// table (`tick_ingest`, `feature_extract`, `risk_check`, ...).
    #[inline]
    pub fn obs_latency<T>(stage: &str) -> Subject<T> {
        Subject::new(format!("{}.{}", OBS_LATENCY, stage))
    }

    /// `obs.budget.breach.<stage>` — per-stage budget-breach event subject.
    #[inline]
    pub fn obs_budget_breach<T>(stage: &str) -> Subject<T> {
        Subject::new(format!("{}.{}", OBS_BUDGET_BREACH, stage))
    }

    /// `obs.error.<source>` — per-source error event subject.
    #[inline]
    pub fn obs_error<T>(source: &str) -> Subject<T> {
        Subject::new(format!("{}.{}", OBS_ERROR, source))
    }
}

// Re-export the namespaced helpers at module level for ergonomic call sites.
// Callers should write `subjects::md_tick(sym)` for readability.

#[cfg(test)]
mod tests {
    use super::*;

    /// Sentinel marker types used only in the tests so we exercise the
    /// `Subject<T>` phantom parameter without depending on `hedge-schemas`.
    struct Tick;
    struct Book;

    #[test]
    fn subject_new_stores_name_verbatim() {
        let s: Subject<Tick> = Subject::new("md.tick.42");
        assert_eq!(s.as_str(), "md.tick.42");
        assert_eq!(format!("{}", s), "md.tick.42");
    }

    #[test]
    fn subject_cast_changes_phantom_only() {
        let t: Subject<Tick> = Subject::new("md.tick.7");
        let b: Subject<Book> = t.cast();
        assert_eq!(b.as_str(), "md.tick.7");
    }

    #[test]
    fn md_tick_helper_uses_symbol_id_raw() {
        let s: Subject<Tick> = subjects::md_tick(SymbolId::new(42));
        assert_eq!(s.as_str(), "md.tick.42");
    }

    #[test]
    fn md_book_md_oi_md_connection_helpers() {
        let book: Subject<Book> = subjects::md_book(SymbolId::new(7));
        let oi: Subject<()> = subjects::md_oi(SymbolId::new(11));
        let conn: Subject<()> = subjects::md_connection("nse_l1");
        assert_eq!(book.as_str(), "md.book.7");
        assert_eq!(oi.as_str(), "md.oi.11");
        assert_eq!(conn.as_str(), "md.connection.nse_l1");
    }

    #[test]
    fn breadth_constants_are_exact() {
        // No parameter — verify the constants haven't drifted from spec.
        assert_eq!(MD_BREADTH_SECTOR, "md.breadth.sector");
        assert_eq!(MD_BREADTH_VOL, "md.breadth.volatility");
    }

    #[test]
    fn signal_and_risk_constants_are_exact() {
        assert_eq!(SIG_EMITTED, "sig.emitted");
        assert_eq!(RISK_DECISION_APPROVED, "risk.decision.approved");
        assert_eq!(RISK_DECISION_REJECTED, "risk.decision.rejected");
        assert_eq!(RISK_KILLSWITCH_ACTIVATED, "risk.killswitch.activated");
        assert_eq!(RISK_TARGET_REACHED, "risk.target.reached");
    }

    #[test]
    fn exec_pos_ai_constants_are_exact() {
        assert_eq!(EXEC_BROKER_FAILOVER, "exec.broker.failover");
        assert_eq!(EXEC_TRADE_CLOSED, "exec.trade.closed");
        assert_eq!(POS_RISK_STATE, "pos.risk_state");
        assert_eq!(AI_REGIME_CHANGED, "ai.regime.changed");
        assert_eq!(AI_PSYCH_STABILITY, "ai.psych.stability");
        assert_eq!(AI_PSYCH_INTERVENTION, "ai.psych.intervention");
        assert_eq!(AI_JOURNAL_ENTRY, "ai.journal.entry");
        assert_eq!(AI_GOV_ACTION, "ai.gov.action");
        assert_eq!(AI_OLLAMA_DEGRADED, "ai.ollama.degraded");
    }

    #[test]
    fn ops_constants_are_exact() {
        assert_eq!(OPS_SESSION_START, "ops.session.start");
        assert_eq!(OPS_SESSION_END, "ops.session.end");
        assert_eq!(OPS_WARMODE_START, "ops.warmode.start");
        assert_eq!(OPS_WARMODE_END, "ops.warmode.end");
    }

    #[test]
    fn trader_intent_constants_are_exact() {
        assert_eq!(TRADER_INTENT_KILLSWITCH, "trader.intent.killswitch");
        assert_eq!(TRADER_INTENT_STRATEGY_TOGGLE, "trader.intent.strategy_toggle");
        assert_eq!(TRADER_INTENT_PRIORITY, "trader.intent.priority");
        assert_eq!(TRADER_INTENT_ORDER, "trader.intent.order");
    }

    #[test]
    fn obs_helpers_format_stage_segment() {
        let lat: Subject<()> = subjects::obs_latency("risk_check");
        let breach: Subject<()> = subjects::obs_budget_breach("risk_check");
        let err: Subject<()> = subjects::obs_error("market_data");
        assert_eq!(lat.as_str(), "obs.latency.risk_check");
        assert_eq!(breach.as_str(), "obs.budget.breach.risk_check");
        assert_eq!(err.as_str(), "obs.error.market_data");
    }

    #[test]
    fn exec_order_state_helper_formats_state_segment() {
        let s: Subject<()> = subjects::exec_order("submitted");
        assert_eq!(s.as_str(), "exec.order.submitted");
        let s2: Subject<()> = subjects::exec_order("filled");
        assert_eq!(s2.as_str(), "exec.order.filled");
    }

    #[test]
    fn ai_rank_helper_uses_correlation_hex() {
        // The CorrelationId is 128-bit — the canonical wire form is hex.
        let s: Subject<()> = subjects::ai_rank("01HJ7VG2YF3T9SP1WX38KN4Y6Z");
        assert_eq!(s.as_str(), "ai.rank.01HJ7VG2YF3T9SP1WX38KN4Y6Z");
    }

    #[test]
    fn subject_domain_classify_recognises_every_known_prefix() {
        let pairs = [
            ("md.tick.1", SubjectDomain::MarketData),
            ("of.event.1", SubjectDomain::Orderflow),
            ("feat.update.1", SubjectDomain::Features),
            ("sig.emitted", SubjectDomain::Signals),
            ("risk.decision.approved", SubjectDomain::Risk),
            ("exec.order.submitted", SubjectDomain::Exec),
            ("pos.update.1", SubjectDomain::Positions),
            ("ai.regime.changed", SubjectDomain::Ai),
            ("mem.prev_day.1", SubjectDomain::Memory),
            ("trader.intent.killswitch", SubjectDomain::TraderIntent),
            ("ops.session.start", SubjectDomain::Ops),
            ("obs.latency.tick_ingest", SubjectDomain::Observability),
        ];
        for (subject, expected) in pairs {
            assert_eq!(
                SubjectDomain::classify(subject),
                Some(expected),
                "wrong classification for `{}`",
                subject
            );
        }
    }

    #[test]
    fn subject_domain_classify_rejects_unknown_prefix() {
        assert_eq!(SubjectDomain::classify("ufo.tick.1"), None);
        assert_eq!(SubjectDomain::classify(""), None);
    }

    #[test]
    fn subject_domain_helper_works_through_subject() {
        let s: Subject<()> = subjects::md_tick(SymbolId::new(1));
        assert_eq!(s.domain(), Some(SubjectDomain::MarketData));
    }

    #[test]
    fn subject_domain_prefix_round_trips() {
        // Every domain's `prefix()` must match what `classify()` accepts.
        for d in [
            SubjectDomain::MarketData,
            SubjectDomain::Orderflow,
            SubjectDomain::Features,
            SubjectDomain::Signals,
            SubjectDomain::Risk,
            SubjectDomain::Exec,
            SubjectDomain::Positions,
            SubjectDomain::Ai,
            SubjectDomain::Memory,
            SubjectDomain::TraderIntent,
            SubjectDomain::Ops,
            SubjectDomain::Observability,
        ] {
            let synthetic = format!("{}.x.y", d.prefix());
            assert_eq!(SubjectDomain::classify(&synthetic), Some(d));
        }
    }

    #[test]
    fn subject_equality_is_by_name_only() {
        let a: Subject<Tick> = Subject::new("sig.emitted");
        let b: Subject<Tick> = Subject::new("sig.emitted");
        let c: Subject<Tick> = Subject::new("sig.other");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
