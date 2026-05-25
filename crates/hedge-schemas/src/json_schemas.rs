//! Re-exports of every Warm_AI_Pipeline JSON schema as a `&'static str`.
//!
//! The Hot_Path Risk_Engine and the UI gateway validate inbound `ai.*`,
//! `mem.*`, `trader.*`, `ops.*`, and `obs.*` JSON payloads against these
//! constants using `serde_json` plus the workspace `jsonschema` crate.
//!
//! These are also the canonical schemas the Python `pydantic` mirrors load
//! at module import time so the two languages cannot drift.

/// Schema for `ai.rank.<correlation_id>` events emitted by the
/// `AI_Trade_Ranking_Engine`.
pub const AI_RANK_SCHEMA: &str = include_str!("../json_schemas/ai_rank.schema.json");

/// Schema for `ai.news.impact.<symbol>` events emitted by `News_Intelligence`.
pub const AI_NEWS_IMPACT_SCHEMA: &str =
    include_str!("../json_schemas/ai_news_impact.schema.json");

/// Schema for `ai.regime.changed` edge-triggered events.
pub const AI_REGIME_CHANGED_SCHEMA: &str =
    include_str!("../json_schemas/ai_regime_changed.schema.json");

/// Schema for `ai.psych.stability` Trader_Psychology_Engine snapshots.
pub const AI_PSYCH_STABILITY_SCHEMA: &str =
    include_str!("../json_schemas/ai_psych_stability.schema.json");

/// Schema for `ai.psych.intervention` Trader_Psychology_Engine recommendations.
pub const AI_PSYCH_INTERVENTION_SCHEMA: &str =
    include_str!("../json_schemas/ai_psych_intervention.schema.json");

/// Schema for `ai.priority.changed.<symbol>` edge-triggered events.
pub const AI_PRIORITY_CHANGED_SCHEMA: &str =
    include_str!("../json_schemas/ai_priority_changed.schema.json");

/// Schema for `ai.gov.action` AI_Governance interventions.
pub const AI_GOV_ACTION_SCHEMA: &str =
    include_str!("../json_schemas/ai_gov_action.schema.json");

/// Schema for `ai.ollama.degraded` Ollama infrastructure events.
pub const AI_OLLAMA_DEGRADED_SCHEMA: &str =
    include_str!("../json_schemas/ai_ollama_degraded.schema.json");

/// Schema for `ai.journal.entry` AI_Trade_Journal entries.
pub const AI_JOURNAL_ENTRY_SCHEMA: &str =
    include_str!("../json_schemas/ai_journal_entry.schema.json");

/// Schema for `mem.prev_day.<symbol>` Previous_Day_Memory_Engine summaries.
pub const MEM_PREV_DAY_SCHEMA: &str =
    include_str!("../json_schemas/mem_prev_day.schema.json");

/// Schema for `trader.intent.killswitch` UI events.
pub const TRADER_INTENT_KILLSWITCH_SCHEMA: &str =
    include_str!("../json_schemas/trader_intent_killswitch.schema.json");

/// Schema for `trader.intent.strategy_toggle` UI events.
pub const TRADER_INTENT_STRATEGY_TOGGLE_SCHEMA: &str =
    include_str!("../json_schemas/trader_intent_strategy_toggle.schema.json");

/// Schema for `trader.intent.priority` UI events.
pub const TRADER_INTENT_PRIORITY_SCHEMA: &str =
    include_str!("../json_schemas/trader_intent_priority.schema.json");

/// Schema for `trader.intent.order` UI events.
pub const TRADER_INTENT_ORDER_SCHEMA: &str =
    include_str!("../json_schemas/trader_intent_order.schema.json");

/// Schema for `ops.session.<phase>` events.
pub const OPS_SESSION_SCHEMA: &str = include_str!("../json_schemas/ops_session.schema.json");

/// Schema for `ops.warmode.<phase>` events.
pub const OPS_WARMODE_SCHEMA: &str = include_str!("../json_schemas/ops_warmode.schema.json");

/// Schema for `ops.action.<target>` Self_Healing_Supervisor events.
pub const OPS_ACTION_SCHEMA: &str = include_str!("../json_schemas/ops_action.schema.json");

/// Schema for `obs.latency.<stage>` JSON mirror of `LatencyRecord_v1`.
pub const OBS_LATENCY_SCHEMA: &str = include_str!("../json_schemas/obs_latency.schema.json");

/// Schema for `obs.budget.breach.<stage>` budget-breach events.
pub const OBS_BUDGET_BREACH_SCHEMA: &str =
    include_str!("../json_schemas/obs_budget_breach.schema.json");

/// Schema for `obs.error.<source>` typed error events.
pub const OBS_ERROR_SCHEMA: &str = include_str!("../json_schemas/obs_error.schema.json");

/// Catalogue of every JSON schema by canonical name. Useful for batch
/// validation, test generators, and documentation.
pub const ALL_SCHEMAS: &[(&str, &str)] = &[
    ("ai_rank", AI_RANK_SCHEMA),
    ("ai_news_impact", AI_NEWS_IMPACT_SCHEMA),
    ("ai_regime_changed", AI_REGIME_CHANGED_SCHEMA),
    ("ai_psych_stability", AI_PSYCH_STABILITY_SCHEMA),
    ("ai_psych_intervention", AI_PSYCH_INTERVENTION_SCHEMA),
    ("ai_priority_changed", AI_PRIORITY_CHANGED_SCHEMA),
    ("ai_gov_action", AI_GOV_ACTION_SCHEMA),
    ("ai_ollama_degraded", AI_OLLAMA_DEGRADED_SCHEMA),
    ("ai_journal_entry", AI_JOURNAL_ENTRY_SCHEMA),
    ("mem_prev_day", MEM_PREV_DAY_SCHEMA),
    ("trader_intent_killswitch", TRADER_INTENT_KILLSWITCH_SCHEMA),
    ("trader_intent_strategy_toggle", TRADER_INTENT_STRATEGY_TOGGLE_SCHEMA),
    ("trader_intent_priority", TRADER_INTENT_PRIORITY_SCHEMA),
    ("trader_intent_order", TRADER_INTENT_ORDER_SCHEMA),
    ("ops_session", OPS_SESSION_SCHEMA),
    ("ops_warmode", OPS_WARMODE_SCHEMA),
    ("ops_action", OPS_ACTION_SCHEMA),
    ("obs_latency", OBS_LATENCY_SCHEMA),
    ("obs_budget_breach", OBS_BUDGET_BREACH_SCHEMA),
    ("obs_error", OBS_ERROR_SCHEMA),
];
