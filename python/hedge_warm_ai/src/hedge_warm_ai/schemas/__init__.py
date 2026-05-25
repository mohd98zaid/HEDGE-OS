"""Warm_AI_Pipeline JSON-schema-mirrored pydantic models.

Every model in this package is a 1:1 mirror of a JSON schema committed
under ``crates/hedge-schemas/json_schemas/`` and re-exported through
``hedge_schemas::json_schemas::*`` in Rust. The two languages share the
same canonical schema files, so they cannot drift.

Models are imported with ``ConfigDict(extra="forbid")`` so unknown fields
raise ``ValidationError`` immediately — matching the
``additionalProperties: false`` constraint on the JSON side.

Each model also carries a ``JSON_SCHEMA`` class-level constant containing
the canonical schema JSON (as ``str``). It is loaded via
``importlib.resources`` from the ``json_schemas/`` data directory so test
generators (``hypothesis-jsonschema``) and JSON-schema validators can both
work off the same source of truth.

Example:
    >>> from hedge_warm_ai.schemas import RankedSignal
    >>> sig = RankedSignal.model_validate({...})
"""

from __future__ import annotations

from .ai_gov_action import AiGovAction
from .ai_journal_entry import AiJournalEntry
from .ai_news_impact import NewsImpact
from .ai_ollama_degraded import OllamaDegraded
from .ai_priority_changed import PriorityChanged
from .ai_psych_intervention import PsychIntervention
from .ai_psych_stability import PsychStability
from .ai_rank import RankedSignal
from .ai_regime_changed import RegimeChanged
from .mem_prev_day import PreviousDayMemory
from .obs_budget_breach import BudgetBreach
from .obs_error import ObsError
from .obs_latency import LatencyRecordJson
from .ops_action import OpsAction
from .ops_session import OpsSession
from .ops_warmode import OpsWarMode
from .trader_intent_killswitch import TraderIntentKillSwitch
from .trader_intent_order import TraderIntentOrder
from .trader_intent_priority import TraderIntentPriority
from .trader_intent_strategy_toggle import TraderIntentStrategyToggle

__all__ = [
    "AiGovAction",
    "AiJournalEntry",
    "BudgetBreach",
    "LatencyRecordJson",
    "NewsImpact",
    "ObsError",
    "OllamaDegraded",
    "OpsAction",
    "OpsSession",
    "OpsWarMode",
    "PreviousDayMemory",
    "PriorityChanged",
    "PsychIntervention",
    "PsychStability",
    "RankedSignal",
    "RegimeChanged",
    "TraderIntentKillSwitch",
    "TraderIntentOrder",
    "TraderIntentPriority",
    "TraderIntentStrategyToggle",
]
