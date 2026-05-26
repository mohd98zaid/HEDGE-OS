"""Canonical NATS subjects + Redis namespaces consumed by AI_Shadow_Mode.

The shadow service is a *consumer* of two surfaces and a *producer*
of zero new ones:

* **Redis (interim shadow flag)** — the flag namespace
  ``hedge.warm.shadow.<component>`` is owned by the
  AI_Governance_Engine (task 28.1). The shadow service reads it on
  every poll cycle to derive the live "shadowed components" set.
* **NATS** — every Warm_AI_Pipeline emission topic that the UI
  gateway's ``/signals`` channel surfaces (R20, design § Components
  § Human_Control_UI; design § Data Models § WebSocket Channels).
  The shadow service does not subscribe to these directly — the
  upstream engines tag their own emissions ``shadow=True`` after
  consulting :meth:`ShadowModeService.is_shadowed`. The list lives
  here so the UI gateway (task 36.1) can compose the same
  :class:`ShadowFilter` callable across every relevant subject.

The Redis namespace name is re-exported verbatim from
:mod:`hedge_warm_ai.governance.subjects` to guarantee the two
subsystems agree at compile time on the key shape.
"""

from __future__ import annotations

from typing import Final

from ..governance.subjects import (
    DEFAULT_SHADOW_FLAG_NAMESPACE,
    shadow_flag_key,
)

# ---------------------------------------------------------------------------
# Redis namespace (re-exported from the governance subsystem)              --
# ---------------------------------------------------------------------------

#: Per-component shadow-flag Redis namespace. Re-exported from
#: :mod:`hedge_warm_ai.governance.subjects` so the shadow service and
#: the AI_Governance_Engine cannot drift on the key shape.
SHADOW_FLAG_NAMESPACE: Final[str] = DEFAULT_SHADOW_FLAG_NAMESPACE


# ---------------------------------------------------------------------------
# NATS subjects ------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Edge-triggered AI ranking emission. Producer:
#: AI_Trade_Ranking_Engine (task 26.1). Consumers: UI ``/signals``
#: channel (joined with ``sig.emitted`` by ``correlation_id``),
#: Risk_Engine via WarmCache. Shape: ``ai.rank.<correlation_id>``.
SUBJECT_AI_RANK_PREFIX: Final[str] = "ai.rank"

#: Edge-triggered regime change. Producer: Market_Regime_Engine
#: (task 22.1). Consumers: UI, Risk_Engine.
SUBJECT_AI_REGIME_CHANGED: Final[str] = "ai.regime.changed"

#: Trader_Stability_Score snapshot. Producer:
#: Trader_Psychology_Engine (task 25.1). Consumers: UI ``/psych``,
#: Risk_Engine.
SUBJECT_AI_PSYCH_STABILITY: Final[str] = "ai.psych.stability"

#: Per-symbol priority change. Producer: Symbol_Priority_Engine.
#: Consumers: Hot_Path via WarmCache, UI. Shape:
#: ``ai.priority.changed.<symbol>``.
SUBJECT_AI_PRIORITY_CHANGED_PREFIX: Final[str] = "ai.priority.changed"

#: Per-symbol news impact. Producer: News_Intelligence (task 21.1).
#: Consumers: UI ``/news``, Risk_Engine. Shape:
#: ``ai.news.impact.<symbol>``.
SUBJECT_AI_NEWS_IMPACT_PREFIX: Final[str] = "ai.news.impact"

#: Post-trade narrative entry. Producer: AI_Trade_Journal_Engine.
#: Consumers: Memory_RAG_Layer, UI.
SUBJECT_AI_JOURNAL_ENTRY: Final[str] = "ai.journal.entry"

#: Per-symbol previous-day memory. Producer: Previous_Day_Memory.
#: Consumers: UI, Risk_Engine. Shape: ``mem.prev_day.<symbol>``.
SUBJECT_MEM_PREV_DAY_PREFIX: Final[str] = "mem.prev_day"


__all__ = [
    "SHADOW_FLAG_NAMESPACE",
    "SUBJECT_AI_JOURNAL_ENTRY",
    "SUBJECT_AI_NEWS_IMPACT_PREFIX",
    "SUBJECT_AI_PRIORITY_CHANGED_PREFIX",
    "SUBJECT_AI_PSYCH_STABILITY",
    "SUBJECT_AI_RANK_PREFIX",
    "SUBJECT_AI_REGIME_CHANGED",
    "SUBJECT_MEM_PREV_DAY_PREFIX",
    "shadow_flag_key",
]
