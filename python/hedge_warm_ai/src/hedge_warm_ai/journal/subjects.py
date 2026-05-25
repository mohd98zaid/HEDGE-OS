"""Canonical NATS subjects for the AI_Trade_Journal_Engine (task 27.1).

Subject strings live in exactly one place — this module — so the engine
never embeds raw subject literals in business code. The Hot_Path Rust
side declares matching constants in ``hedge_bus::subject``::

    pub const EXEC_TRADE_CLOSED: &str = "exec.trade.closed";
    pub const AI_JOURNAL_ENTRY:  &str = "ai.journal.entry";

References:
- ``crates/hedge-bus/src/subject.rs`` — single source of truth for the
  Rust side; mirrored here for the Warm_AI_Pipeline.
- Design § Components § AI_Trade_Journal_Engine (R18).
- Design § Data Models — NATS Subject Naming Convention.
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# Subject constants ---------------------------------------------------------
# ---------------------------------------------------------------------------

#: Hot_Path → Warm_AI_Pipeline trigger. Emitted by the Execution_Engine
#: once a round-trip trade has fully closed (a Position aggregator
#: collapses fills until the symbol's signed quantity reaches zero).
#: Mirrors ``hedge_bus::subject::EXEC_TRADE_CLOSED``.
SUBJECT_EXEC_TRADE_CLOSED: Final[str] = "exec.trade.closed"

#: Edge-triggered "post-trade narrative ready" announcement — one event
#: per closed trade per subscriber (Property 10). Producers: the
#: AI_Trade_Journal_Engine. Consumers: UI cockpit, AI_Governance,
#: AI_Shadow_Mode replay analyser, Memory_RAG retrieval pipeline.
#: Mirrors ``hedge_bus::subject::AI_JOURNAL_ENTRY``.
SUBJECT_AI_JOURNAL_ENTRY: Final[str] = "ai.journal.entry"

#: Request-reply subject for one-shot journal queries. The reply
#: payload is a :class:`JournalQueryReply` JSON document. Producers:
#: UI gateway, AI_Governance, retrieval pipeline. Responder: the
#: AI_Trade_Journal_Engine.
#:
#: Note: this subject lives under the ``mem.*`` domain because the
#: payload is *retrieved* from the Memory_RAG_Layer (TimescaleDB +
#: Qdrant). Emission of fresh journal entries uses ``ai.journal.entry``
#: as documented above.
SUBJECT_MEM_JOURNAL_QUERY: Final[str] = "mem.journal.query"


__all__ = [
    "SUBJECT_AI_JOURNAL_ENTRY",
    "SUBJECT_EXEC_TRADE_CLOSED",
    "SUBJECT_MEM_JOURNAL_QUERY",
]
