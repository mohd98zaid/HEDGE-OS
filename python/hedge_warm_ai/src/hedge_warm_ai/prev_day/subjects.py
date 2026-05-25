"""Canonical NATS subjects for the Previous_Day_Memory_Engine (task 24.1).

Subject strings live in exactly one place — this module — so the engine
never embeds raw subject literals in business code. Hot_Path consumers
go through ``hedge-bus``'s typed ``Subject<T>`` wrappers (Rust); the
Warm_AI_Pipeline mirrors them here so the two languages stay in sync.

References:
- ``crates/hedge-bus/src/subject.rs``: ``MEM_PREV_DAY``, ``OPS_SESSION_START``,
  ``OPS_SESSION_END``.
- Design § Data Models — NATS Subject Naming Convention.
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# Subject constants ---------------------------------------------------------
# ---------------------------------------------------------------------------

#: Per-symbol previous-day record subject prefix. Concrete subjects are
#: ``mem.prev_day.<symbol_id>`` and are produced by
#: :func:`mem_prev_day_subject`. Mirrors ``hedge_bus::subject::MEM_PREV_DAY``.
SUBJECT_MEM_PREV_DAY: Final[str] = "mem.prev_day"

#: Request-reply subject for one-shot queries. The reply payload is a
#: :class:`PrevDayQueryReply` (or its JSON form). Producers: Signal_Engine,
#: Risk_Engine, UI. Responder: this engine.
SUBJECT_MEM_PREV_DAY_QUERY: Final[str] = "mem.prev_day.query"

#: Edge-triggered "next-session dataset is persisted and queryable"
#: announcement. Emitted exactly once per ``ops.session.end`` →
#: ``ops.session.start`` window. Consumers may use it as a barrier
#: before issuing the first ``mem.prev_day.query``.
SUBJECT_MEM_PREV_DAY_READY: Final[str] = "mem.prev_day.ready"

#: Session manager start announcement (R31). Consumed for cancellation
#: of any still-running compute job whose deadline has elapsed.
SUBJECT_OPS_SESSION_START: Final[str] = "ops.session.start"

#: Session manager end announcement (R31). Triggers the next-session
#: compute job.
SUBJECT_OPS_SESSION_END: Final[str] = "ops.session.end"


def mem_prev_day_subject(symbol_id: int) -> str:
    """Build the per-symbol ``mem.prev_day.<symbol_id>`` subject.

    Mirrors ``hedge_bus::subject::Subject::mem_prev_day(SymbolId)``.
    Raises ``ValueError`` for negative ids so misuse is loud.
    """
    if symbol_id < 0:
        raise ValueError(f"symbol_id must be >= 0, got {symbol_id!r}")
    return f"{SUBJECT_MEM_PREV_DAY}.{symbol_id}"


__all__ = [
    "SUBJECT_MEM_PREV_DAY",
    "SUBJECT_MEM_PREV_DAY_QUERY",
    "SUBJECT_MEM_PREV_DAY_READY",
    "SUBJECT_OPS_SESSION_END",
    "SUBJECT_OPS_SESSION_START",
    "mem_prev_day_subject",
]
