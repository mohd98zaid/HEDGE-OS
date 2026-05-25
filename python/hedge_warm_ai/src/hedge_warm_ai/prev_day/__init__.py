"""Previous_Day_Memory_Engine (R15, task 24.1).

Persists each tracked symbol's previous Trading_Session structural
data — high, low, close, failed-breakout markers, gap reactions,
delivery volume, trend continuation indicators, institutional
behavior indicators, and significant news reactions — through the
Memory_RAG_Layer (Timescale rows + Qdrant ``market_memory``
embedding-friendly summaries) and exposes both:

* a request-reply API on subject ``mem.prev_day.query`` returning the
  latest record for one or more symbols, and
* a per-symbol subscription on ``mem.prev_day.<symbol_id>`` that fans
  out the canonical schema record on every persistence.

On ``ops.session.end`` the engine schedules an async job that computes
and persists the next-session dataset before the next
``ops.session.start``; on completion it emits ``mem.prev_day.ready``
so the Hot_Path Signal_Engine and Risk_Engine know fresh data is
available.

References:
- Requirements 15.1, 15.2, 15.3
- Design § Components § Previous_Day_Memory_Engine

Public surface::

    from hedge_warm_ai.prev_day import (
        PrevDayBusPublisher,
        PrevDayBusSubscriber,
        PrevDayQueryReply,
        PrevDayQueryRequest,
        PrevDayMemoryEngine,
        PrevDaySessionInputs,
        SymbolSessionData,
        build_prev_day_row,
        build_prev_day_event,
        format_prev_day_summary,
        SUBJECT_MEM_PREV_DAY,
        SUBJECT_MEM_PREV_DAY_QUERY,
        SUBJECT_MEM_PREV_DAY_READY,
        SUBJECT_OPS_SESSION_END,
        SUBJECT_OPS_SESSION_START,
    )
"""

from __future__ import annotations

from .bus import (
    InMemoryPrevDayBus,
    PrevDayBusPublisher,
    PrevDayBusSubscriber,
    PrevDayPublishCallable,
    PrevDayRequestCallable,
    PrevDayRequestReplyServer,
    PrevDaySubscribeCallable,
)
from .compute import (
    PrevDaySessionInputs,
    SymbolSessionData,
    build_prev_day_event,
    build_prev_day_row,
    format_prev_day_summary,
)
from .engine import (
    PrevDayEngineError,
    PrevDayMemoryEngine,
    PrevDayQueryReply,
    PrevDayQueryRequest,
    PrevDayReady,
)
from .subjects import (
    SUBJECT_MEM_PREV_DAY,
    SUBJECT_MEM_PREV_DAY_QUERY,
    SUBJECT_MEM_PREV_DAY_READY,
    SUBJECT_OPS_SESSION_END,
    SUBJECT_OPS_SESSION_START,
    mem_prev_day_subject,
)

__all__ = [
    # Bus
    "InMemoryPrevDayBus",
    "PrevDayBusPublisher",
    "PrevDayBusSubscriber",
    "PrevDayPublishCallable",
    "PrevDayRequestCallable",
    "PrevDayRequestReplyServer",
    "PrevDaySubscribeCallable",
    # Compute
    "PrevDaySessionInputs",
    "SymbolSessionData",
    "build_prev_day_event",
    "build_prev_day_row",
    "format_prev_day_summary",
    # Engine
    "PrevDayEngineError",
    "PrevDayMemoryEngine",
    "PrevDayQueryReply",
    "PrevDayQueryRequest",
    "PrevDayReady",
    # Subjects
    "SUBJECT_MEM_PREV_DAY",
    "SUBJECT_MEM_PREV_DAY_QUERY",
    "SUBJECT_MEM_PREV_DAY_READY",
    "SUBJECT_OPS_SESSION_END",
    "SUBJECT_OPS_SESSION_START",
    "mem_prev_day_subject",
]
