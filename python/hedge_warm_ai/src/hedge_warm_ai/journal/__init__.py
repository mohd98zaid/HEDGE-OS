"""AI_Trade_Journal_Engine (R18, task 27.1).

This subpackage implements the Warm_AI_Pipeline component that turns
every closed trade into a post-trade narrative and persists it through
the Memory_RAG_Layer. The engine:

* Subscribes to ``exec.trade.closed`` (Hot_Path → Warm_AI_Pipeline
  trigger).
* Runs the standard narrative pass over Qwen2.5:14B
  (``OllamaRole.PRIMARY``) and an optional deeper post-mortem over
  DeepSeek-R1 (``OllamaRole.DEEP``) via
  :class:`hedge_warm_ai.ollama_client.OllamaClient.stream_generate`.
* Persists each :class:`hedge_warm_ai.schemas.AiJournalEntry` to:

  - the ``journal_entries`` Timescale hypertable via
    :class:`hedge_memory_rag.timescale.TimescaleWriter.write_journal_entry`;
  - the ``journal_entries`` Qdrant collection via
    :class:`hedge_memory_rag.qdrant.MemoryRagQdrant.upsert`, with the
    narrative embedded by
    :class:`hedge_warm_ai.onnx_runtime.DistilBERTEmbedding`.

* Emits ``ai.journal.entry`` to NATS on every persisted entry.
* Serves ``mem.journal.query`` request-reply for one-shot windowed +
  similarity reads.

Slow-path invariant
-------------------

The trade-closed bus callback **never blocks** on the LLM, Timescale,
or Qdrant. Slow narrative work is dispatched via
:func:`asyncio.create_task` inside :meth:`AiTradeJournalEngine._on_trade_closed`,
so the bus reader returns within microseconds and Property 10
(every-event-delivered-exactly-once) holds.

Public surface
--------------

* :class:`AiTradeJournalEngine`               — orchestrator (R18.1–R18.3).
* :class:`JournalConfig` / :func:`load_journal_config` — config
  resolved from :class:`hedge_warm_ai.config.HedgeConfig` (Ollama
  routing keys + post-mortem trigger + embedding dim).
* :class:`TradeClosedEvent`                  — Warm-side wire model
  for the ``exec.trade.closed`` payload.
* :class:`OllamaNarrativeBuilder` /
  :class:`NarrativeProvider`                  — narrative + post-mortem
  generators; production binding wraps :class:`OllamaClient`.
* :class:`InMemoryJournalBus` /
  :class:`CallableJournalPublisher` /
  :class:`JournalBusPublisher` …               — bus protocols and
  in-process implementations.
* :class:`InMemoryJournalEntryPublisher` /
  :class:`NatsJournalEntryPublisher` /
  :class:`NoopJournalEntryPublisher`          — entry publishers.
* :class:`TimescaleJournalRowSink` /
  :class:`QdrantJournalEmbeddingSink` /
  :class:`DistilBERTEmbeddingAdapter`         — persistence sinks +
  embedder adapter.
* :class:`JournalQueryRequest` /
  :class:`JournalQueryReply`                  — request-reply payloads
  for ``mem.journal.query``.
* :data:`SUBJECT_EXEC_TRADE_CLOSED` /
  :data:`SUBJECT_AI_JOURNAL_ENTRY` /
  :data:`SUBJECT_MEM_JOURNAL_QUERY`           — canonical NATS subjects.

References
----------
- Requirements §18 (R18.1, R18.2, R18.3).
- Design § Components § AI_Trade_Journal_Engine.
- Design § Correctness Properties § Property 5 — Persistence
  Round-Trip; Property 10 — Subscriber Delivery.
"""

from __future__ import annotations

from .bus import (
    CallableJournalPublisher,
    InMemoryJournalBus,
    JournalBusPublisher,
    JournalBusSubscriber,
    JournalPublishCallable,
    JournalReplyHandler,
    JournalRequestReplyServer,
    JournalSubscribeCallable,
)
from .config import JournalConfig, load_journal_config
from .engine import AiTradeJournalEngine, JournalEngineError
from .narrative import (
    NarrativeProvider,
    OllamaNarrativeBuilder,
    build_narrative_prompt,
    build_postmortem_prompt,
    build_trade_context,
)
from .persistence import (
    DistilBERTEmbeddingAdapter,
    JournalEmbeddingSink,
    JournalRowSink,
    NoopJournalEmbeddingSink,
    NoopJournalRowSink,
    QdrantJournalEmbeddingSink,
    TimescaleJournalRowSink,
    journal_entry_to_row,
    journal_point_id,
)
from .publisher import (
    InMemoryJournalEntryPublisher,
    JournalEntryPublisher,
    NatsJournalEntryPublisher,
    NoopJournalEntryPublisher,
)
from .query import (
    DEFAULT_KNN_K,
    DEFAULT_QUERY_LIMIT,
    JournalQueryError,
    JournalQueryHit,
    JournalQueryReply,
    JournalQueryRequest,
    hit_from_qdrant_payload,
    row_to_entry,
)
from .state import (
    EmotionalSnapshot,
    ExecutionQuality,
    OrderSide,
    RegimeName,
    TradeClosedEvent,
)
from .subjects import (
    SUBJECT_AI_JOURNAL_ENTRY,
    SUBJECT_EXEC_TRADE_CLOSED,
    SUBJECT_MEM_JOURNAL_QUERY,
)

__all__ = [
    # engine
    "AiTradeJournalEngine",
    "JournalEngineError",
    # config
    "JournalConfig",
    "load_journal_config",
    # state
    "EmotionalSnapshot",
    "ExecutionQuality",
    "OrderSide",
    "RegimeName",
    "TradeClosedEvent",
    # narrative
    "NarrativeProvider",
    "OllamaNarrativeBuilder",
    "build_narrative_prompt",
    "build_postmortem_prompt",
    "build_trade_context",
    # bus
    "CallableJournalPublisher",
    "InMemoryJournalBus",
    "JournalBusPublisher",
    "JournalBusSubscriber",
    "JournalPublishCallable",
    "JournalReplyHandler",
    "JournalRequestReplyServer",
    "JournalSubscribeCallable",
    # publisher
    "InMemoryJournalEntryPublisher",
    "JournalEntryPublisher",
    "NatsJournalEntryPublisher",
    "NoopJournalEntryPublisher",
    # persistence
    "DistilBERTEmbeddingAdapter",
    "JournalEmbeddingSink",
    "JournalRowSink",
    "NoopJournalEmbeddingSink",
    "NoopJournalRowSink",
    "QdrantJournalEmbeddingSink",
    "TimescaleJournalRowSink",
    "journal_entry_to_row",
    "journal_point_id",
    # query
    "DEFAULT_KNN_K",
    "DEFAULT_QUERY_LIMIT",
    "JournalQueryError",
    "JournalQueryHit",
    "JournalQueryReply",
    "JournalQueryRequest",
    "hit_from_qdrant_payload",
    "row_to_entry",
    # subjects
    "SUBJECT_AI_JOURNAL_ENTRY",
    "SUBJECT_EXEC_TRADE_CLOSED",
    "SUBJECT_MEM_JOURNAL_QUERY",
]
