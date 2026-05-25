"""PROJECT HEDGE Memory_RAG_Layer package.

Persistence + retrieval layer combining Qdrant (vectors), PostgreSQL +
TimescaleDB (time-series), and Redis (hot cache). Reachable from the
Warm_AI_Pipeline only — the Hot_Path never invokes this package
synchronously (R19.7).

Submodules (each landed by its own task):

* :mod:`hedge_memory_rag.qdrant` — Qdrant vector-store collections,
  writers, readers, and the CBOR embedding codec (R19.1, R19.2,
  task 31.1).
* :mod:`hedge_memory_rag.timescale` — async writers, readers, models,
  and migration runner for the eight Timescale hypertables (R19.1,
  R19.3, task 32.1).
* :mod:`hedge_memory_rag.redis_cache` — Redis bounded-LRU hot cache
  for last-N trades / news per symbol, current regime, and current
  Trader_Stability_Score (R19.1, R19.4, task 33.1).
* :mod:`hedge_memory_rag.retrieval` — five-stage trader-event
  reasoning pipeline composing the three persistence layers above
  with :mod:`hedge_warm_ai.ollama_client` (R19.5, R19.6, R19.7,
  task 34.1). Reachable from the Warm_AI_Pipeline only.

Importing this package does **not** eagerly import every submodule — a
service that only needs the Redis hot cache should not be forced to
pull in :mod:`asyncpg` or :mod:`qdrant_client`. Callers import
:mod:`hedge_memory_rag.redis_cache`, :mod:`hedge_memory_rag.qdrant`,
:mod:`hedge_memory_rag.timescale`, or :mod:`hedge_memory_rag.retrieval`
directly.
"""

__version__ = "0.1.0"

__all__: list[str] = []
