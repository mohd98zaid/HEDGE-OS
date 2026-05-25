# `hedge_memory_rag.retrieval` — Memory_RAG_Layer retrieval pipeline (task 34.1)

Five-stage async pipeline composed via `await`, mapped onto the
existing Memory_RAG_Layer primitives:

```
trader_event_lookup
    └── memory_retrieval (Qdrant kNN ⊕ Timescale window — parallel)
        └── context_assembly (deterministic prompt, no LLM)
            └── ollama_reasoning (OllamaClient.stream_generate)
                └── recommendation_generation → Recommendation
```

| Stage | Module | Heavy primitive |
|-------|--------|-----------------|
| 1 — `trader_event_lookup` | `trader_event_lookup.py` | `RedisHotCache.recent_trades / recent_news / get_regime / get_stability_score` |
| 2 — `memory_retrieval`    | `memory_retrieval.py`    | `MemoryRagQdrant.knn_search` ‖ `TimescaleReader.read_window_any` |
| 3 — `context_assembly`    | `context_assembly.py`    | (deterministic, pure-Python) |
| 4 — `ollama_reasoning`    | `ollama_reasoning.py`    | `hedge_warm_ai.ollama_client.OllamaClient.stream_generate` |
| 5 — `recommendation_generation` | `recommendation_generation.py` | (deterministic JSON parser → typed `Recommendation`) |

The single public surface is `RetrievalPipeline.run(request)` —
defined in `pipeline.py` and re-exported from `__init__.py`.

## R19.7 invariant — Hot_Path purity

> THE Memory_RAG_Layer SHALL be reachable from the Warm_AI_Pipeline
> only and SHALL NOT be invoked synchronously by the Hot_Path
> (requirements.md §19, R19.7).

This module enforces the invariant by construction:

1. **No NATS subscriber registration.** The retrieval pipeline does
   not register a subscriber on any subject. It is invoked as a
   plain async function (`await pipeline.run(request)`) by
   Warm_AI_Pipeline services that already gate their inputs through
   the curated subject set.
2. **No reachable Hot_Path subject.** Even when callers wrap
   `pipeline.run` with their own NATS subscriber, the only subjects
   that may trigger a synchronous round-trip into the pipeline are:

       ai.*       (Warm_AI_Pipeline outputs — Trade_Ranking,
                  Governance, Shadow_Mode, Journal)
       mem.*      (Memory_RAG_Layer outputs — Previous_Day_Memory,
                  journal queries)
       trader.*   (Human_Control_UI intents)

   Subscribing this pipeline to **any** of the following is forbidden
   and would violate Property 2 (Authority Hierarchy and Hot_Path
   Purity):

       md.*    of.*    feat.*    sig.*    risk.*    exec.*    pos.*

3. **Async-only API.** No synchronous wrapper exists. The Hot_Path
   crates (Rust, `tokio`-based) cannot invoke a Python `async def` in
   a synchronous round-trip without an explicit FFI bridge — and no
   such bridge is built into this package or any of its dependencies.
4. **CI gate.** The `hot-path-purity.yml` workflow already forbids
   `pyo3` / `numpy` / `pandas` and any cloud LLM SDK in every
   Hot_Path crate (R30.4, R30.7, R30.8). That gate plus this module's
   async-only surface together make a synchronous Hot_Path round-trip
   into the Memory_RAG_Layer architecturally unrepresentable.

The matching Property 2 verification lives in task 48.1
(end-to-end Property 2 suite) and the synchronicity property in task
34.2 — both are in the testing track, not this implementation task.

## Configuration

All knobs live in `RetrievalSettings` and resolve from environment
variables — nothing is hardcoded.

| Env var                          | Default              | Meaning |
|----------------------------------|----------------------|---------|
| `HEDGE_RAG_KNN_K`                | `8`                  | kNN `k` per Qdrant collection |
| `HEDGE_RAG_WINDOW_MINUTES`       | `60`                 | Timescale window length anchored at `event.ts` |
| `HEDGE_RAG_OLLAMA_ROLE`          | `qwen`               | Ollama routing key for Stage 4 |
| `HEDGE_RAG_REQUEST_TIMEOUT_S`    | `60.0`               | Wall-clock budget for the whole pipeline |
| `HEDGE_RAG_RECENT_TRADES`        | `50`                 | Cap for the Stage-1 trades ring |
| `HEDGE_RAG_RECENT_NEWS`          | `50`                 | Cap for the Stage-1 news ring |
| `HEDGE_RAG_QDRANT_COLLECTIONS`   | all five canonical   | CSV of collection names |
| `HEDGE_RAG_TIMESCALE_TABLES`     | `fills,ai_scores,regime_history,journal_entries` | CSV of hypertable names |

The collection / hypertable names themselves are defined in
`hedge_memory_rag.qdrant.collections.CollectionName` and
`hedge_memory_rag.timescale.models.HYPERTABLE_NAMES`; the retrieval
layer never duplicates them.

## Failure semantics

* **Cache misses** (Redis) — best-effort; logged at WARNING and
  dropped. The kNN + Timescale stages are the authoritative source.
* **Per-collection / per-table errors** (Qdrant or Timescale) —
  best-effort; logged at WARNING. The failed collection contributes
  zero hits, but the rest of the pipeline still runs.
* **Ollama exhausted** — `OllamaReasoningFailedError`. The fallback
  chain is the OllamaClient's responsibility (R10.9); the pipeline
  surfaces only the final exhaustion.
* **Unparseable response** — `RecommendationParseError` with the raw
  text attached for post-mortem.
* **Pipeline timeout** — `RetrievalTimeoutError` after
  `request_timeout_s`. Bound so a stuck Ollama daemon does not pin a
  Warm_AI_Pipeline coroutine forever.

## Usage

```python
from hedge_memory_rag.retrieval import (
    RetrievalPipeline,
    RetrievalRequest,
    RetrievalSettings,
    TraderEvent,
)

pipeline = RetrievalPipeline(
    settings=RetrievalSettings.load(),
    ollama=ollama_client,
    qdrant=qdrant_store,
    timescale=timescale_reader,
    redis=redis_cache,
)

request = RetrievalRequest(
    correlation_id="ai-rank-2024-01-15-001",
    event=TraderEvent(kind="ai.rank.<cid>", symbol="RELIANCE", payload={...}),
    query_vector=embedding,  # length matches Qdrant collection dim
)
recommendation = await pipeline.run(request)
```
