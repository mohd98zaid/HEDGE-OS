# `hedge_warm_ai.news` — News_Intelligence_Engine (R12, task 21.1)

This subpackage implements the Warm_AI_Pipeline component that ingests
news from eight canonical sources, runs a 10 ms fast path on FinBERT,
dispatches a slow path to Ollama asynchronously, persists headline
embeddings into the Memory_RAG_Layer's Qdrant `news` collection, and
edge-emits `ai.news.impact.<sym>` to NATS for the Risk_Engine and
Signal_Engine.

## Modules

| Module             | Purpose                                                                                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `headline.py`      | `Headline` + `HeadlineSource`. Frozen dataclass + enum (R12.1).                                                                                        |
| `sources.py`       | `SourceAdapter` ABC + one concrete subclass per listed source. Each adapter exposes `async def stream() -> AsyncIterator[Headline]`.                   |
| `dedup.py`         | `Dedup` — bounded LRU keyed by `content_hash` (SHA-1 of the normalised headline text).                                                                 |
| `fast_path.py`     | `entity_extract` → FinBERT `score` → `impact_score` → `symbol_map`. The 10 ms p95 design target lives here (R12.2).                                    |
| `slow_path.py`     | `OllamaSlowPath` wraps `OllamaClient.stream_generate` (R10.7) and emits a `SlowPathResult` into a sink. Failures captured, never raised.               |
| `qdrant_sink.py`   | `QdrantNewsEmbeddingSink` writes the DistilBERT embedding into the `news` Qdrant collection (R19.2).                                                   |
| `publisher.py`     | `NewsPublisher` protocol + `Noop` / `InMemory` / `Nats` implementations for `ai.news.impact.<sym>` (R12.4).                                            |
| `config.py`        | `NewsConfig` — dedup window, fast-path budget, slow-path role, Qdrant collection, tracked-symbol universe.                                              |
| `engine.py`        | `NewsIntelligenceEngine` — wires the pipeline. Holds the **fast-path-non-blocking** invariant.                                                          |
| `errors.py`        | Typed exception hierarchy: `NewsConfigError`, `NewsIngestionError`, `NewsPublishError`, `NewsQdrantError`.                                              |

## Pipeline

```
Source_Adapter (per source)
    └── Headline
          └── Dedup (content-hash bounded LRU)
                └── Fast_Path
                     { entity_extract,
                       finbert_sentiment,
                       impact_score,
                       symbol_map }
                          └── NewsImpact_v1 ─► ai.news.impact.<sym>
                          └── DistilBERT embed ─► Qdrant `news`
                          └── asyncio.create_task(Slow_Path ollama_reasoning)
```

## Critical invariants

1. **Slow-path non-blocking (R12.3, Property 2).**
   `NewsIntelligenceEngine.ingest` schedules the slow-path coroutine
   via `asyncio.create_task` and emits the fast-path
   `NewsImpact` payload **without awaiting** the slow path. The
   engine retains strong references to spawned tasks in
   `_pending_tasks`; tasks remove themselves on completion. The
   property test in 21.2 fuzzes this.

2. **Bounded outputs (R12.4, Property 4).** Every emitted
   `NewsImpact` payload has `sentiment ∈ [-1.0, 1.0]` and
   `impact_magnitude ∈ [0.0, 1.0]`. The bounds are enforced
   structurally:
   * `FinBERTSentiment.score` clamps `sentiment` at construction.
   * `impact_score` clips the magnitude at construction.
   * `NewsImpact` (Pydantic) re-validates both at payload creation.

3. **Reuse, do not reinvent.** The fast path uses
   `hedge_warm_ai.onnx_runtime.FinBERTSentiment` (task 20.1); the
   slow path uses `hedge_warm_ai.ollama_client.OllamaClient.stream_generate`
   (task 19.1); the embedding sink uses
   `hedge_memory_rag.qdrant.MemoryRagQdrant.upsert` against the
   `news` collection (task 31.1). No new dependencies are
   introduced.

## Configuration

`NewsConfig` is the single tunables surface:

| Field                          | Default       | Purpose                                                       |
| ------------------------------ | ------------- | ------------------------------------------------------------- |
| `dedup_window`                 | `4096`        | Max content hashes the LRU keeps.                              |
| `fast_path_budget_ms`          | `10.0`        | p95 target (R12.2).                                            |
| `slow_path_role`               | `"deepseek"`  | Ollama role for reasoning. Validated against the registry.     |
| `slow_path_request_timeout_s`  | `None`        | Per-call override; `None` uses the role default.               |
| `slow_path_max_tokens`         | `0`           | Token cap; `0` disables the cap.                               |
| `qdrant_collection`            | `"news"`      | Name of the Memory_RAG_Layer Qdrant collection (R19.2).        |
| `symbols`                      | `()`          | Tracked-symbol universe used by `symbol_map`.                  |
| `slow_path_enabled`            | `True`        | Master switch; `False` for replay-only contexts.               |

`NewsConfig.from_yaml_path` mirrors `RegimeConfig.from_yaml_path`:
`extra="forbid"` plus a `with_role_check(ollama: OllamaConfig)`
adaptor that validates the configured slow-path role exists in the
active `HedgeConfig.ollama` registry.

## NATS subjects

| Direction | Subject                          | Schema                              |
| --------- | -------------------------------- | ----------------------------------- |
| `out`     | `ai.news.impact.<symbol>`        | `ai_news_impact.schema.json`        |
| `in`      | (source adapters, per protocol)  | n/a — adapter-defined                |

The publisher takes an `async def publish(subject, payload)`
callable, matching every other Warm_AI_Pipeline emitter
(`NatsPriorityChangedPublisher`, `NatsPsychPublisher`, etc.). The
engine itself has no transport dependency — tests use
`InMemoryNewsPublisher` to assert on emitted events without spinning
up NATS.

## References

- Requirements §12 — News_Intelligence_Engine (R12.1–R12.6).
- Requirements §11 — Fast NLP via ONNX (R11.2, R11.3, R11.4).
- Requirements §19 — Memory_RAG_Layer Qdrant collections (R19.1, R19.2, R19.7).
- Design § Components § News_Intelligence_Engine.
- Design § Architecture § Warm_AI_Pipeline Architecture.
- Design § Correctness Properties § Property 2 (Hot_Path purity),
  Property 3 (latency), Property 4 (bounds), Property 7 (gating).
- Task 21.1 (this task), 21.2 (`hypothesis` property test).
