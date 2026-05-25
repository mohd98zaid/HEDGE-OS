# hedge-memory-rag

Persistence + retrieval layer for PROJECT HEDGE.

Wraps Qdrant (vectors), PostgreSQL+TimescaleDB (time-series + relational), and
Redis (hot cache). Reachable from the Warm_AI_Pipeline only — never invoked
synchronously by the Hot_Path (R19.7).

## Status

| Submodule                 | Task  | Status      |
| ------------------------- | ----- | ----------- |
| `hedge_memory_rag.qdrant` | 31.1  | ✅ landed    |
| `hedge_memory_rag.timescale` | 32.1 | ✅ landed   |
| `hedge_memory_rag.redis_cache` | 33.1 | ✅ landed |
| `hedge_memory_rag.retrieval` | 34.1 | ✅ landed   |

## Qdrant integration (task 31.1)

Five canonical collections are provisioned idempotently on startup:

* `trades` — closed-trade embeddings (entry / exit context).
* `news` — News_Intelligence headline embeddings.
* `journal_entries` — AI_Trade_Journal narratives.
* `market_memory` — Previous_Day_Memory + intraday market-state vectors.
* `psychology_history` — Trader_Psychology snapshots.

Embedding payloads are CBOR-encoded (design § Data Models — *Warm_AI_Pipeline
payloads are JSON for ergonomics, except embeddings which are CBOR*).

```python
from hedge_memory_rag.qdrant import (
    CollectionName,
    MemoryRagQdrant,
    QdrantSettings,
    VectorRecord,
)

settings = QdrantSettings.load()  # honours HEDGE_QDRANT_URL / API_KEY
async with MemoryRagQdrant(settings=settings) as store:
    await store.ensure_collections()

    await store.upsert(
        CollectionName.TRADES,
        VectorRecord(
            point_id="trade-2024-01-15-001",
            vector=embedding,           # 768-d list[float] or numpy.ndarray
            payload={"symbol": "RELIANCE", "side": "Buy"},
        ),
    )

    hits = await store.knn_search(
        CollectionName.TRADES,
        query_vector=query_embedding,
        k=10,
        payload_filter={"symbol": "RELIANCE"},
    )
```

### Configuration

`QdrantSettings.load()` reads (in order of precedence):

1. `HEDGE_QDRANT_URL` (e.g. `http://qdrant:6333`) — scheme + host + port.
2. `HEDGE_QDRANT_HOST`, `HEDGE_QDRANT_PORT`, `HEDGE_QDRANT_GRPC_PORT` — overrides.
3. `HEDGE_QDRANT_API_KEY` — optional auth (Qdrant Cloud / mTLS deployments).

Defaults match `docker-compose.yml`: `qdrant:6333` HTTP, `qdrant:6334` gRPC.

### Idempotent provisioning

`ensure_collections()` is safe to call on every service boot:

* Missing collections are created with the configured `vector_dim` /
  `distance` / `on_disk` parameters.
* Existing collections are validated against the spec; a mismatch in
  dimensionality or distance metric raises
  `CollectionDimensionMismatchError` rather than silently recreating
  the collection (which would drop every persisted vector).
