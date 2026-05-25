# hedge-memory-rag

Persistence + retrieval layer for PROJECT HEDGE.

Wraps Qdrant (vectors), PostgreSQL+TimescaleDB (time-series + relational), and
Redis (hot cache). Reachable from the Warm_AI_Pipeline only — never invoked
synchronously by the Hot_Path (R19.7).

## Timescale hypertables (task 32.1, R19.1, R19.3)

The `hedge_memory_rag.timescale` subpackage provisions and accesses the
following Timescale hypertables:

| Hypertable             | Producer (canonical schema)                  | `chunk_time_interval` |
| ---------------------- | -------------------------------------------- | --------------------- |
| `tick_samples`         | sampled `Tick_v1`                            | 1 hour                |
| `orders`               | `OrderState_v1` lifecycle                    | 6 hours               |
| `fills`                | `OrderState_v1` partial / final fills        | 6 hours               |
| `ai_scores`            | `ai.rank.<cid>` (`ai_rank.schema.json`)      | 6 hours               |
| `regime_history`       | `ai.regime.changed`                          | 1 day                 |
| `psychology_timeline`  | `ai.psych.stability`                         | 6 hours               |
| `broker_metrics`       | broker latency / error / connectivity sample | 6 hours               |
| `journal_entries`      | `ai.journal.entry` (R18.2)                   | 1 day                 |

### Connection settings

All settings come from environment variables (never hardcoded):

| Variable                            | Purpose                                                                                    | Default        |
| ----------------------------------- | ------------------------------------------------------------------------------------------ | -------------- |
| `HEDGE_POSTGRES_URL`                | Full libpq DSN. When set, this **wins** over the discrete fields below.                    | unset          |
| `HEDGE_POSTGRES_HOST`               | Hostname / address                                                                         | `postgres`     |
| `HEDGE_POSTGRES_PORT`               | TCP port                                                                                   | `5432`         |
| `HEDGE_POSTGRES_DB`                 | Database name                                                                              | `hedge`        |
| `HEDGE_POSTGRES_USER`               | Username                                                                                   | `hedge`        |
| `HEDGE_POSTGRES_PASSWORD`           | Password                                                                                   | `hedge`        |
| `HEDGE_POSTGRES_MIN_POOL`           | `asyncpg.create_pool(min_size=...)`                                                        | `1`            |
| `HEDGE_POSTGRES_MAX_POOL`           | `asyncpg.create_pool(max_size=...)`                                                        | `10`           |
| `HEDGE_POSTGRES_STATEMENT_TIMEOUT_MS` | Per-session `SET statement_timeout`                                                      | `5000`         |
| `HEDGE_POSTGRES_COMMAND_TIMEOUT_S`  | Pool-wide command timeout                                                                  | `10`           |

### Bootstrap

```python
from hedge_memory_rag.timescale import (
    apply_migrations,
    create_pool,
    TimescaleReader,
    TimescaleWriter,
)

async def bootstrap() -> None:
    pool = await create_pool()              # env-driven
    await apply_migrations(pool.raw)        # idempotent
    writer = TimescaleWriter(pool)
    reader = TimescaleReader(pool)
    # ... use writer / reader from any Warm_AI service ...
```

### Time-window queries (consumed by the retrieval pipeline, task 34.1)

```python
from datetime import datetime, timedelta, timezone

end = datetime.now(timezone.utc)
start = end - timedelta(minutes=15)

ai_scores = await reader.read_ai_scores(start, end, signal_id="abc-123")
ticks      = await reader.read_tick_samples(start, end, symbol_id=12345, limit=5_000)
journal    = await reader.read_journal_entries(start, end, symbol="RELIANCE")

# Generic dispatcher used by the retrieval pipeline:
rows = await reader.read_window_any("regime_history", start, end)
n    = await reader.count_window("fills", start, end)
```

The migration runner records applied files in
`hedge_memory_rag.schema_migrations` and is safe to call on every service
start. Each SQL statement is itself idempotent (`CREATE ... IF NOT EXISTS`,
`create_hypertable(..., if_not_exists => TRUE)`).

