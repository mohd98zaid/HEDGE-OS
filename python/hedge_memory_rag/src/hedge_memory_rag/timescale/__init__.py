"""Memory_RAG_Layer Timescale subpackage (R19.3, task 32.1).

Hypertables for sampled ticks, fills, orders, AI scores, regime history,
psychology timeline, broker metrics, and journal entries. Async writers
and readers are built on top of ``asyncpg`` with prepared statements; the
``apply_migrations`` helper applies the bundled SQL migrations to a fresh
database (idempotent — safe to run on every service start).

The pydantic models in :mod:`hedge_memory_rag.timescale.models` mirror
the canonical FlatBuffers / JSON schemas committed in
``crates/hedge-schemas`` so the serialised wire formats are the source of
truth and the persisted rows round-trip back into typed records.

References:
- Design § Memory_RAG_Layer (R19) — hypertable list and producer set.
- Requirements 19.1, 19.3 — persistence + Timescale extension.
"""

from .config_re_export import TimescaleSettings, load_timescale_settings  # noqa: F401
from .migrator import (
    MigrationError,
    apply_migrations,
    iter_migration_files,
    list_hypertables,
)
from .models import HYPERTABLE_NAMES  # re-exported for convenience
from .models import (
    AiScore,
    BrokerMetric,
    GovernanceAction,
    GovernanceLevel,
    GovernanceMetric,
    GovernanceMetricKind,
    GovernanceMetricSample,
    JournalEntry,
    OrderRecord,
    FillRecord,
    PrevDayKeyLevel,
    PreviousDayMemoryRow,
    PsychologyTimelinePoint,
    RegimeTransition,
    TickSample,
)
from .pool import TimescalePool, TimescalePoolError, create_pool
from .readers import TimescaleReader
from .writers import TimescaleWriter

__all__ = [
    # config (re-exported for convenience)
    "TimescaleSettings",
    "load_timescale_settings",
    # migrations
    "HYPERTABLE_NAMES",
    "MigrationError",
    "apply_migrations",
    "iter_migration_files",
    "list_hypertables",
    # models
    "AiScore",
    "BrokerMetric",
    "FillRecord",
    "GovernanceAction",
    "GovernanceLevel",
    "GovernanceMetric",
    "GovernanceMetricKind",
    "GovernanceMetricSample",
    "JournalEntry",
    "OrderRecord",
    "PrevDayKeyLevel",
    "PreviousDayMemoryRow",
    "PsychologyTimelinePoint",
    "RegimeTransition",
    "TickSample",
    # pool
    "TimescalePool",
    "TimescalePoolError",
    "create_pool",
    # readers / writers
    "TimescaleReader",
    "TimescaleWriter",
]
