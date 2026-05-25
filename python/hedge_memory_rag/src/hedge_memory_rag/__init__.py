"""PROJECT HEDGE Memory_RAG_Layer package.

Persistence + retrieval layer combining Qdrant (vectors), PostgreSQL +
TimescaleDB (time-series), and Redis (hot cache). Reachable from the
Warm_AI_Pipeline only — the Hot_Path never invokes this package
synchronously (R19.7).

Top-level re-exports:

* :mod:`hedge_memory_rag.timescale` — async writers, readers, models,
  and migration runner for the eight Timescale hypertables (R19.1, R19.3,
  task 32.1).
* :mod:`hedge_memory_rag.config` — env-driven Postgres/Timescale
  connection settings (no secrets in code).
"""

from . import timescale
from .config import TimescaleConfigError, TimescaleSettings, load_settings

__version__ = "0.1.0"

__all__ = [
    "TimescaleConfigError",
    "TimescaleSettings",
    "load_settings",
    "timescale",
]
