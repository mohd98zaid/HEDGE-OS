"""Convenience re-export of :mod:`hedge_memory_rag.config`.

Lets callers `from hedge_memory_rag.timescale import TimescaleSettings`
without forcing a deeper import path.
"""

from ..config import TimescaleConfigError, TimescaleSettings, load_timescale_settings

__all__ = [
    "TimescaleConfigError",
    "TimescaleSettings",
    "load_timescale_settings",
]
