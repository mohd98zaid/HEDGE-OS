"""Settings for :mod:`hedge_memory_rag.retrieval` (R19.5, R19.6, task 34.1).

Configuration flows in from environment variables (``HEDGE_RAG_*``) and
from already-loaded subpackage settings (Qdrant collection names,
Timescale hypertable names). Nothing in this module hardcodes a host,
port, model name, or collection name.

The retrieval pipeline composes existing primitives, so this module
deliberately stays small — all heavy lifting (connection pooling,
client lifecycle) lives in the underlying subpackages.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final

from ..qdrant.collections import CollectionName
from ..timescale.models import HYPERTABLE_NAMES
from .errors import RetrievalConfigurationError

# --- Environment variable names -------------------------------------------

ENV_K: Final[str] = "HEDGE_RAG_KNN_K"
ENV_WINDOW_MIN: Final[str] = "HEDGE_RAG_WINDOW_MINUTES"
ENV_OLLAMA_ROLE: Final[str] = "HEDGE_RAG_OLLAMA_ROLE"
ENV_REQUEST_TIMEOUT_S: Final[str] = "HEDGE_RAG_REQUEST_TIMEOUT_S"
ENV_RECENT_TRADES: Final[str] = "HEDGE_RAG_RECENT_TRADES"
ENV_RECENT_NEWS: Final[str] = "HEDGE_RAG_RECENT_NEWS"
ENV_QDRANT_COLLECTIONS: Final[str] = "HEDGE_RAG_QDRANT_COLLECTIONS"
ENV_TIMESCALE_TABLES: Final[str] = "HEDGE_RAG_TIMESCALE_TABLES"

# --- Defaults --------------------------------------------------------------
#
# These match the design's reasonable expectations and the existing
# subpackage defaults; production deployments override every value via
# the ``HEDGE_RAG_*`` environment variables.

#: Default kNN ``k``. Each Qdrant collection contributes up to ``k``
#: hits; the assembled context dedupes by point id.
DEFAULT_K: Final[int] = 8

#: Default Timescale ``[start, end)`` window length, applied as
#: ``[end - WINDOW_MINUTES, end)`` where ``end`` is the trader-event
#: timestamp.
DEFAULT_WINDOW_MINUTES: Final[int] = 60

#: Default :class:`hedge_warm_ai.ollama_client.OllamaRoleKey` used for
#: the reasoning step. ``"qwen"`` is the design's primary reasoning
#: model; degraded routing to deepseek/mistral/phi happens inside the
#: client and is invisible to this module.
DEFAULT_OLLAMA_ROLE: Final[str] = "qwen"

#: Default overall pipeline budget in seconds. Bound to keep a stuck
#: Ollama daemon from pinning a Warm_AI_Pipeline coroutine forever.
DEFAULT_REQUEST_TIMEOUT_S: Final[float] = 60.0

#: Default ring-fetch sizes for the Stage-1 hot-cache lookups.
DEFAULT_RECENT_TRADES_PER_SYMBOL: Final[int] = 50
DEFAULT_RECENT_NEWS_PER_SYMBOL: Final[int] = 50

#: Default Qdrant collections to query in the kNN step. Order is
#: irrelevant — the kNN searches run concurrently inside Stage 2.
_DEFAULT_QDRANT_COLLECTIONS: Final[tuple[CollectionName, ...]] = (
    CollectionName.TRADES,
    CollectionName.NEWS,
    CollectionName.JOURNAL_ENTRIES,
    CollectionName.MARKET_MEMORY,
    CollectionName.PSYCHOLOGY_HISTORY,
)

#: Default Timescale hypertables to read a window from. Mirrors the
#: persistent state most useful to a trader-event reasoning request:
#: recent fills, AI scores, regime transitions, and journal entries.
_DEFAULT_TIMESCALE_TABLES: Final[tuple[str, ...]] = (
    "fills",
    "ai_scores",
    "regime_history",
    "journal_entries",
)


# --- Helpers ---------------------------------------------------------------


def _parse_int(name: str, raw: str | None, default: int, *, minimum: int = 1) -> int:
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RetrievalConfigurationError(
            f"{name}={raw!r} is not a valid integer"
        ) from exc
    if value < minimum:
        raise RetrievalConfigurationError(
            f"{name}={value} must be >= {minimum}"
        )
    return value


def _parse_float(name: str, raw: str | None, default: float, *, minimum: float = 0.0) -> float:
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise RetrievalConfigurationError(
            f"{name}={raw!r} is not a valid float"
        ) from exc
    if value <= minimum:
        raise RetrievalConfigurationError(
            f"{name}={value} must be > {minimum}"
        )
    return value


def _parse_collections(raw: str | None) -> tuple[CollectionName, ...]:
    if raw is None or raw == "":
        return _DEFAULT_QDRANT_COLLECTIONS
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        return _DEFAULT_QDRANT_COLLECTIONS
    valid = {member.value: member for member in CollectionName}
    out: list[CollectionName] = []
    for part in parts:
        if part not in valid:
            raise RetrievalConfigurationError(
                f"{ENV_QDRANT_COLLECTIONS}: unknown Qdrant collection {part!r}; "
                f"expected one of {sorted(valid)}"
            )
        out.append(valid[part])
    # Preserve order, dedupe.
    seen: set[CollectionName] = set()
    unique: list[CollectionName] = []
    for c in out:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return tuple(unique)


def _parse_tables(raw: str | None) -> tuple[str, ...]:
    if raw is None or raw == "":
        return _DEFAULT_TIMESCALE_TABLES
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        return _DEFAULT_TIMESCALE_TABLES
    valid = set(HYPERTABLE_NAMES)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if part not in valid:
            raise RetrievalConfigurationError(
                f"{ENV_TIMESCALE_TABLES}: unknown hypertable {part!r}; "
                f"expected one of {sorted(valid)}"
            )
        if part not in seen:
            seen.add(part)
            out.append(part)
    return tuple(out)


# --- Settings --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RetrievalSettings:
    """Resolved settings for a :class:`RetrievalPipeline` instance.

    Attributes:
        k: kNN ``k`` applied per Qdrant collection.
        window_minutes: Length of the Timescale ``[start, end)`` window
            anchored at the trader-event timestamp.
        ollama_role: Routing key for the reasoning step. Resolved
            through :class:`hedge_warm_ai.ollama_client.OllamaClient`'s
            registered endpoints; degraded fallback is the client's
            responsibility.
        request_timeout_s: Wall-clock budget for the entire pipeline.
            Bound to keep a stuck Ollama daemon from pinning a
            Warm_AI_Pipeline coroutine forever.
        recent_trades_per_symbol: Maximum number of recent trades to
            pull from the Redis hot cache during Stage 1. ``0``
            disables the lookup; the cache is treated as best-effort
            and a miss is never fatal.
        recent_news_per_symbol: Same as ``recent_trades_per_symbol``
            but for the per-symbol news ring.
        qdrant_collections: Ordered tuple of Qdrant collections to
            query in Stage 2. Defaults to all five canonical
            collections.
        timescale_tables: Ordered tuple of Timescale hypertable names
            to ``read_window_any`` during Stage 2. Each name must be
            in :data:`HYPERTABLE_NAMES`.
    """

    k: int = DEFAULT_K
    window_minutes: int = DEFAULT_WINDOW_MINUTES
    ollama_role: str = DEFAULT_OLLAMA_ROLE
    request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S
    recent_trades_per_symbol: int = DEFAULT_RECENT_TRADES_PER_SYMBOL
    recent_news_per_symbol: int = DEFAULT_RECENT_NEWS_PER_SYMBOL
    qdrant_collections: tuple[CollectionName, ...] = field(
        default_factory=lambda: _DEFAULT_QDRANT_COLLECTIONS
    )
    timescale_tables: tuple[str, ...] = field(
        default_factory=lambda: _DEFAULT_TIMESCALE_TABLES
    )

    def __post_init__(self) -> None:
        if self.k < 1:
            raise RetrievalConfigurationError(
                f"RetrievalSettings.k must be >= 1, got {self.k!r}"
            )
        if self.window_minutes < 1:
            raise RetrievalConfigurationError(
                f"RetrievalSettings.window_minutes must be >= 1, got {self.window_minutes!r}"
            )
        if not self.ollama_role:
            raise RetrievalConfigurationError(
                "RetrievalSettings.ollama_role must be a non-empty string"
            )
        if self.request_timeout_s <= 0:
            raise RetrievalConfigurationError(
                "RetrievalSettings.request_timeout_s must be > 0, "
                f"got {self.request_timeout_s!r}"
            )
        if self.recent_trades_per_symbol < 0:
            raise RetrievalConfigurationError(
                "RetrievalSettings.recent_trades_per_symbol must be >= 0, "
                f"got {self.recent_trades_per_symbol!r}"
            )
        if self.recent_news_per_symbol < 0:
            raise RetrievalConfigurationError(
                "RetrievalSettings.recent_news_per_symbol must be >= 0, "
                f"got {self.recent_news_per_symbol!r}"
            )
        if not self.qdrant_collections and not self.timescale_tables:
            # Stage 2 would be a no-op — that is almost certainly a
            # caller bug.
            raise RetrievalConfigurationError(
                "RetrievalSettings: at least one of qdrant_collections or "
                "timescale_tables must be non-empty"
            )
        valid_tables = set(HYPERTABLE_NAMES)
        for table in self.timescale_tables:
            if table not in valid_tables:
                raise RetrievalConfigurationError(
                    f"RetrievalSettings.timescale_tables: unknown hypertable {table!r}; "
                    f"expected one of {sorted(valid_tables)}"
                )

    @classmethod
    def load(cls, env: dict[str, str] | None = None) -> "RetrievalSettings":
        """Build :class:`RetrievalSettings` from environment variables.

        ``env`` exists so tests can pass a synthetic environment without
        mutating ``os.environ``. Production callers leave it ``None``.
        """
        source = env if env is not None else dict(os.environ)
        return cls(
            k=_parse_int(ENV_K, source.get(ENV_K), DEFAULT_K, minimum=1),
            window_minutes=_parse_int(
                ENV_WINDOW_MIN, source.get(ENV_WINDOW_MIN), DEFAULT_WINDOW_MINUTES, minimum=1
            ),
            ollama_role=source.get(ENV_OLLAMA_ROLE, DEFAULT_OLLAMA_ROLE) or DEFAULT_OLLAMA_ROLE,
            request_timeout_s=_parse_float(
                ENV_REQUEST_TIMEOUT_S,
                source.get(ENV_REQUEST_TIMEOUT_S),
                DEFAULT_REQUEST_TIMEOUT_S,
            ),
            recent_trades_per_symbol=_parse_int(
                ENV_RECENT_TRADES,
                source.get(ENV_RECENT_TRADES),
                DEFAULT_RECENT_TRADES_PER_SYMBOL,
                minimum=0,
            ),
            recent_news_per_symbol=_parse_int(
                ENV_RECENT_NEWS,
                source.get(ENV_RECENT_NEWS),
                DEFAULT_RECENT_NEWS_PER_SYMBOL,
                minimum=0,
            ),
            qdrant_collections=_parse_collections(source.get(ENV_QDRANT_COLLECTIONS)),
            timescale_tables=_parse_tables(source.get(ENV_TIMESCALE_TABLES)),
        )


def load_retrieval_settings(env: dict[str, str] | None = None) -> RetrievalSettings:
    """Module-level alias for :meth:`RetrievalSettings.load`."""
    return RetrievalSettings.load(env)


__all__ = [
    "DEFAULT_K",
    "DEFAULT_OLLAMA_ROLE",
    "DEFAULT_RECENT_NEWS_PER_SYMBOL",
    "DEFAULT_RECENT_TRADES_PER_SYMBOL",
    "DEFAULT_REQUEST_TIMEOUT_S",
    "DEFAULT_WINDOW_MINUTES",
    "ENV_K",
    "ENV_OLLAMA_ROLE",
    "ENV_QDRANT_COLLECTIONS",
    "ENV_RECENT_NEWS",
    "ENV_RECENT_TRADES",
    "ENV_REQUEST_TIMEOUT_S",
    "ENV_TIMESCALE_TABLES",
    "ENV_WINDOW_MIN",
    "RetrievalSettings",
    "load_retrieval_settings",
]
