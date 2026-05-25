"""Pure compute helpers for the Previous_Day_Memory_Engine (task 24.1).

Decoupled from the bus and the storage layer so the same code paths
can be exercised by offline replay, backtests, and the live next-session
compute job. Two boundary types:

* :class:`SymbolSessionData` — the inputs the compute job receives for
  one symbol (OHLCV, key-level hints, behaviour markers).
* :class:`PrevDaySessionInputs` — the batch produced by the session
  manager / Memory_RAG aggregation, keyed by ``symbol_id``.

Two pure builders:

* :func:`build_prev_day_row` — produces a typed
  :class:`PreviousDayMemoryRow` ready to hand to
  :meth:`hedge_memory_rag.timescale.TimescaleWriter.write_prev_day_memory`.
* :func:`build_prev_day_event` — produces the canonical
  :class:`PreviousDayMemory` schema record used as the
  ``mem.prev_day.<sym>`` payload (R15.2).

A third helper :func:`format_prev_day_summary` formats a short text
summary suitable for embedding into the Qdrant ``market_memory``
collection — the embedder itself lives in a downstream task and
consumes the output of this helper verbatim.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from hedge_memory_rag.timescale.models import (
    PrevDayKeyLevel as TimescalePrevDayKeyLevel,
)
from hedge_memory_rag.timescale.models import (
    PreviousDayMemoryRow,
)

from ..schemas.mem_prev_day import KeyLevel, KeyLevelKind, PreviousDayMemory


# ---------------------------------------------------------------------------
# Inputs --------------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SymbolSessionData:
    """Per-symbol data the compute job ingests for one trading session.

    All price fields use signed-integer paise (the canonical
    fixed-decimal unit used by ``hedge_core::Px``). Volume fields use
    non-negative ``int``.

    The behaviour markers (``failed_breakouts``, ``gap_reactions``,
    ``trend_continuation``, ``institutional_behavior``,
    ``news_reactions``) are JSON-able free-form mappings / sequences so
    the schema can grow without further migrations.
    """

    symbol_id: int
    symbol: str
    session_date: date
    open_paise: int
    high_paise: int
    low_paise: int
    close_paise: int
    vwap_paise: int
    total_volume: int
    delivery_volume: int

    # Optional hints — explicit empty sequences keep the call sites
    # symmetric.
    key_levels: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    failed_breakouts: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    gap_reactions: Mapping[str, Any] = field(default_factory=dict)
    trend_continuation: Mapping[str, Any] = field(default_factory=dict)
    institutional_behavior: Mapping[str, Any] = field(default_factory=dict)
    news_reactions: Sequence[Mapping[str, Any]] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class PrevDaySessionInputs:
    """Batch input produced by the session aggregator (R15.3).

    Attributes:
        session_date: Calendar date of the trading session whose data is
            being persisted. Stored as ``date`` so the row keys reduce
            to ``(symbol_id, ts == session_date::timestamptz)``.
        symbols: Per-symbol payloads, keyed by ``symbol_id``. Iterable
            so the engine can stream large batches without holding the
            whole input in memory at once.
        computed_ts_ns: Wall-clock nanosecond timestamp at which the
            compute job ran. Persisted on every row for forensics.
    """

    session_date: date
    symbols: Sequence[SymbolSessionData]
    computed_ts_ns: int


# ---------------------------------------------------------------------------
# Builders ------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _coerce_key_level(raw: Mapping[str, Any]) -> TimescalePrevDayKeyLevel:
    """Coerce one free-form key-level dict into the typed timescale model."""
    kind = raw.get("kind")
    if kind not in (
        "support",
        "resistance",
        "swing_high",
        "swing_low",
        "vwap",
        "open",
        "close",
    ):
        raise ValueError(
            f"key_levels[*].kind must be one of the canonical enum values, got {kind!r}"
        )
    price_paise = raw.get("price_paise")
    if not isinstance(price_paise, int):
        raise ValueError(
            f"key_levels[*].price_paise must be int, got {type(price_paise).__name__}"
        )
    return TimescalePrevDayKeyLevel(kind=kind, price_paise=price_paise)


def _session_date_to_ts(session_date: date) -> datetime:
    """Coerce a ``date`` to a UTC midnight ``datetime`` for the hypertable."""
    return datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        tzinfo=timezone.utc,
    )


def build_prev_day_row(
    data: SymbolSessionData,
    *,
    computed_ts_ns: int,
    embedding_point_id: str | None = None,
) -> PreviousDayMemoryRow:
    """Build the typed Timescale row from one symbol's session data.

    Validates the OHLCV invariants (``low <= open/close/vwap <= high``,
    ``delivery_volume <= total_volume``) up-front so the writer never
    persists structurally inconsistent rows.
    """
    if data.low_paise > data.high_paise:
        raise ValueError(
            f"low_paise={data.low_paise} must be <= high_paise={data.high_paise}"
        )
    for name, value in (
        ("open_paise", data.open_paise),
        ("close_paise", data.close_paise),
        ("vwap_paise", data.vwap_paise),
    ):
        if not (data.low_paise <= value <= data.high_paise):
            raise ValueError(
                f"{name}={value} must lie within [low_paise, high_paise] "
                f"= [{data.low_paise}, {data.high_paise}]"
            )
    if data.total_volume < 0:
        raise ValueError(f"total_volume={data.total_volume} must be >= 0")
    if data.delivery_volume < 0:
        raise ValueError(f"delivery_volume={data.delivery_volume} must be >= 0")
    if data.delivery_volume > data.total_volume:
        raise ValueError(
            f"delivery_volume={data.delivery_volume} must be <= "
            f"total_volume={data.total_volume}"
        )
    if computed_ts_ns < 0:
        raise ValueError(f"computed_ts_ns={computed_ts_ns} must be >= 0")

    key_levels = [_coerce_key_level(lvl) for lvl in data.key_levels]

    return PreviousDayMemoryRow(
        ts=_session_date_to_ts(data.session_date),
        session_date=data.session_date,
        symbol_id=data.symbol_id,
        symbol=data.symbol,
        open_paise=data.open_paise,
        high_paise=data.high_paise,
        low_paise=data.low_paise,
        close_paise=data.close_paise,
        vwap_paise=data.vwap_paise,
        total_volume=data.total_volume,
        delivery_volume=data.delivery_volume,
        key_levels=key_levels,
        failed_breakouts=[dict(item) for item in data.failed_breakouts],
        gap_reactions=dict(data.gap_reactions),
        trend_continuation=dict(data.trend_continuation),
        institutional_behavior=dict(data.institutional_behavior),
        news_reactions=[dict(item) for item in data.news_reactions],
        embedding_point_id=embedding_point_id,
        computed_ts_ns=computed_ts_ns,
    )


def build_prev_day_event(
    row: PreviousDayMemoryRow,
) -> PreviousDayMemory:
    """Project a stored row into the canonical ``mem.prev_day.<sym>`` schema (R15.2).

    The behaviour markers do not appear in the wire schema (the schema
    only carries OHLCV / VWAP / key levels) so they are dropped here. A
    consumer that needs them issues a ``mem.prev_day.query`` request
    against the Memory_RAG_Layer instead.
    """
    return PreviousDayMemory(
        symbol=row.symbol,
        session_date=row.session_date.isoformat(),
        open_paise=row.open_paise,
        high_paise=row.high_paise,
        low_paise=row.low_paise,
        close_paise=row.close_paise,
        vwap_paise=row.vwap_paise,
        key_levels=[
            KeyLevel(
                kind=_kind_to_schema(lvl.kind),
                price_paise=lvl.price_paise,
            )
            for lvl in row.key_levels
        ],
        ts_ns=row.computed_ts_ns,
    )


def _kind_to_schema(kind: str) -> KeyLevelKind:
    """Defensive narrowing for the schema's ``Literal`` kind."""
    if kind not in (
        "support",
        "resistance",
        "swing_high",
        "swing_low",
        "vwap",
        "open",
        "close",
    ):  # pragma: no cover - guarded upstream
        raise ValueError(f"unsupported key-level kind {kind!r}")
    return kind  # type: ignore[return-value]


def format_prev_day_summary(row: PreviousDayMemoryRow) -> str:
    """Format a short text summary for the Qdrant ``market_memory`` embedder.

    The string is deterministic and stable across runs: same row
    in → same summary out. Downstream embedders feed this into
    DistilBERT / FinBERT and persist the resulting vector under
    :class:`hedge_memory_rag.qdrant.CollectionName.MARKET_MEMORY`.
    """
    range_paise = row.high_paise - row.low_paise
    delivery_pct = (
        (row.delivery_volume / row.total_volume) if row.total_volume > 0 else 0.0
    )
    parts: list[str] = [
        f"{row.symbol} {row.session_date.isoformat()}",
        f"o={row.open_paise} h={row.high_paise} l={row.low_paise} c={row.close_paise}",
        f"vwap={row.vwap_paise} range={range_paise}",
        f"vol={row.total_volume} delivery={row.delivery_volume} ({delivery_pct:.2%})",
    ]
    if row.key_levels:
        parts.append(
            "levels=[" + ",".join(f"{lvl.kind}@{lvl.price_paise}" for lvl in row.key_levels) + "]"
        )
    if row.failed_breakouts:
        parts.append(f"failed_breakouts={len(row.failed_breakouts)}")
    if row.news_reactions:
        parts.append(f"news_reactions={len(row.news_reactions)}")
    if row.gap_reactions:
        parts.append(
            "gap=" + ",".join(f"{k}={v}" for k, v in sorted(row.gap_reactions.items()))
        )
    if row.trend_continuation:
        parts.append(
            "trend=" + ",".join(f"{k}={v}" for k, v in sorted(row.trend_continuation.items()))
        )
    return " | ".join(parts)


def stable_embedding_point_id(row: PreviousDayMemoryRow) -> str:
    """Derive a stable Qdrant point id from ``(symbol_id, session_date)``.

    Used by the embedder to keep the upsert idempotent across re-runs of
    the next-session compute job — the same (symbol, day) always
    overwrites the same vector instead of accumulating duplicates.
    """
    raw = f"prev_day:{row.symbol_id}:{row.session_date.isoformat()}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return f"prev_day_{row.symbol_id}_{row.session_date.isoformat()}_{digest[:16]}"


def chunk_inputs(
    inputs: PrevDaySessionInputs,
    *,
    chunk_size: int,
) -> Iterable[Sequence[SymbolSessionData]]:
    """Iterate ``inputs.symbols`` in fixed-size chunks. ``chunk_size`` must be > 0."""
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be > 0, got {chunk_size!r}")
    bucket: list[SymbolSessionData] = []
    for sym in inputs.symbols:
        bucket.append(sym)
        if len(bucket) >= chunk_size:
            yield tuple(bucket)
            bucket = []
    if bucket:
        yield tuple(bucket)


__all__ = [
    "PrevDaySessionInputs",
    "SymbolSessionData",
    "build_prev_day_event",
    "build_prev_day_row",
    "chunk_inputs",
    "format_prev_day_summary",
    "stable_embedding_point_id",
]
