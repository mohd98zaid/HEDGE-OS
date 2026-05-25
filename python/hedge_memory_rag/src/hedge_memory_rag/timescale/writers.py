"""Async writers for the Memory_RAG_Layer Timescale hypertables.

Every writer:

* uses an ``asyncpg`` connection acquired from the shared
  :class:`TimescalePool`,
* sends rows through ``Connection.executemany`` so the parameterised
  statement is prepared once per call (R19.3, "prepared statements"),
* coerces typed pydantic records back into positional row tuples
  matching the column order of the underlying hypertable.

The Memory_RAG_Layer is reachable from the Warm_AI_Pipeline only and
must NOT be invoked synchronously by the Hot_Path (R19.7); these helpers
are async functions on purpose.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Final

import structlog

from .models import (
    AiScore,
    BrokerMetric,
    FillRecord,
    JournalEntry,
    OrderRecord,
    PsychologyTimelinePoint,
    RegimeTransition,
    TickSample,
)
from .pool import TimescalePool

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg

_LOG: Final = structlog.get_logger(__name__)

# --- Exchange enum mapping (mirrors hedge_core::Exchange byte order) -------

_EXCHANGE_TO_BYTE: Final[dict[str, int]] = {"NSE": 0, "BSE": 1}
_BYTE_TO_EXCHANGE: Final[dict[int, str]] = {v: k for k, v in _EXCHANGE_TO_BYTE.items()}


# --- Prepared SQL ---------------------------------------------------------

_INSERT_TICK = """
    INSERT INTO tick_samples
        (ts, symbol_id, exchange, ltp_paise, bid_paise, ask_paise,
         ltq, total_buy_qty, total_sell_qty, correlation_id)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""

_INSERT_ORDER = """
    INSERT INTO orders
        (ts, correlation_id, broker_order_id, state, symbol_id, side,
         order_type, quantity, limit_paise, filled_qty, avg_fill_paise)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
"""

_INSERT_FILL = """
    INSERT INTO fills
        (ts, correlation_id, broker_order_id, symbol_id, side,
         fill_qty, fill_paise, cumulative_qty, avg_fill_paise)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
"""

_INSERT_AI_SCORE = """
    INSERT INTO ai_scores
        (ts, correlation_id, signal_id, trade_confidence_score,
         factor_orderflow, factor_technical_strength, factor_news_sentiment,
         factor_market_regime, factor_trader_discipline, shadow)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""

_INSERT_REGIME = """
    INSERT INTO regime_history (ts, from_regime, to_regime)
    VALUES ($1, $2, $3)
"""

_INSERT_PSYCH = """
    INSERT INTO psychology_timeline
        (ts, score, discipline, emotional_control, risk_consistency, patience, behaviors)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

_INSERT_BROKER_METRIC = """
    INSERT INTO broker_metrics
        (ts, broker, latency_ms, error_rate, connected, last_error)
    VALUES ($1, $2, $3, $4, $5, $6)
"""

_INSERT_JOURNAL = """
    INSERT INTO journal_entries
        (ts, correlation_id, trade_id, symbol, side, quantity,
         entry_paise, exit_paise, pnl_inr, narrative)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"""


# --- Row coercion ---------------------------------------------------------


def _tick_row(t: TickSample) -> tuple[Any, ...]:
    return (
        t.ts,
        int(t.symbol_id),
        _EXCHANGE_TO_BYTE[t.exchange],
        int(t.ltp_paise),
        int(t.bid_paise),
        int(t.ask_paise),
        int(t.ltq),
        int(t.total_buy_qty),
        int(t.total_sell_qty),
        bytes(t.correlation_id),
    )


def _order_row(o: OrderRecord) -> tuple[Any, ...]:
    return (
        o.ts,
        bytes(o.correlation_id),
        o.broker_order_id,
        o.state,
        int(o.symbol_id),
        o.side,
        o.order_type,
        int(o.quantity),
        None if o.limit_paise is None else int(o.limit_paise),
        int(o.filled_qty),
        int(o.avg_fill_paise),
    )


def _fill_row(f: FillRecord) -> tuple[Any, ...]:
    return (
        f.ts,
        bytes(f.correlation_id),
        f.broker_order_id,
        int(f.symbol_id),
        f.side,
        int(f.fill_qty),
        int(f.fill_paise),
        int(f.cumulative_qty),
        int(f.avg_fill_paise),
    )


def _ai_score_row(s: AiScore) -> tuple[Any, ...]:
    return (
        s.ts,
        s.correlation_id,
        s.signal_id,
        float(s.trade_confidence_score),
        float(s.factor_orderflow),
        float(s.factor_technical_strength),
        float(s.factor_news_sentiment),
        float(s.factor_market_regime),
        float(s.factor_trader_discipline),
        bool(s.shadow),
    )


def _regime_row(r: RegimeTransition) -> tuple[Any, ...]:
    return (r.ts, r.from_regime, r.to_regime)


def _psych_row(p: PsychologyTimelinePoint) -> tuple[Any, ...]:
    return (
        p.ts,
        float(p.score),
        float(p.discipline),
        float(p.emotional_control),
        float(p.risk_consistency),
        float(p.patience),
        list(p.behaviors),
    )


def _broker_metric_row(m: BrokerMetric) -> tuple[Any, ...]:
    return (
        m.ts,
        m.broker,
        float(m.latency_ms),
        float(m.error_rate),
        bool(m.connected),
        m.last_error,
    )


def _journal_row(j: JournalEntry) -> tuple[Any, ...]:
    return (
        j.ts,
        j.correlation_id,
        j.trade_id,
        j.symbol,
        j.side,
        int(j.quantity),
        int(j.entry_paise),
        int(j.exit_paise),
        float(j.pnl_inr),
        j.narrative,
    )


# --- Writer ---------------------------------------------------------------


class TimescaleWriter:
    """Async batch writer for every Memory_RAG_Layer hypertable."""

    def __init__(self, pool: TimescalePool) -> None:
        self._pool = pool

    # The ``write_*`` methods accept either a single record or an iterable
    # of records. Empty iterables are a no-op.

    async def write_tick(self, sample: TickSample | Iterable[TickSample]) -> int:
        return await self._executemany(_INSERT_TICK, sample, _tick_row)

    async def write_order(self, order: OrderRecord | Iterable[OrderRecord]) -> int:
        return await self._executemany(_INSERT_ORDER, order, _order_row)

    async def write_fill(self, fill: FillRecord | Iterable[FillRecord]) -> int:
        return await self._executemany(_INSERT_FILL, fill, _fill_row)

    async def write_ai_score(self, score: AiScore | Iterable[AiScore]) -> int:
        return await self._executemany(_INSERT_AI_SCORE, score, _ai_score_row)

    async def write_regime_transition(
        self, transition: RegimeTransition | Iterable[RegimeTransition]
    ) -> int:
        return await self._executemany(_INSERT_REGIME, transition, _regime_row)

    async def write_psychology_point(
        self, point: PsychologyTimelinePoint | Iterable[PsychologyTimelinePoint]
    ) -> int:
        return await self._executemany(_INSERT_PSYCH, point, _psych_row)

    async def write_broker_metric(
        self, metric: BrokerMetric | Iterable[BrokerMetric]
    ) -> int:
        return await self._executemany(_INSERT_BROKER_METRIC, metric, _broker_metric_row)

    async def write_journal_entry(
        self, entry: JournalEntry | Iterable[JournalEntry]
    ) -> int:
        return await self._executemany(_INSERT_JOURNAL, entry, _journal_row)

    # ---- internals --------------------------------------------------

    async def _executemany(
        self,
        sql: str,
        items: Any,
        mapper: Any,
    ) -> int:
        rows = list(_iter_rows(items, mapper))
        if not rows:
            return 0
        async with self._pool.acquire() as conn:
            # asyncpg's executemany uses the statement once and binds each
            # row tuple, satisfying the "prepared statements" requirement.
            await conn.executemany(sql, rows)
        return len(rows)


def _iter_rows(items: Any, mapper: Any) -> Iterable[tuple[Any, ...]]:
    """Yield row tuples for both single-record and iterable inputs."""
    if items is None:
        return
    if isinstance(items, (TickSample, OrderRecord, FillRecord, AiScore,
                          RegimeTransition, PsychologyTimelinePoint,
                          BrokerMetric, JournalEntry)):
        yield mapper(items)
        return
    # Treat strings/bytes as scalars even though they're iterable.
    if isinstance(items, (str, bytes)):
        raise TypeError(f"unexpected scalar input: {type(items).__name__}")
    for record in items:
        yield mapper(record)


__all__ = ["TimescaleWriter"]
