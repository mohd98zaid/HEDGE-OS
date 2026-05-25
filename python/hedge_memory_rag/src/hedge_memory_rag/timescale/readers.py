"""Async readers for the Memory_RAG_Layer Timescale hypertables.

The retrieval pipeline (task 34.1) calls into this layer to perform the
``Timescale window`` step of:

    trader_event_lookup → memory_retrieval (Qdrant kNN + Timescale window)
                       → context_assembly → ollama_reasoning
                       → recommendation_generation

Every reader exposes a ``read_window(...)`` method that takes a half-open
``[start_ts, end_ts)`` window and returns rows ordered by ``ts ASC``.
Optional column-level filters (``symbol_id``, ``broker``, ``trade_id``,
``correlation_id``, ``shadow``) narrow the scan further.

The readers also expose a ``read_window_any`` dispatcher keyed by
:data:`HYPERTABLE_NAMES` so the retrieval pipeline can ask for a window
from any hypertable without coupling itself to the per-table API:

.. code-block:: python

    rows = await reader.read_window_any("ai_scores", start, end)

All queries go through ``Connection.fetch`` (prepared under the hood) and
all results are coerced back into the typed pydantic records defined in
:mod:`hedge_memory_rag.timescale.models` so the round-trip property test
(task 32.2) can rely on structural equality.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Final

import structlog

from .models import (
    HYPERTABLE_NAMES,
    AiScore,
    BrokerMetric,
    FillRecord,
    JournalEntry,
    OrderRecord,
    PrevDayKeyLevel,
    PreviousDayMemoryRow,
    PsychologyTimelinePoint,
    RegimeTransition,
    TickSample,
)
from .pool import TimescalePool

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg

_LOG: Final = structlog.get_logger(__name__)

# --- Exchange enum mapping (kept in sync with writers.py) ------------------

_EXCHANGE_TO_BYTE: Final[dict[str, int]] = {"NSE": 0, "BSE": 1}
_BYTE_TO_EXCHANGE: Final[dict[int, str]] = {v: k for k, v in _EXCHANGE_TO_BYTE.items()}

# Maximum rows a single ``read_window`` call returns by default. Callers
# can opt into a higher cap by passing ``limit=...``.
DEFAULT_LIMIT: Final[int] = 10_000


def _ensure_aware(ts: datetime, *, name: str) -> datetime:
    if ts.tzinfo is None:
        # Treat naive datetimes as UTC; raising is too aggressive for
        # callers that build datetimes via `datetime.utcnow()`.
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _prev_day_row_to_model(row: "asyncpg.Record") -> PreviousDayMemoryRow:
    """Coerce one ``prev_day_memory`` SELECT row into the typed model."""
    import json as _json

    def _maybe_json(value: Any, default: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, (str, bytes, bytearray)):
            return _json.loads(value)
        return value

    raw_levels = _maybe_json(row["key_levels"], [])
    levels = [
        PrevDayKeyLevel(
            kind=str(item["kind"]),  # validated by Literal
            price_paise=int(item["price_paise"]),
        )
        for item in raw_levels
    ]
    return PreviousDayMemoryRow(
        ts=row["ts"],
        session_date=row["session_date"],
        symbol_id=int(row["symbol_id"]),
        symbol=str(row["symbol"]),
        open_paise=int(row["open_paise"]),
        high_paise=int(row["high_paise"]),
        low_paise=int(row["low_paise"]),
        close_paise=int(row["close_paise"]),
        vwap_paise=int(row["vwap_paise"]),
        total_volume=int(row["total_volume"]),
        delivery_volume=int(row["delivery_volume"]),
        key_levels=levels,
        failed_breakouts=list(_maybe_json(row["failed_breakouts"], [])),
        gap_reactions=dict(_maybe_json(row["gap_reactions"], {})),
        trend_continuation=dict(_maybe_json(row["trend_continuation"], {})),
        institutional_behavior=dict(_maybe_json(row["institutional_behavior"], {})),
        news_reactions=list(_maybe_json(row["news_reactions"], [])),
        embedding_point_id=(
            None if row["embedding_point_id"] is None else str(row["embedding_point_id"])
        ),
        computed_ts_ns=int(row["computed_ts_ns"]),
    )


def _validate_window(start_ts: datetime, end_ts: datetime, limit: int | None) -> tuple[
    datetime, datetime, int,
]:
    s = _ensure_aware(start_ts, name="start_ts")
    e = _ensure_aware(end_ts, name="end_ts")
    if s > e:
        raise ValueError(f"start_ts ({s.isoformat()}) must be <= end_ts ({e.isoformat()})")
    cap = DEFAULT_LIMIT if limit is None else int(limit)
    if cap <= 0:
        raise ValueError(f"limit must be > 0, got {cap}")
    return s, e, cap


class TimescaleReader:
    """Async reader exposing time-window queries for every hypertable."""

    def __init__(self, pool: TimescalePool) -> None:
        self._pool = pool

    # ----- Per-table readers --------------------------------------------

    async def read_tick_samples(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        symbol_id: int | None = None,
        limit: int | None = None,
    ) -> list[TickSample]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        if symbol_id is None:
            sql = """
                SELECT ts, symbol_id, exchange, ltp_paise, bid_paise, ask_paise,
                       ltq, total_buy_qty, total_sell_qty, correlation_id
                FROM tick_samples
                WHERE ts >= $1 AND ts < $2
                ORDER BY ts ASC, symbol_id ASC
                LIMIT $3
            """
            rows = await self._fetch(sql, s, e, cap)
        else:
            sql = """
                SELECT ts, symbol_id, exchange, ltp_paise, bid_paise, ask_paise,
                       ltq, total_buy_qty, total_sell_qty, correlation_id
                FROM tick_samples
                WHERE ts >= $1 AND ts < $2 AND symbol_id = $3
                ORDER BY ts ASC
                LIMIT $4
            """
            rows = await self._fetch(sql, s, e, int(symbol_id), cap)
        return [
            TickSample(
                ts=r["ts"],
                symbol_id=int(r["symbol_id"]),
                exchange=_BYTE_TO_EXCHANGE[int(r["exchange"])],
                ltp_paise=int(r["ltp_paise"]),
                bid_paise=int(r["bid_paise"]),
                ask_paise=int(r["ask_paise"]),
                ltq=int(r["ltq"]),
                total_buy_qty=int(r["total_buy_qty"]),
                total_sell_qty=int(r["total_sell_qty"]),
                correlation_id=bytes(r["correlation_id"]),
            )
            for r in rows
        ]

    async def read_orders(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        correlation_id: bytes | None = None,
        broker_order_id: str | None = None,
        limit: int | None = None,
    ) -> list[OrderRecord]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        clauses = ["ts >= $1", "ts < $2"]
        args: list[Any] = [s, e]
        if correlation_id is not None:
            args.append(bytes(correlation_id))
            clauses.append(f"correlation_id = ${len(args)}")
        if broker_order_id is not None:
            args.append(broker_order_id)
            clauses.append(f"broker_order_id = ${len(args)}")
        args.append(cap)
        sql = f"""
            SELECT ts, correlation_id, broker_order_id, state, symbol_id, side,
                   order_type, quantity, limit_paise, filled_qty, avg_fill_paise
            FROM orders
            WHERE {" AND ".join(clauses)}
            ORDER BY ts ASC
            LIMIT ${len(args)}
        """
        rows = await self._fetch(sql, *args)
        return [
            OrderRecord(
                ts=r["ts"],
                correlation_id=bytes(r["correlation_id"]),
                broker_order_id=str(r["broker_order_id"]),
                state=str(r["state"]),  # validated by Literal
                symbol_id=int(r["symbol_id"]),
                side=str(r["side"]),
                order_type=str(r["order_type"]),
                quantity=int(r["quantity"]),
                limit_paise=None if r["limit_paise"] is None else int(r["limit_paise"]),
                filled_qty=int(r["filled_qty"]),
                avg_fill_paise=int(r["avg_fill_paise"]),
            )
            for r in rows
        ]

    async def read_fills(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        correlation_id: bytes | None = None,
        symbol_id: int | None = None,
        limit: int | None = None,
    ) -> list[FillRecord]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        clauses = ["ts >= $1", "ts < $2"]
        args: list[Any] = [s, e]
        if correlation_id is not None:
            args.append(bytes(correlation_id))
            clauses.append(f"correlation_id = ${len(args)}")
        if symbol_id is not None:
            args.append(int(symbol_id))
            clauses.append(f"symbol_id = ${len(args)}")
        args.append(cap)
        sql = f"""
            SELECT ts, correlation_id, broker_order_id, symbol_id, side,
                   fill_qty, fill_paise, cumulative_qty, avg_fill_paise
            FROM fills
            WHERE {" AND ".join(clauses)}
            ORDER BY ts ASC
            LIMIT ${len(args)}
        """
        rows = await self._fetch(sql, *args)
        return [
            FillRecord(
                ts=r["ts"],
                correlation_id=bytes(r["correlation_id"]),
                broker_order_id=str(r["broker_order_id"]),
                symbol_id=int(r["symbol_id"]),
                side=str(r["side"]),
                fill_qty=int(r["fill_qty"]),
                fill_paise=int(r["fill_paise"]),
                cumulative_qty=int(r["cumulative_qty"]),
                avg_fill_paise=int(r["avg_fill_paise"]),
            )
            for r in rows
        ]

    async def read_ai_scores(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        correlation_id: str | None = None,
        signal_id: str | None = None,
        shadow: bool | None = None,
        limit: int | None = None,
    ) -> list[AiScore]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        clauses = ["ts >= $1", "ts < $2"]
        args: list[Any] = [s, e]
        if correlation_id is not None:
            args.append(correlation_id)
            clauses.append(f"correlation_id = ${len(args)}")
        if signal_id is not None:
            args.append(signal_id)
            clauses.append(f"signal_id = ${len(args)}")
        if shadow is not None:
            args.append(bool(shadow))
            clauses.append(f"shadow = ${len(args)}")
        args.append(cap)
        sql = f"""
            SELECT ts, correlation_id, signal_id, trade_confidence_score,
                   factor_orderflow, factor_technical_strength,
                   factor_news_sentiment, factor_market_regime,
                   factor_trader_discipline, shadow
            FROM ai_scores
            WHERE {" AND ".join(clauses)}
            ORDER BY ts ASC
            LIMIT ${len(args)}
        """
        rows = await self._fetch(sql, *args)
        return [
            AiScore(
                ts=r["ts"],
                correlation_id=str(r["correlation_id"]),
                signal_id=str(r["signal_id"]),
                trade_confidence_score=float(r["trade_confidence_score"]),
                factor_orderflow=float(r["factor_orderflow"]),
                factor_technical_strength=float(r["factor_technical_strength"]),
                factor_news_sentiment=float(r["factor_news_sentiment"]),
                factor_market_regime=float(r["factor_market_regime"]),
                factor_trader_discipline=float(r["factor_trader_discipline"]),
                shadow=bool(r["shadow"]),
            )
            for r in rows
        ]

    async def read_regime_history(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        limit: int | None = None,
    ) -> list[RegimeTransition]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        sql = """
            SELECT ts, from_regime, to_regime
            FROM regime_history
            WHERE ts >= $1 AND ts < $2
            ORDER BY ts ASC
            LIMIT $3
        """
        rows = await self._fetch(sql, s, e, cap)
        return [
            RegimeTransition(
                ts=r["ts"],
                from_regime=str(r["from_regime"]),
                to_regime=str(r["to_regime"]),
            )
            for r in rows
        ]

    async def read_psychology_timeline(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        limit: int | None = None,
    ) -> list[PsychologyTimelinePoint]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        sql = """
            SELECT ts, score, discipline, emotional_control, risk_consistency,
                   patience, behaviors
            FROM psychology_timeline
            WHERE ts >= $1 AND ts < $2
            ORDER BY ts ASC
            LIMIT $3
        """
        rows = await self._fetch(sql, s, e, cap)
        return [
            PsychologyTimelinePoint(
                ts=r["ts"],
                score=float(r["score"]),
                discipline=float(r["discipline"]),
                emotional_control=float(r["emotional_control"]),
                risk_consistency=float(r["risk_consistency"]),
                patience=float(r["patience"]),
                behaviors=list(r["behaviors"] or []),
            )
            for r in rows
        ]

    async def read_broker_metrics(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        broker: str | None = None,
        limit: int | None = None,
    ) -> list[BrokerMetric]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        clauses = ["ts >= $1", "ts < $2"]
        args: list[Any] = [s, e]
        if broker is not None:
            args.append(broker)
            clauses.append(f"broker = ${len(args)}")
        args.append(cap)
        sql = f"""
            SELECT ts, broker, latency_ms, error_rate, connected, last_error
            FROM broker_metrics
            WHERE {" AND ".join(clauses)}
            ORDER BY ts ASC
            LIMIT ${len(args)}
        """
        rows = await self._fetch(sql, *args)
        return [
            BrokerMetric(
                ts=r["ts"],
                broker=str(r["broker"]),
                latency_ms=float(r["latency_ms"]),
                error_rate=float(r["error_rate"]),
                connected=bool(r["connected"]),
                last_error=None if r["last_error"] is None else str(r["last_error"]),
            )
            for r in rows
        ]

    async def read_journal_entries(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        symbol: str | None = None,
        trade_id: str | None = None,
        limit: int | None = None,
    ) -> list[JournalEntry]:
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        clauses = ["ts >= $1", "ts < $2"]
        args: list[Any] = [s, e]
        if symbol is not None:
            args.append(symbol)
            clauses.append(f"symbol = ${len(args)}")
        if trade_id is not None:
            args.append(trade_id)
            clauses.append(f"trade_id = ${len(args)}")
        args.append(cap)
        sql = f"""
            SELECT ts, correlation_id, trade_id, symbol, side, quantity,
                   entry_paise, exit_paise, pnl_inr, narrative
            FROM journal_entries
            WHERE {" AND ".join(clauses)}
            ORDER BY ts ASC
            LIMIT ${len(args)}
        """
        rows = await self._fetch(sql, *args)
        return [
            JournalEntry(
                ts=r["ts"],
                correlation_id=str(r["correlation_id"]),
                trade_id=str(r["trade_id"]),
                symbol=str(r["symbol"]),
                side=str(r["side"]),
                quantity=int(r["quantity"]),
                entry_paise=int(r["entry_paise"]),
                exit_paise=int(r["exit_paise"]),
                pnl_inr=float(r["pnl_inr"]),
                narrative=str(r["narrative"]),
            )
            for r in rows
        ]

    # ----- Previous-day memory reader -----------------------------------

    async def read_prev_day_memory(
        self,
        start_ts: datetime,
        end_ts: datetime,
        *,
        symbol_id: int | None = None,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> list[PreviousDayMemoryRow]:
        """Window-scan ``prev_day_memory``. ``ts`` is the session_date as TIMESTAMPTZ."""
        s, e, cap = _validate_window(start_ts, end_ts, limit)
        clauses = ["ts >= $1", "ts < $2"]
        args: list[Any] = [s, e]
        if symbol_id is not None:
            args.append(int(symbol_id))
            clauses.append(f"symbol_id = ${len(args)}")
        if symbol is not None:
            args.append(symbol)
            clauses.append(f"symbol = ${len(args)}")
        args.append(cap)
        sql = f"""
            SELECT ts, session_date, symbol_id, symbol,
                   open_paise, high_paise, low_paise, close_paise, vwap_paise,
                   total_volume, delivery_volume,
                   key_levels, failed_breakouts, gap_reactions,
                   trend_continuation, institutional_behavior, news_reactions,
                   embedding_point_id, computed_ts_ns
            FROM prev_day_memory
            WHERE {" AND ".join(clauses)}
            ORDER BY ts ASC, symbol_id ASC
            LIMIT ${len(args)}
        """
        rows = await self._fetch(sql, *args)
        return [_prev_day_row_to_model(r) for r in rows]

    async def read_prev_day_memory_latest(
        self,
        symbol_id: int,
    ) -> PreviousDayMemoryRow | None:
        """Return the most-recent persisted ``prev_day_memory`` row for ``symbol_id``."""
        sql = """
            SELECT ts, session_date, symbol_id, symbol,
                   open_paise, high_paise, low_paise, close_paise, vwap_paise,
                   total_volume, delivery_volume,
                   key_levels, failed_breakouts, gap_reactions,
                   trend_continuation, institutional_behavior, news_reactions,
                   embedding_point_id, computed_ts_ns
            FROM prev_day_memory
            WHERE symbol_id = $1
            ORDER BY ts DESC
            LIMIT 1
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, int(symbol_id))
        if row is None:
            return None
        return _prev_day_row_to_model(row)

    # ----- Generic dispatcher (used by the retrieval pipeline) -----------

    async def read_window_any(
        self,
        table: str,
        start_ts: datetime,
        end_ts: datetime,
        *,
        limit: int | None = None,
        **filters: Any,
    ) -> list[Any]:
        """Dispatch a ``read_window`` call by canonical hypertable name.

        Accepts the same keyword filters as the per-table methods. Raises
        :class:`ValueError` for unknown tables.
        """
        if table not in HYPERTABLE_NAMES:
            raise ValueError(
                f"unknown hypertable {table!r}; expected one of {HYPERTABLE_NAMES}"
            )
        dispatch = {
            "tick_samples": self.read_tick_samples,
            "orders": self.read_orders,
            "fills": self.read_fills,
            "ai_scores": self.read_ai_scores,
            "regime_history": self.read_regime_history,
            "psychology_timeline": self.read_psychology_timeline,
            "broker_metrics": self.read_broker_metrics,
            "journal_entries": self.read_journal_entries,
            "prev_day_memory": self.read_prev_day_memory,
        }
        method = dispatch[table]
        return await method(start_ts, end_ts, limit=limit, **filters)  # type: ignore[arg-type]

    # ----- Aggregates ---------------------------------------------------

    async def count_window(
        self,
        table: str,
        start_ts: datetime,
        end_ts: datetime,
    ) -> int:
        """Return the row count in ``[start_ts, end_ts)`` for ``table``."""
        if table not in HYPERTABLE_NAMES:
            raise ValueError(
                f"unknown hypertable {table!r}; expected one of {HYPERTABLE_NAMES}"
            )
        s, e, _ = _validate_window(start_ts, end_ts, 1)
        sql = f"SELECT count(*) AS n FROM {table} WHERE ts >= $1 AND ts < $2"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, s, e)
        return int(row["n"]) if row is not None else 0

    # ----- Internals ----------------------------------------------------

    async def _fetch(self, sql: str, *args: Any) -> list["asyncpg.Record"]:
        async with self._pool.acquire() as conn:
            return await conn.fetch(sql, *args)


__all__ = ["DEFAULT_LIMIT", "TimescaleReader"]
