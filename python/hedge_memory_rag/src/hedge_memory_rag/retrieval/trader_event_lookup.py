"""Stage 1 of the retrieval pipeline — trader_event_lookup (R19.5).

Loads:

* The trader event payload from the request (verbatim).
* Best-effort short-history context from the Redis hot cache:
  ``recent_trades(symbol)``, ``recent_news(symbol)``, ``get_regime()``,
  ``get_stability_score()``.

A Redis miss is **never fatal** — the kNN + Timescale stages are the
authoritative source of long-term memory, and Stage 1 is intentionally
fast and tolerant of partial failures.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import structlog

from .config import RetrievalSettings
from .records import EventContext, RetrievalRequest

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..redis_cache.cache import RedisHotCache

_LOG = structlog.get_logger(__name__)


async def trader_event_lookup(
    request: RetrievalRequest,
    *,
    redis: "RedisHotCache | None",
    settings: RetrievalSettings,
) -> EventContext:
    """Run Stage 1: load the trader event + best-effort hot-cache snapshots.

    Args:
        request: The originating :class:`RetrievalRequest`. The trader
            event lives on ``request.event``.
        redis: Connected Redis hot cache, or ``None`` to skip the
            lookup entirely (e.g. when the deployment has not wired a
            cache instance — Stage 1 still returns a valid
            :class:`EventContext`).
        settings: Resolved settings; ``recent_trades_per_symbol`` and
            ``recent_news_per_symbol`` cap how many entries the
            assembler renders.

    Returns:
        :class:`EventContext` with whichever hot-cache fields were
        successfully retrieved. Cache failures are logged and dropped.
    """
    if redis is None or request.event.symbol is None:
        # Skip the per-symbol rings when no cache is wired or the
        # event is account-wide.
        return await _account_wide_lookup(request, redis=redis)

    symbol = request.event.symbol

    async def _safe_recent_trades() -> tuple[object, ...]:
        if settings.recent_trades_per_symbol == 0:
            return ()
        try:
            items = await redis.recent_trades(symbol)
        except Exception as exc:  # noqa: BLE001 - cache miss is best-effort
            _LOG.warning(
                "trader_event_lookup.recent_trades_failed",
                correlation_id=request.correlation_id,
                symbol=symbol,
                error=str(exc),
            )
            return ()
        return tuple(items[: settings.recent_trades_per_symbol])

    async def _safe_recent_news() -> tuple[object, ...]:
        if settings.recent_news_per_symbol == 0:
            return ()
        try:
            items = await redis.recent_news(symbol)
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "trader_event_lookup.recent_news_failed",
                correlation_id=request.correlation_id,
                symbol=symbol,
                error=str(exc),
            )
            return ()
        return tuple(items[: settings.recent_news_per_symbol])

    async def _safe_get_regime() -> object | None:
        try:
            return await redis.get_regime()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "trader_event_lookup.get_regime_failed",
                correlation_id=request.correlation_id,
                error=str(exc),
            )
            return None

    async def _safe_get_stability() -> object | None:
        try:
            return await redis.get_stability_score()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "trader_event_lookup.get_stability_failed",
                correlation_id=request.correlation_id,
                error=str(exc),
            )
            return None

    trades, news, regime, stability = await asyncio.gather(
        _safe_recent_trades(),
        _safe_recent_news(),
        _safe_get_regime(),
        _safe_get_stability(),
    )
    return EventContext(
        request=request,
        recent_trades=trades,
        recent_news=news,
        current_regime=regime,
        current_stability_score=stability,
    )


async def _account_wide_lookup(
    request: RetrievalRequest,
    *,
    redis: "RedisHotCache | None",
) -> EventContext:
    """Hot-cache lookup branch for events without a symbol."""
    if redis is None:
        return EventContext(request=request)

    async def _safe_get_regime() -> object | None:
        try:
            return await redis.get_regime()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "trader_event_lookup.get_regime_failed",
                correlation_id=request.correlation_id,
                error=str(exc),
            )
            return None

    async def _safe_get_stability() -> object | None:
        try:
            return await redis.get_stability_score()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning(
                "trader_event_lookup.get_stability_failed",
                correlation_id=request.correlation_id,
                error=str(exc),
            )
            return None

    regime, stability = await asyncio.gather(_safe_get_regime(), _safe_get_stability())
    return EventContext(
        request=request,
        current_regime=regime,
        current_stability_score=stability,
    )


__all__ = ["trader_event_lookup"]
