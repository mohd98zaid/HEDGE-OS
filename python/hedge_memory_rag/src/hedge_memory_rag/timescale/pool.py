"""Async connection pool wrapper around ``asyncpg``.

The wrapper:

* registers an ``init`` callback on every new connection that switches the
  per-session ``statement_timeout`` to the value configured in
  :class:`TimescaleSettings`,
* exposes :meth:`TimescalePool.execute` / :meth:`fetch` / :meth:`fetchrow`
  helpers that always use prepared statements (we go through
  ``Connection.cursor`` / ``Connection.fetch`` which are prepared under
  the hood; and the explicit per-table writers in
  :mod:`hedge_memory_rag.timescale.writers` go through
  ``Connection.executemany`` which uses a prepared statement once per
  call),
* delegates ``close`` and async-context-manager semantics to the
  underlying ``asyncpg.Pool``.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, Self

import structlog

from .config_re_export import TimescaleSettings, load_timescale_settings

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg
    from collections.abc import AsyncIterator

_LOG: Final = structlog.get_logger(__name__)


class TimescalePoolError(RuntimeError):
    """Raised when the pool cannot be created or has been closed."""


class TimescalePool:
    """Thin async wrapper around an ``asyncpg.Pool``.

    Construct via :func:`create_pool`. Use as an async context manager:

    .. code-block:: python

        async with await create_pool() as pool:
            await pool.execute("SELECT 1")
    """

    def __init__(self, pool: "asyncpg.Pool", settings: TimescaleSettings) -> None:
        self._pool = pool
        self._settings = settings
        self._closed = False

    @property
    def settings(self) -> TimescaleSettings:
        return self._settings

    @property
    def raw(self) -> "asyncpg.Pool":
        """Return the underlying ``asyncpg.Pool`` (for migration runner usage)."""
        if self._closed:
            raise TimescalePoolError("pool is closed")
        return self._pool

    @asynccontextmanager
    async def acquire(self) -> "AsyncIterator[asyncpg.Connection]":
        """Acquire a connection for the duration of the context."""
        if self._closed:
            raise TimescalePoolError("pool is closed")
        async with self._pool.acquire() as conn:
            yield conn

    async def execute(self, query: str, *args: Any) -> str:
        """Run a query and return the asyncpg status string."""
        if self._closed:
            raise TimescalePoolError("pool is closed")
        return await self._pool.execute(query, *args)

    async def fetch(self, query: str, *args: Any) -> list["asyncpg.Record"]:
        if self._closed:
            raise TimescalePoolError("pool is closed")
        return await self._pool.fetch(query, *args)

    async def fetchrow(
        self, query: str, *args: Any
    ) -> "asyncpg.Record | None":
        if self._closed:
            raise TimescalePoolError("pool is closed")
        return await self._pool.fetchrow(query, *args)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._pool.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


async def create_pool(
    settings: TimescaleSettings | None = None,
) -> TimescalePool:
    """Create an :class:`TimescalePool` using ``settings`` (or env-loaded ones).

    The pool size and statement timeout are taken from the settings. The
    pool is created with ``init`` set to a coroutine that sets the
    per-session ``statement_timeout``; subsequent prepared statements
    inherit it.
    """
    import asyncpg

    cfg = settings or load_timescale_settings()
    statement_timeout_ms = cfg.statement_timeout_ms

    async def _init(conn: "asyncpg.Connection") -> None:
        if statement_timeout_ms > 0:
            # asyncpg requires the value as a string for SET LOCAL/SET.
            await conn.execute(f"SET statement_timeout = {int(statement_timeout_ms)}")

    try:
        pool = await asyncpg.create_pool(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            min_size=cfg.min_pool_size,
            max_size=cfg.max_pool_size,
            command_timeout=float(cfg.command_timeout_s),
            init=_init,
        )
    except Exception as exc:  # asyncpg.PostgresError, ConnectionError, ...
        raise TimescalePoolError(
            f"failed to create asyncpg pool to {cfg.host}:{cfg.port}/{cfg.database}: {exc}"
        ) from exc

    if pool is None:  # pragma: no cover - defensive
        raise TimescalePoolError(
            f"asyncpg.create_pool returned None for {cfg.host}:{cfg.port}/{cfg.database}"
        )

    _LOG.info(
        "timescale.pool.ready",
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        min_pool=cfg.min_pool_size,
        max_pool=cfg.max_pool_size,
        statement_timeout_ms=cfg.statement_timeout_ms,
    )
    return TimescalePool(pool=pool, settings=cfg)


# Re-export for static type checkers that resolve via the package init.
ConnectionContext = AbstractAsyncContextManager  # type: ignore[assignment]


__all__ = [
    "TimescalePool",
    "TimescalePoolError",
    "create_pool",
]
