"""Async migration runner for the Memory_RAG_Layer Timescale tables.

The runner:

* ensures the bookkeeping table ``hedge_memory_rag.schema_migrations``
  exists,
* iterates the bundled SQL files under
  :mod:`hedge_memory_rag.timescale.migrations` in lexicographic order,
* skips files that are already recorded as applied,
* applies new files inside a single transaction, then records them.

Each SQL file is itself idempotent (uses ``CREATE ... IF NOT EXISTS`` and
``create_hypertable(..., if_not_exists => TRUE)``) so re-runs on a partially
migrated database remain safe.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from importlib import resources
from typing import TYPE_CHECKING, Final

import structlog

from .models import HYPERTABLE_NAMES

if TYPE_CHECKING:  # pragma: no cover - typing only
    import asyncpg

_LOG: Final = structlog.get_logger(__name__)

_MIGRATIONS_PACKAGE: Final[str] = "hedge_memory_rag.timescale.migrations"

_BOOKKEEPING_DDL: Final[str] = """
CREATE SCHEMA IF NOT EXISTS hedge_memory_rag;

CREATE TABLE IF NOT EXISTS hedge_memory_rag.schema_migrations (
    name        TEXT        PRIMARY KEY,
    sha256      TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class MigrationError(RuntimeError):
    """Raised when a migration cannot be applied."""


def iter_migration_files() -> Iterator[tuple[str, str]]:
    """Yield ``(name, body)`` pairs for every bundled SQL migration.

    Returned in lexicographic order, which matches our ``NNN_*.sql``
    naming convention.
    """
    package = resources.files(_MIGRATIONS_PACKAGE)
    files = sorted(
        (entry.name for entry in package.iterdir() if entry.name.endswith(".sql")),
    )
    for name in files:
        body = package.joinpath(name).read_text(encoding="utf-8")
        yield name, body


def _sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


async def apply_migrations(conn: "asyncpg.Connection | asyncpg.pool.Pool") -> list[str]:
    """Apply every pending Timescale migration. Returns the names applied.

    Accepts either a single ``Connection`` or a ``Pool``. Pool callers go
    through ``pool.acquire()`` so the bookkeeping insert and the migration
    body run in the same connection, but we don't wrap pool acquisition
    in a transaction — each migration file gets its own transaction below.
    """
    # Local import keeps asyncpg out of the import path until first use,
    # which matters for downstream packages that vendor stubs.
    import asyncpg  # noqa: F401  (used only for type narrowing)

    applied: list[str] = []

    async def _run(executor: "asyncpg.Connection") -> None:
        await executor.execute(_BOOKKEEPING_DDL)
        already = {
            row["name"]
            for row in await executor.fetch(
                "SELECT name FROM hedge_memory_rag.schema_migrations"
            )
        }
        for name, body in iter_migration_files():
            if name in already:
                _LOG.debug("timescale.migration.skip", name=name)
                continue
            digest = _sha256(body)
            _LOG.info("timescale.migration.apply", name=name, sha256=digest)
            try:
                async with executor.transaction():
                    await executor.execute(body)
                    await executor.execute(
                        """
                        INSERT INTO hedge_memory_rag.schema_migrations (name, sha256)
                        VALUES ($1, $2)
                        """,
                        name,
                        digest,
                    )
            except Exception as exc:  # asyncpg.PostgresError or anything else
                raise MigrationError(
                    f"failed to apply migration {name!r}: {exc}"
                ) from exc
            applied.append(name)

    # Duck-type the executor: pools expose `acquire()`, raw connections do not.
    acquire = getattr(conn, "acquire", None)
    if callable(acquire):
        async with conn.acquire() as held:
            await _run(held)
    else:
        await _run(conn)
    return applied


async def list_hypertables(
    conn: "asyncpg.Connection | asyncpg.pool.Pool",
) -> list[str]:
    """Return the canonical hypertable names that exist in the database."""
    query = """
        SELECT hypertable_name
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = ANY($1)
        ORDER BY hypertable_name
    """
    acquire = getattr(conn, "acquire", None)
    if callable(acquire):
        async with conn.acquire() as held:
            rows = await held.fetch(query, list(HYPERTABLE_NAMES))
    else:
        rows = await conn.fetch(query, list(HYPERTABLE_NAMES))
    return [row["hypertable_name"] for row in rows]


__all__ = [
    "MigrationError",
    "apply_migrations",
    "iter_migration_files",
    "list_hypertables",
]
