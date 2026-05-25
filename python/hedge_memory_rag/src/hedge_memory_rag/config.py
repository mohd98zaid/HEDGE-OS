"""Connection settings for the Memory_RAG_Layer.

Configuration is loaded from environment variables only — values are
**never** hardcoded in code. The fallbacks below match the dev profile
defaults in ``docker-compose.yml`` (user/password ``hedge``/``hedge`` on
``postgres:5432/hedge``); they let test rigs and the local compose stack
boot, but production deployments must override every setting via the
container environment.

Two equivalent sources are accepted:

* ``HEDGE_POSTGRES_URL`` — a libpq DSN (``postgresql://user:pw@host:port/db``).
  This is what ``docker-compose.yml`` sets today (R29.4 deployment surface).
* Discrete ``HEDGE_POSTGRES_*`` fields (``HOST``, ``PORT``, ``DB``, ``USER``,
  ``PASSWORD``) — useful for operators that prefer split env vars.

The DSN form wins when both are provided.

References:
- Design § Memory_RAG_Layer (R19.3)
- Requirement 19.1, 19.3
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final
from urllib.parse import quote, urlparse

# --- Environment variable names -------------------------------------------

_ENV_DSN: Final[str] = "HEDGE_POSTGRES_URL"
_ENV_HOST: Final[str] = "HEDGE_POSTGRES_HOST"
_ENV_PORT: Final[str] = "HEDGE_POSTGRES_PORT"
_ENV_DB: Final[str] = "HEDGE_POSTGRES_DB"
_ENV_USER: Final[str] = "HEDGE_POSTGRES_USER"
_ENV_PASSWORD: Final[str] = "HEDGE_POSTGRES_PASSWORD"
_ENV_MIN_POOL: Final[str] = "HEDGE_POSTGRES_MIN_POOL"
_ENV_MAX_POOL: Final[str] = "HEDGE_POSTGRES_MAX_POOL"
_ENV_STATEMENT_TIMEOUT_MS: Final[str] = "HEDGE_POSTGRES_STATEMENT_TIMEOUT_MS"
_ENV_COMMAND_TIMEOUT_S: Final[str] = "HEDGE_POSTGRES_COMMAND_TIMEOUT_S"

# Dev-profile defaults that mirror docker-compose. Production deployments
# MUST override these through environment variables. They exist solely so
# the local stack and tests can boot without a wall of required env vars.
_DEFAULT_HOST: Final[str] = "postgres"
_DEFAULT_PORT: Final[int] = 5432
_DEFAULT_DB: Final[str] = "hedge"
_DEFAULT_USER: Final[str] = "hedge"
_DEFAULT_PASSWORD: Final[str] = "hedge"


class TimescaleConfigError(Exception):
    """Raised when an environment override is malformed."""


def _parse_int(name: str, raw: str | None, default: int, *, minimum: int = 0) -> int:
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise TimescaleConfigError(f"{name}={raw!r} is not a valid integer") from exc
    if value < minimum:
        raise TimescaleConfigError(f"{name}={value} must be >= {minimum}")
    return value


@dataclass(frozen=True, slots=True)
class TimescaleSettings:
    """Resolved connection settings for the Timescale hypertables.

    Created by :func:`load_timescale_settings`. All fields are deliberately public so
    tests can construct instances directly without setting env vars.
    """

    host: str
    port: int
    database: str
    user: str
    password: str
    min_pool_size: int = 1
    max_pool_size: int = 10
    statement_timeout_ms: int = 5_000
    command_timeout_s: int = 10

    def to_dsn(self) -> str:
        """Build a libpq DSN with URL-quoted user and password."""
        user = quote(self.user, safe="")
        password = quote(self.password, safe="")
        return (
            f"postgresql://{user}:{password}@"
            f"{self.host}:{self.port}/{self.database}"
        )


def _from_dsn(dsn: str, *, env: dict[str, str]) -> TimescaleSettings:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise TimescaleConfigError(
            f"{_ENV_DSN}={dsn!r} must use a postgres:// or postgresql:// scheme"
        )
    if not parsed.hostname:
        raise TimescaleConfigError(f"{_ENV_DSN}={dsn!r} must include a host")
    db = (parsed.path or "/").lstrip("/")
    if not db:
        raise TimescaleConfigError(f"{_ENV_DSN}={dsn!r} must include a database")
    return TimescaleSettings(
        host=parsed.hostname,
        port=parsed.port or _DEFAULT_PORT,
        database=db,
        user=parsed.username or _DEFAULT_USER,
        password=parsed.password or _DEFAULT_PASSWORD,
        min_pool_size=_parse_int(_ENV_MIN_POOL, env.get(_ENV_MIN_POOL), 1, minimum=1),
        max_pool_size=_parse_int(_ENV_MAX_POOL, env.get(_ENV_MAX_POOL), 10, minimum=1),
        statement_timeout_ms=_parse_int(
            _ENV_STATEMENT_TIMEOUT_MS, env.get(_ENV_STATEMENT_TIMEOUT_MS), 5_000, minimum=0
        ),
        command_timeout_s=_parse_int(
            _ENV_COMMAND_TIMEOUT_S, env.get(_ENV_COMMAND_TIMEOUT_S), 10, minimum=1
        ),
    )


def _from_discrete(env: dict[str, str]) -> TimescaleSettings:
    port = _parse_int(_ENV_PORT, env.get(_ENV_PORT), _DEFAULT_PORT, minimum=1)
    return TimescaleSettings(
        host=env.get(_ENV_HOST, _DEFAULT_HOST),
        port=port,
        database=env.get(_ENV_DB, _DEFAULT_DB),
        user=env.get(_ENV_USER, _DEFAULT_USER),
        password=env.get(_ENV_PASSWORD, _DEFAULT_PASSWORD),
        min_pool_size=_parse_int(_ENV_MIN_POOL, env.get(_ENV_MIN_POOL), 1, minimum=1),
        max_pool_size=_parse_int(_ENV_MAX_POOL, env.get(_ENV_MAX_POOL), 10, minimum=1),
        statement_timeout_ms=_parse_int(
            _ENV_STATEMENT_TIMEOUT_MS, env.get(_ENV_STATEMENT_TIMEOUT_MS), 5_000, minimum=0
        ),
        command_timeout_s=_parse_int(
            _ENV_COMMAND_TIMEOUT_S, env.get(_ENV_COMMAND_TIMEOUT_S), 10, minimum=1
        ),
    )


def load_timescale_settings(env: dict[str, str] | None = None) -> TimescaleSettings:
    """Load Timescale connection settings from the environment.

    The DSN form wins when ``HEDGE_POSTGRES_URL`` is set; otherwise discrete
    ``HEDGE_POSTGRES_*`` variables are used, falling back to the dev-profile
    defaults baked into this module.

    Args:
        env: Optional override dict (defaults to :data:`os.environ`).

    Raises:
        TimescaleConfigError: when a value is malformed or out of range.
    """
    source = env if env is not None else dict(os.environ)
    dsn = source.get(_ENV_DSN, "").strip()
    settings = _from_dsn(dsn, env=source) if dsn else _from_discrete(source)
    if settings.min_pool_size > settings.max_pool_size:
        raise TimescaleConfigError(
            f"{_ENV_MIN_POOL}={settings.min_pool_size} must be "
            f"<= {_ENV_MAX_POOL}={settings.max_pool_size}"
        )
    return settings


__all__ = [
    "TimescaleConfigError",
    "TimescaleSettings",
    "load_timescale_settings",
]
