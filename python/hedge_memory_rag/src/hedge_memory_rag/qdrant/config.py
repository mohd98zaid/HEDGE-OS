"""Settings for the Qdrant client (R19.2).

The Memory_RAG_Layer reads its connection string from the same
environment surface every other PROJECT HEDGE service uses
(``HEDGE_QDRANT_URL`` per ``docker-compose.yml``). Per-collection
vector dimensions can be overridden through the typed
:class:`hedge_warm_ai.config.HedgeConfig` mirror once that surface is
extended (R32 surfaces in tasks 6.x); until then this module exposes a
small, dependency-light :class:`QdrantSettings` that

* Accepts an explicit URL (``http://qdrant:6333``) **or** host + port.
* Falls back to ``HEDGE_QDRANT_URL`` then to a sensible local default.
* Validates the dimension and timeout invariants up front.
* Carries an optional API key (Qdrant Cloud / mTLS deployments).

Hardcoded credentials and hosts are explicitly avoided per the task brief.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final
from urllib.parse import urlparse

from .collections import DEFAULT_VECTOR_DIM, CollectionName, DistanceMetric
from .errors import QdrantConfigurationError

#: Environment variable matching ``docker-compose.yml`` env wiring.
ENV_QDRANT_URL: Final[str] = "HEDGE_QDRANT_URL"
#: Optional override when only host is exposed (e.g. via secret manager).
ENV_QDRANT_HOST: Final[str] = "HEDGE_QDRANT_HOST"
ENV_QDRANT_PORT: Final[str] = "HEDGE_QDRANT_PORT"
ENV_QDRANT_GRPC_PORT: Final[str] = "HEDGE_QDRANT_GRPC_PORT"
#: Optional API key for Qdrant Cloud / authenticated deployments. The
#: value is read from the environment only — never logged or persisted.
ENV_QDRANT_API_KEY: Final[str] = "HEDGE_QDRANT_API_KEY"

#: Conservative defaults for a co-located Qdrant container as defined by
#: ``docker-compose.yml``. The Hot_Path never reaches Qdrant directly
#: (R19.7), so these defaults exist purely for the Warm_AI_Pipeline.
DEFAULT_HOST: Final[str] = "qdrant"
DEFAULT_HTTP_PORT: Final[int] = 6333
DEFAULT_GRPC_PORT: Final[int] = 6334


@dataclass(frozen=True, slots=True)
class QdrantSettings:
    """Connection + provisioning parameters for :class:`MemoryRagQdrant`.

    Attributes:
        host: Hostname of the Qdrant daemon.
        port: HTTP/REST port (default 6333). The async client uses gRPC
            when ``prefer_grpc=True`` but the REST port is always set
            because the lib falls back to REST for collection-management
            calls in older deployments.
        grpc_port: gRPC port (default 6334). Used when ``prefer_grpc``
            is enabled — the gRPC transport is faster for bulk upsert
            but does not affect kNN latency materially.
        prefer_grpc: When ``True``, the async client uses gRPC for
            data-plane calls and REST for control-plane calls.
        https: ``True`` for TLS-fronted deployments. Defaults to the
            scheme parsed from ``HEDGE_QDRANT_URL`` when provided.
        api_key: Optional API key. Sourced from
            :envvar:`HEDGE_QDRANT_API_KEY` and never logged.
        timeout_s: Per-operation timeout in seconds. Applies to every
            client call (provisioning, upsert, search).
        default_vector_dim: Default vector dimension applied to every
            collection unless overridden via :attr:`vector_dims`.
        distance: Default distance metric for collection provisioning.
        vector_dims: Per-collection override for the vector dimension.
            Keys are :class:`CollectionName` members.
        provision_timeout_s: Wall-clock budget for the idempotent
            ``ensure_collections`` startup routine. Larger than
            :attr:`timeout_s` because it covers five HTTP round-trips
            on a cold daemon.

    Construct via :meth:`load` or build manually for tests.
    """

    host: str = DEFAULT_HOST
    port: int = DEFAULT_HTTP_PORT
    grpc_port: int = DEFAULT_GRPC_PORT
    prefer_grpc: bool = False
    https: bool = False
    api_key: str | None = None
    timeout_s: float = 10.0
    default_vector_dim: int = DEFAULT_VECTOR_DIM
    distance: DistanceMetric = DistanceMetric.COSINE
    vector_dims: dict[CollectionName, int] = field(default_factory=dict)
    provision_timeout_s: float = 30.0

    def __post_init__(self) -> None:
        if not self.host:
            raise QdrantConfigurationError("QdrantSettings.host must be a non-empty string")
        if not (1 <= self.port <= 65535):
            raise QdrantConfigurationError(
                f"QdrantSettings.port must be in [1, 65535], got {self.port!r}"
            )
        if not (1 <= self.grpc_port <= 65535):
            raise QdrantConfigurationError(
                f"QdrantSettings.grpc_port must be in [1, 65535], got {self.grpc_port!r}"
            )
        if self.timeout_s <= 0:
            raise QdrantConfigurationError(
                f"QdrantSettings.timeout_s must be > 0, got {self.timeout_s!r}"
            )
        if self.provision_timeout_s <= 0:
            raise QdrantConfigurationError(
                "QdrantSettings.provision_timeout_s must be > 0, "
                f"got {self.provision_timeout_s!r}"
            )
        if self.default_vector_dim <= 0:
            raise QdrantConfigurationError(
                "QdrantSettings.default_vector_dim must be > 0, "
                f"got {self.default_vector_dim!r}"
            )
        for collection, dim in self.vector_dims.items():
            if dim <= 0:
                raise QdrantConfigurationError(
                    f"QdrantSettings.vector_dims[{collection.value!r}] must be > 0, got {dim!r}"
                )

    # -----------------------------------------------------------------
    # Loaders ----------------------------------------------------------
    # -----------------------------------------------------------------

    @classmethod
    def load(
        cls,
        *,
        env: dict[str, str] | None = None,
        default_vector_dim: int = DEFAULT_VECTOR_DIM,
        distance: DistanceMetric = DistanceMetric.COSINE,
        vector_dims: dict[CollectionName, int] | None = None,
        prefer_grpc: bool = False,
        timeout_s: float = 10.0,
        provision_timeout_s: float = 30.0,
    ) -> "QdrantSettings":
        """Build :class:`QdrantSettings` from environment variables.

        Resolution order:

        1. ``HEDGE_QDRANT_URL`` is parsed for scheme + host + port if set.
        2. ``HEDGE_QDRANT_HOST`` / ``HEDGE_QDRANT_PORT`` /
           ``HEDGE_QDRANT_GRPC_PORT`` override individual fields.
        3. ``HEDGE_QDRANT_API_KEY`` is consumed when present.
        4. Anything left unspecified falls back to the local-compose
           defaults (``qdrant:6333``).

        ``env`` exists so tests can pass a synthetic environment without
        mutating ``os.environ``. Production callers leave it ``None``.
        """
        environ = env if env is not None else os.environ

        host: str = DEFAULT_HOST
        port: int = DEFAULT_HTTP_PORT
        grpc_port: int = DEFAULT_GRPC_PORT
        https: bool = False

        url = environ.get(ENV_QDRANT_URL)
        if url:
            parsed = urlparse(url)
            if not parsed.hostname:
                raise QdrantConfigurationError(
                    f"{ENV_QDRANT_URL}={url!r} is missing a hostname"
                )
            host = parsed.hostname
            https = parsed.scheme == "https"
            if parsed.port is not None:
                port = parsed.port

        # Explicit overrides win over the URL-derived values.
        if (raw_host := environ.get(ENV_QDRANT_HOST)):
            host = raw_host
        if (raw_port := environ.get(ENV_QDRANT_PORT)):
            try:
                port = int(raw_port)
            except ValueError as exc:
                raise QdrantConfigurationError(
                    f"{ENV_QDRANT_PORT}={raw_port!r} is not a valid port number"
                ) from exc
        if (raw_grpc := environ.get(ENV_QDRANT_GRPC_PORT)):
            try:
                grpc_port = int(raw_grpc)
            except ValueError as exc:
                raise QdrantConfigurationError(
                    f"{ENV_QDRANT_GRPC_PORT}={raw_grpc!r} is not a valid port number"
                ) from exc

        api_key = environ.get(ENV_QDRANT_API_KEY) or None

        return cls(
            host=host,
            port=port,
            grpc_port=grpc_port,
            prefer_grpc=prefer_grpc,
            https=https,
            api_key=api_key,
            timeout_s=timeout_s,
            default_vector_dim=default_vector_dim,
            distance=distance,
            vector_dims=dict(vector_dims or {}),
            provision_timeout_s=provision_timeout_s,
        )

    # -----------------------------------------------------------------
    # Convenience ------------------------------------------------------
    # -----------------------------------------------------------------

    @property
    def url(self) -> str:
        """Compose an HTTP URL from host / port / scheme. Used in logs only."""
        scheme = "https" if self.https else "http"
        return f"{scheme}://{self.host}:{self.port}"

    def vector_dim_for(self, collection: CollectionName) -> int:
        """Return the configured vector dimension for ``collection``."""
        return self.vector_dims.get(collection, self.default_vector_dim)


__all__ = [
    "DEFAULT_HOST",
    "DEFAULT_GRPC_PORT",
    "DEFAULT_HTTP_PORT",
    "ENV_QDRANT_API_KEY",
    "ENV_QDRANT_GRPC_PORT",
    "ENV_QDRANT_HOST",
    "ENV_QDRANT_PORT",
    "ENV_QDRANT_URL",
    "QdrantSettings",
]
