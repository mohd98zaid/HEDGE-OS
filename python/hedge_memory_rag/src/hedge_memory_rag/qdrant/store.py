"""Async writer / reader / kNN helper for the Memory_RAG_Layer Qdrant collections.

This module is the single Memory_RAG_Layer entry point for vector
storage. It implements the contract demanded by task 31.1:

* Idempotent provisioning of the five canonical collections — ``trades``,
  ``news``, ``journal_entries``, ``market_memory``, ``psychology_history``
  (R19.1, R19.2). Existing collections are validated against the
  configured :class:`CollectionSpec`; mismatched dimensionality raises
  :class:`CollectionDimensionMismatchError` rather than silently
  recreating the collection.
* Async writers and readers (:meth:`upsert`, :meth:`upsert_batch`,
  :meth:`retrieve`, :meth:`delete`, :meth:`count`) wrapping the
  ``qdrant-client`` async API.
* CBOR encoding for embedding payloads (design § Data Models).
  Callers wrap their payloads with
  :func:`hedge_memory_rag.qdrant.codec.attach_embedding_cbor` (or the
  ``attach_embedding`` flag on the writers below).
* :meth:`knn_search` exposed to the Warm_AI_Pipeline retrieval
  pipeline (task 34.x) — supports an optional payload filter and
  configurable ``k``, ``with_payload``, ``with_vector``.

The Memory_RAG_Layer is reachable from the Warm_AI_Pipeline only and
**not** invoked synchronously by the Hot_Path (R19.7). The async API
makes that constraint mechanical: there is no synchronous wrapper.
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Any, Final, Iterable, Mapping, Sequence, Type

import numpy as np
import structlog

from .codec import attach_embedding_cbor, vector_to_floats
from .collections import (
    CollectionName,
    CollectionSpec,
    DistanceMetric,
    default_collection_specs,
)
from .config import QdrantSettings
from .errors import (
    CollectionDimensionMismatchError,
    QdrantClientError,
    QdrantConfigurationError,
    QdrantConnectionError,
)
from .records import KnnHit, PointId, VectorRecord

# Heavy imports are deferred to avoid pulling qdrant-client into every
# Warm_AI_Pipeline service that imports :mod:`hedge_memory_rag` for
# only its data classes. The module is small enough that a top-level
# import is acceptable, but the typing-only imports help mypy without
# the runtime cost.
from qdrant_client import AsyncQdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.http.models import models as qmodels


_LOG: Final = structlog.get_logger(__name__)

#: Default kNN result count when callers do not supply ``k``.
DEFAULT_KNN_K: Final[int] = 10


# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _to_qdrant_distance(distance: DistanceMetric) -> qmodels.Distance:
    """Coerce :class:`DistanceMetric` into the qdrant-client enum.

    The qdrant-client enum carries five members; this function only
    maps the three the design uses. Any unmapped value (e.g. a future
    addition) raises :class:`QdrantConfigurationError`.
    """
    mapping = {
        DistanceMetric.COSINE: qmodels.Distance.COSINE,
        DistanceMetric.EUCLID: qmodels.Distance.EUCLID,
        DistanceMetric.DOT: qmodels.Distance.DOT,
    }
    try:
        return mapping[distance]
    except KeyError as exc:  # pragma: no cover - guarded by enum
        raise QdrantConfigurationError(
            f"unsupported distance metric {distance!r}"
        ) from exc


def _filter_from_payload_match(
    payload_filter: Mapping[str, Any] | qmodels.Filter | None,
) -> qmodels.Filter | None:
    """Build a Qdrant ``Filter`` from a flat mapping of equality conditions.

    If ``payload_filter`` is already a :class:`qmodels.Filter`, it is
    returned unchanged so power users can construct complex filters
    with ``must_not`` / ``should`` clauses.

    The flat-mapping shorthand ``{"symbol": "RELIANCE", "side": "Buy"}``
    is the common case and produces an AND-combined ``must`` filter,
    which is sufficient for every retrieval-pipeline call planned in
    task 34.x.
    """
    if payload_filter is None:
        return None
    if isinstance(payload_filter, qmodels.Filter):
        return payload_filter

    must: list[qmodels.FieldCondition] = []
    for key, value in payload_filter.items():
        must.append(
            qmodels.FieldCondition(
                key=key, match=qmodels.MatchValue(value=value)
            )
        )
    return qmodels.Filter(must=must) if must else None


def _wrap_unexpected(exc: BaseException, *, collection: str | None) -> QdrantClientError:
    """Wrap a transport-layer exception into the typed hierarchy."""
    if isinstance(exc, QdrantClientError):
        return exc
    if isinstance(exc, UnexpectedResponse):
        return QdrantConnectionError(
            f"qdrant unexpected response (status={exc.status_code}): {exc!s}",
            collection=collection,
        )
    return QdrantConnectionError(str(exc) or exc.__class__.__name__, collection=collection)


# ---------------------------------------------------------------------------
# Store ---------------------------------------------------------------------
# ---------------------------------------------------------------------------


class MemoryRagQdrant:
    """Async Qdrant gateway for the Memory_RAG_Layer.

    Lifecycle::

        async with MemoryRagQdrant(settings=QdrantSettings.load()) as store:
            await store.ensure_collections()
            ...

    The class is **async-safe** (multiple coroutines may call
    :meth:`upsert` / :meth:`knn_search` concurrently) but not
    thread-safe — share one instance per :class:`asyncio.Task` tree.
    """

    def __init__(
        self,
        *,
        settings: QdrantSettings | None = None,
        specs: Mapping[CollectionName, CollectionSpec] | None = None,
        client: AsyncQdrantClient | None = None,
    ) -> None:
        """Construct the store.

        Args:
            settings: Connection + provisioning parameters. Defaults to
                :meth:`QdrantSettings.load` so production callers do not
                need to construct one explicitly.
            specs: Per-collection spec overrides. Defaults to
                :func:`default_collection_specs` populated with the
                per-collection vector-dimension overrides from
                ``settings.vector_dims``.
            client: Pre-built :class:`AsyncQdrantClient`. When ``None``
                (the common case), the store builds its own from
                :attr:`settings`. Tests inject a mock to avoid the
                network.
        """
        self._settings = settings or QdrantSettings.load()

        if specs is None:
            specs = default_collection_specs(
                vector_dim=self._settings.default_vector_dim,
                distance=self._settings.distance,
                overrides=self._settings.vector_dims,
            )

        # Validate uniqueness — duplicates in a custom override map
        # are a configuration bug and would silently overwrite each
        # other in the dict comprehension below.
        seen: set[str] = set()
        for spec in specs.values():
            if spec.name in seen:
                raise QdrantConfigurationError(
                    f"duplicate collection name in specs: {spec.name!r}"
                )
            seen.add(spec.name)

        self._specs: dict[CollectionName, CollectionSpec] = dict(specs)
        self._owns_client: bool = client is None
        self._client: AsyncQdrantClient | None = client
        self._closed: bool = False

    # -----------------------------------------------------------------
    # Async context-manager hooks --------------------------------------
    # -----------------------------------------------------------------

    async def __aenter__(self) -> "MemoryRagQdrant":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def start(self) -> None:
        """Lazily build the underlying :class:`AsyncQdrantClient`."""
        if self._client is not None:
            return
        s = self._settings
        self._client = AsyncQdrantClient(
            host=s.host,
            port=s.port,
            grpc_port=s.grpc_port,
            prefer_grpc=s.prefer_grpc,
            https=s.https,
            api_key=s.api_key,
            timeout=int(s.timeout_s),
        )

    async def aclose(self) -> None:
        """Close the owned client and mark the store as unusable."""
        if self._closed:
            return
        self._closed = True
        if self._owns_client and self._client is not None:
            try:
                await self._client.close()
            except Exception as exc:  # pragma: no cover - logged + swallowed
                _LOG.warning("qdrant_client_close_failed", error=str(exc))
        self._client = None

    # -----------------------------------------------------------------
    # Internal getters -------------------------------------------------
    # -----------------------------------------------------------------

    def _require_client(self) -> AsyncQdrantClient:
        if self._closed or self._client is None:
            raise QdrantConfigurationError(
                "MemoryRagQdrant has not been started — call `await store.start()` "
                "or use it as an async context manager."
            )
        return self._client

    def _spec(self, collection: CollectionName) -> CollectionSpec:
        try:
            return self._specs[collection]
        except KeyError as exc:
            raise QdrantConfigurationError(
                f"no spec configured for collection {collection.value!r}"
            ) from exc

    @property
    def settings(self) -> QdrantSettings:
        return self._settings

    @property
    def specs(self) -> Mapping[CollectionName, CollectionSpec]:
        """Read-only view of the configured per-collection specs."""
        return self._specs

    # -----------------------------------------------------------------
    # Provisioning -----------------------------------------------------
    # -----------------------------------------------------------------

    async def ensure_collections(
        self,
        collections: Iterable[CollectionName] | None = None,
    ) -> None:
        """Provision the requested collections, idempotently.

        For each collection:

        * If it does not exist, create it with the configured vector
          parameters.
        * If it exists with the configured vector parameters, do nothing.
        * If it exists with **different** parameters, raise
          :class:`CollectionDimensionMismatchError`.

        ``collections`` defaults to every spec configured on this store.
        The whole loop is bounded by :attr:`QdrantSettings.provision_timeout_s`.
        """
        client = self._require_client()
        targets = list(collections) if collections is not None else list(self._specs.keys())

        async def _ensure_one(name: CollectionName) -> None:
            spec = self._spec(name)
            try:
                exists = await client.collection_exists(spec.name)
            except Exception as exc:
                raise _wrap_unexpected(exc, collection=spec.name) from exc

            if not exists:
                await self._create_collection(spec)
                _LOG.info(
                    "qdrant_collection_created",
                    collection=spec.name,
                    vector_dim=spec.vector_dim,
                    distance=spec.distance.value,
                    on_disk=spec.on_disk,
                )
                return

            await self._validate_collection(spec)
            _LOG.debug(
                "qdrant_collection_validated",
                collection=spec.name,
                vector_dim=spec.vector_dim,
                distance=spec.distance.value,
            )

        try:
            await asyncio.wait_for(
                asyncio.gather(*[_ensure_one(t) for t in targets]),
                timeout=self._settings.provision_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise QdrantConnectionError(
                f"qdrant ensure_collections exceeded "
                f"{self._settings.provision_timeout_s:.1f}s budget"
            ) from exc

    async def _create_collection(self, spec: CollectionSpec) -> None:
        client = self._require_client()
        try:
            await client.create_collection(
                collection_name=spec.name,
                vectors_config=qmodels.VectorParams(
                    size=spec.vector_dim,
                    distance=_to_qdrant_distance(spec.distance),
                    on_disk=spec.on_disk,
                ),
            )
        except Exception as exc:
            raise _wrap_unexpected(exc, collection=spec.name) from exc

    async def _validate_collection(self, spec: CollectionSpec) -> None:
        client = self._require_client()
        try:
            info = await client.get_collection(spec.name)
        except Exception as exc:
            raise _wrap_unexpected(exc, collection=spec.name) from exc

        # The qdrant-client returns either a single ``VectorParams`` or
        # a mapping of named-vector configs depending on how the
        # collection was created. We only persist unnamed (default)
        # vectors, so a mapping result indicates a mismatch.
        params = info.config.params.vectors
        if params is None:
            raise CollectionDimensionMismatchError(
                f"collection {spec.name!r} has no vector params",
                collection=spec.name,
                expected_dim=spec.vector_dim,
                actual_dim=0,
                expected_distance=spec.distance.value,
                actual_distance="<none>",
            )
        if isinstance(params, dict):
            raise CollectionDimensionMismatchError(
                f"collection {spec.name!r} uses named vectors; expected unnamed",
                collection=spec.name,
                expected_dim=spec.vector_dim,
                actual_dim=-1,
                expected_distance=spec.distance.value,
                actual_distance="<named>",
            )

        actual_dim = int(params.size)
        actual_distance = params.distance.value if hasattr(params.distance, "value") else str(params.distance)
        if actual_dim != spec.vector_dim or actual_distance != spec.distance.value:
            raise CollectionDimensionMismatchError(
                f"collection {spec.name!r} vector params mismatch: "
                f"expected dim={spec.vector_dim} distance={spec.distance.value}, "
                f"got dim={actual_dim} distance={actual_distance}",
                collection=spec.name,
                expected_dim=spec.vector_dim,
                actual_dim=actual_dim,
                expected_distance=spec.distance.value,
                actual_distance=actual_distance,
            )

    # -----------------------------------------------------------------
    # Writers ----------------------------------------------------------
    # -----------------------------------------------------------------

    async def upsert(
        self,
        collection: CollectionName,
        record: VectorRecord,
        *,
        attach_embedding: bool = True,
        wait: bool = True,
    ) -> None:
        """Persist one :class:`VectorRecord` to ``collection``.

        Args:
            collection: Target collection.
            record: The record to upsert. ``record.payload`` may be empty;
                the canonical CBOR copy of the embedding is added under
                :data:`hedge_memory_rag.qdrant.codec.EMBEDDING_PAYLOAD_KEY`
                when ``attach_embedding=True`` (the default).
            attach_embedding: When ``True`` (the default), the CBOR-encoded
                embedding is included in the persisted payload alongside
                the indexed vector. Disable for cases where the caller
                has already attached a custom CBOR copy or wants to skip
                it entirely.
            wait: Block until Qdrant confirms the write. The Memory_RAG_Layer
                always awaits writes by default to satisfy *Property 5:
                Serialization and Persistence Round-Trip*.
        """
        await self.upsert_batch(
            collection,
            [record],
            attach_embedding=attach_embedding,
            wait=wait,
        )

    async def upsert_batch(
        self,
        collection: CollectionName,
        records: Sequence[VectorRecord],
        *,
        attach_embedding: bool = True,
        wait: bool = True,
    ) -> None:
        """Persist a batch of :class:`VectorRecord` values."""
        if not records:
            return
        client = self._require_client()
        spec = self._spec(collection)

        points: list[qmodels.PointStruct] = []
        for record in records:
            vector = vector_to_floats(record.vector)
            if len(vector) != spec.vector_dim:
                raise QdrantConfigurationError(
                    f"collection {collection.value!r}: vector length {len(vector)} "
                    f"does not match configured dim {spec.vector_dim}"
                )
            payload: dict[str, Any]
            if attach_embedding:
                payload = attach_embedding_cbor(dict(record.payload), record.vector)
            else:
                payload = dict(record.payload)
            points.append(
                qmodels.PointStruct(
                    id=record.point_id,
                    vector=vector,
                    payload=payload,
                )
            )

        try:
            await client.upsert(
                collection_name=spec.name,
                points=points,
                wait=wait,
            )
        except Exception as exc:
            raise _wrap_unexpected(exc, collection=spec.name) from exc

    async def delete(
        self,
        collection: CollectionName,
        point_ids: Sequence[PointId],
        *,
        wait: bool = True,
    ) -> None:
        """Delete one or more points by id."""
        if not point_ids:
            return
        client = self._require_client()
        spec = self._spec(collection)
        try:
            await client.delete(
                collection_name=spec.name,
                points_selector=qmodels.PointIdsList(points=list(point_ids)),
                wait=wait,
            )
        except Exception as exc:
            raise _wrap_unexpected(exc, collection=spec.name) from exc

    # -----------------------------------------------------------------
    # Readers ----------------------------------------------------------
    # -----------------------------------------------------------------

    async def retrieve(
        self,
        collection: CollectionName,
        point_ids: Sequence[PointId],
        *,
        with_vector: bool = False,
    ) -> list[KnnHit]:
        """Fetch points by id. Useful for write-then-read round-trip tests.

        Returns a list of :class:`KnnHit` (with ``score=0.0`` because
        retrieval is not similarity-scored). The list preserves the
        order Qdrant returns them in (id-sorted, not request-order).
        """
        if not point_ids:
            return []
        client = self._require_client()
        spec = self._spec(collection)
        try:
            results = await client.retrieve(
                collection_name=spec.name,
                ids=list(point_ids),
                with_payload=True,
                with_vectors=with_vector,
            )
        except Exception as exc:
            raise _wrap_unexpected(exc, collection=spec.name) from exc

        hits: list[KnnHit] = []
        for record in results:
            vec: list[float] | None = None
            if with_vector and record.vector is not None:
                # ``record.vector`` may be a list[float] (unnamed) or a
                # dict[str, list[float]] (named). We only persist
                # unnamed vectors, so dict means a misconfigured
                # collection — treat it as None defensively.
                if isinstance(record.vector, list):
                    vec = [float(x) for x in record.vector]
            hits.append(
                KnnHit(
                    point_id=record.id,
                    score=0.0,
                    payload=record.payload or {},
                    vector=vec,
                )
            )
        return hits

    async def count(self, collection: CollectionName, *, exact: bool = True) -> int:
        """Return the number of points in ``collection``."""
        client = self._require_client()
        spec = self._spec(collection)
        try:
            result = await client.count(collection_name=spec.name, exact=exact)
        except Exception as exc:
            raise _wrap_unexpected(exc, collection=spec.name) from exc
        return int(result.count)

    # -----------------------------------------------------------------
    # kNN search -------------------------------------------------------
    # -----------------------------------------------------------------

    async def knn_search(
        self,
        collection: CollectionName,
        query_vector: Sequence[float] | np.ndarray,
        k: int = DEFAULT_KNN_K,
        *,
        payload_filter: Mapping[str, Any] | qmodels.Filter | None = None,
        with_payload: bool = True,
        with_vector: bool = False,
        score_threshold: float | None = None,
    ) -> list[KnnHit]:
        """Run a kNN query against ``collection`` and return the top ``k`` hits.

        This is the public surface consumed by the Warm_AI_Pipeline
        retrieval pipeline (task 34.x). The Memory_RAG_Layer guarantees
        (R19.7) that the Hot_Path never reaches it directly — all
        callers run inside the Warm_AI_Pipeline.

        Args:
            collection: Target collection.
            query_vector: 1-D query embedding. Length must match the
                configured vector dimension; mismatches raise
                :class:`QdrantConfigurationError` rather than letting
                the daemon return a confusing 4xx.
            k: Maximum number of results. Must be ``>= 1``.
            payload_filter: Optional filter:
                * ``None`` — no filter.
                * ``Mapping[str, Any]`` — flat AND-combined equality
                  filter, e.g. ``{"symbol": "RELIANCE"}``.
                * :class:`qmodels.Filter` — full Qdrant filter for
                  callers that need ``must_not`` / ``should`` /
                  ranges.
            with_payload: Include the stored payload in each hit.
                The CBOR-encoded canonical embedding lives in the
                payload under
                :data:`hedge_memory_rag.qdrant.codec.EMBEDDING_PAYLOAD_KEY`
                so callers that need the exact stored vector should
                keep this ``True``.
            with_vector: Include the indexed (post-quantisation) vector.
                Costs extra bandwidth; defaults to ``False``.
            score_threshold: When set, hits with a similarity score
                below this threshold are suppressed.

        Returns:
            List of :class:`KnnHit` ordered by descending similarity
            score. The list may be shorter than ``k`` when fewer
            matches exist or ``score_threshold`` filters them out.
        """
        if k < 1:
            raise QdrantConfigurationError(f"k must be >= 1, got {k!r}")

        spec = self._spec(collection)
        vector = vector_to_floats(query_vector)
        if len(vector) != spec.vector_dim:
            raise QdrantConfigurationError(
                f"collection {collection.value!r}: query vector length {len(vector)} "
                f"does not match configured dim {spec.vector_dim}"
            )

        client = self._require_client()
        try:
            response = await client.query_points(
                collection_name=spec.name,
                query=vector,
                limit=k,
                query_filter=_filter_from_payload_match(payload_filter),
                with_payload=with_payload,
                with_vectors=with_vector,
                score_threshold=score_threshold,
            )
        except Exception as exc:
            raise _wrap_unexpected(exc, collection=spec.name) from exc

        hits: list[KnnHit] = []
        for point in response.points:
            vec: list[float] | None = None
            if with_vector and point.vector is not None and isinstance(point.vector, list):
                vec = [float(x) for x in point.vector]
            hits.append(
                KnnHit(
                    point_id=point.id,
                    score=float(point.score),
                    payload=point.payload or {},
                    vector=vec,
                )
            )
        return hits


__all__ = [
    "DEFAULT_KNN_K",
    "MemoryRagQdrant",
]
