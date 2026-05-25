"""Qdrant ``news`` collection sink for headline embeddings (R19.2).

The News_Intelligence_Engine persists every headline's mean-pooled
DistilBERT embedding (R11.2, task 20.1) into the Memory_RAG_Layer's
``news`` Qdrant collection (task 31.1). Downstream subsystems
consume this as part of the retrieval pipeline (task 34.x):

* The slow path's reasoning context can include topically similar
  past headlines.
* The AI_Trade_Journal_Engine pulls correlated headlines when
  composing post-trade narratives (R18.2).
* The shadow-mode evaluation harness uses the embedding for offline
  similarity backtests.

This module wraps :class:`hedge_memory_rag.qdrant.MemoryRagQdrant`
behind the same protocol-based pattern the rest of the news
subpackage uses (publisher, slow-path sink), so:

1. Tests substitute :class:`InMemoryNewsEmbeddingSink` and assert on
   captured records without spinning up Qdrant.
2. Production wires :class:`QdrantNewsEmbeddingSink` around an
   already-started :class:`MemoryRagQdrant`.

The Memory_RAG_Layer is **off the Hot_Path** by construction (R19.7)
— the engine schedules the upsert as a background task so it never
adds to the fast-path budget.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Any, Final, Mapping, Protocol, Sequence

import numpy as np
import structlog

from .config import DEFAULT_NEWS_QDRANT_COLLECTION
from .errors import NewsQdrantError
from .fast_path import FastPathResult
from .headline import Headline

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Sink protocol -------------------------------------------------------------
# ---------------------------------------------------------------------------


class NewsEmbeddingSink(Protocol):
    """Sink that receives headline embeddings for persistence.

    Implementations MUST:

    * Persist each ``(headline, fast, embedding)`` triple to whatever
      durable store the deployment uses for the ``news`` collection.
    * Treat persistence failures as non-fatal — the engine logs and
      continues so a transient Qdrant outage does not block the
      fast-path emission. Implementations that need to surface
      failure can raise :class:`NewsQdrantError`; the engine catches
      it and routes it through the structured log to the supervisor.
    """

    async def upsert_embedding(
        self,
        *,
        headline: Headline,
        fast: FastPathResult,
        embedding: Sequence[float],
    ) -> None: ...


class NoopNewsEmbeddingSink:
    """Discards every record. Default while no Qdrant is available."""

    async def upsert_embedding(
        self,
        *,
        headline: Headline,
        fast: FastPathResult,
        embedding: Sequence[float],
    ) -> None:  # noqa: D401
        return


class InMemoryNewsEmbeddingSink:
    """Captures every upsert in memory for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._records: list[tuple[Headline, FastPathResult, list[float]]] = []

    async def upsert_embedding(
        self,
        *,
        headline: Headline,
        fast: FastPathResult,
        embedding: Sequence[float],
    ) -> None:
        with self._lock:
            self._records.append(
                (headline, fast, [float(x) for x in embedding])
            )

    @property
    def records(self) -> list[tuple[Headline, FastPathResult, list[float]]]:
        with self._lock:
            return list(self._records)

    def reset(self) -> None:
        with self._lock:
            self._records.clear()


# ---------------------------------------------------------------------------
# Qdrant-backed implementation ---------------------------------------------
# ---------------------------------------------------------------------------


@dataclass
class QdrantNewsEmbeddingSink:
    """Qdrant-backed :class:`NewsEmbeddingSink`.

    The sink builds a :class:`hedge_memory_rag.qdrant.VectorRecord`
    per call and writes it into the configured collection. The
    point id is the :attr:`Headline.headline_id` so re-fetching the
    same headline (e.g. on a feed retry) idempotently overwrites the
    earlier record rather than inserting a duplicate.

    The payload mirrors the design's traceability rule: every
    persisted record carries the headline source, primary symbol,
    fast-path sentiment and impact magnitude, and the producer-side
    timestamp.

    Construction:

    * ``store`` — already-started
      :class:`hedge_memory_rag.qdrant.MemoryRagQdrant`. The store
      lifecycle (``await store.start()``, ``ensure_collections``,
      ``aclose``) is owned by the service binary, not the sink.
    * ``collection`` — collection name. Defaults to
      :data:`DEFAULT_NEWS_QDRANT_COLLECTION` (``"news"``); deployments
      with multiple collections (e.g. shadow vs production) override
      via :class:`hedge_warm_ai.news.config.NewsConfig.qdrant_collection`.
    """

    store: Any  # hedge_memory_rag.qdrant.MemoryRagQdrant — typed at runtime
    collection: str = DEFAULT_NEWS_QDRANT_COLLECTION

    async def upsert_embedding(
        self,
        *,
        headline: Headline,
        fast: FastPathResult,
        embedding: Sequence[float],
    ) -> None:
        # Lazy import keeps ``hedge_memory_rag`` an optional runtime
        # dependency — the news subpackage remains importable in
        # environments that do not have it installed.
        try:
            from hedge_memory_rag.qdrant import (  # type: ignore[import-not-found]
                CollectionName,
                VectorRecord,
            )
        except ImportError as exc:  # pragma: no cover - exercised at runtime
            raise NewsQdrantError(
                "QdrantNewsEmbeddingSink requires hedge_memory_rag.qdrant; "
                "install hedge-memory-rag in this environment."
            ) from exc

        # Coerce numpy arrays to a flat list[float] so the qdrant
        # record stays JSON-serialisable through CBOR.
        if isinstance(embedding, np.ndarray):
            vector: list[float] = [float(x) for x in embedding.tolist()]
        else:
            vector = [float(x) for x in embedding]

        if not vector:
            raise NewsQdrantError(
                f"refusing to upsert empty embedding for headline_id="
                f"{headline.headline_id!r}"
            )

        payload: Mapping[str, Any] = {
            "headline_id": headline.headline_id,
            "source": headline.source.value,
            "text": headline.text,
            "url": headline.url,
            "symbol": fast.mapping.primary or "",
            "symbols": list(fast.mapping.symbols),
            "sentiment": fast.sentiment.sentiment,
            "sentiment_label": fast.sentiment.label,
            "impact_magnitude": fast.impact_magnitude,
            "keywords_hit": list(fast.entities.keywords_hit),
            "ts_ns": int(headline.ts_ns),
        }
        record = VectorRecord(
            point_id=headline.headline_id,
            vector=vector,
            payload=payload,
        )
        target = self._resolve_collection(CollectionName)
        try:
            await self.store.upsert(target, record)
        except Exception as exc:
            raise NewsQdrantError(
                f"qdrant upsert failed for collection={self.collection!r}, "
                f"headline_id={headline.headline_id!r}: {exc}"
            ) from exc

    def _resolve_collection(self, enum_cls: Any) -> Any:
        """Map :attr:`collection` (a string) to the enum the store expects."""
        # Direct value match first — covers the ``"news"`` default and
        # any future canonical name added to the enum.
        for member in enum_cls:
            if member.value == self.collection:
                return member
        raise NewsQdrantError(
            f"unknown Qdrant collection {self.collection!r}; the configured "
            f"name does not match any hedge_memory_rag CollectionName"
        )


__all__ = [
    "InMemoryNewsEmbeddingSink",
    "NewsEmbeddingSink",
    "NoopNewsEmbeddingSink",
    "QdrantNewsEmbeddingSink",
]
