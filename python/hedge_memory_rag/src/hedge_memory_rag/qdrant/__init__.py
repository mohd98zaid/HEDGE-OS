"""Qdrant vector store integration for the Memory_RAG_Layer (R19.1, R19.2).

Task 31.1 implements:

* The five canonical collections — ``trades``, ``news``,
  ``journal_entries``, ``market_memory``, ``psychology_history`` — each
  provisioned idempotently against the running Qdrant server.
* CBOR-encoded embedding payloads (design § Data Models — Warm_AI_Pipeline
  payloads use JSON for ergonomics, **except embeddings which are CBOR**).
* Async writers and readers built on :mod:`qdrant_client` (the
  ``AsyncQdrantClient``).
* kNN query helpers consumed by the Warm_AI_Pipeline retrieval pipeline
  (task 34.x).

Public surface::

    from hedge_memory_rag.qdrant import (
        CollectionName,
        CollectionSpec,
        MemoryRagQdrant,
        QdrantSettings,
        VectorRecord,
        encode_embedding_cbor,
        decode_embedding_cbor,
    )

    settings = QdrantSettings.load()
    async with MemoryRagQdrant(settings=settings) as store:
        await store.ensure_collections()
        await store.upsert(
            CollectionName.TRADES,
            VectorRecord(point_id="t-1", vector=[0.1, 0.2, ...], payload={...}),
        )
        hits = await store.knn_search(
            CollectionName.TRADES, query_vector=[0.1, 0.2, ...], k=5
        )
"""

from __future__ import annotations

from .codec import (
    EMBEDDING_PAYLOAD_KEY,
    EmbeddingDecodeError,
    EmbeddingEncodeError,
    decode_embedding_cbor,
    encode_embedding_cbor,
)
from .collections import (
    DEFAULT_VECTOR_DIM,
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
from .records import KnnHit, VectorRecord
from .store import MemoryRagQdrant

__all__ = [
    # Codec
    "EMBEDDING_PAYLOAD_KEY",
    "EmbeddingDecodeError",
    "EmbeddingEncodeError",
    "decode_embedding_cbor",
    "encode_embedding_cbor",
    # Collections
    "DEFAULT_VECTOR_DIM",
    "CollectionName",
    "CollectionSpec",
    "DistanceMetric",
    "default_collection_specs",
    # Config
    "QdrantSettings",
    # Errors
    "CollectionDimensionMismatchError",
    "QdrantClientError",
    "QdrantConfigurationError",
    "QdrantConnectionError",
    # Records
    "KnnHit",
    "VectorRecord",
    # Store
    "MemoryRagQdrant",
]
