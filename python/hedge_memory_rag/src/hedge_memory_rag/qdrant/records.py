"""Typed record + result wrappers for :class:`MemoryRagQdrant`.

These dataclasses keep the qdrant-client surface (``PointStruct``,
``ScoredPoint``, etc.) at the boundary of the package. Callers in the
Warm_AI_Pipeline see only :class:`VectorRecord` and :class:`KnnHit`,
which makes mocking and round-trip property tests straightforward.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import numpy as np


#: Type alias for a Qdrant point identifier. Qdrant accepts ``int`` or
#: ``str`` (UUID) — both are propagated unchanged.
PointId = int | str


@dataclass(frozen=True, slots=True)
class VectorRecord:
    """One vector + payload bound for a Qdrant collection.

    Attributes:
        point_id: Qdrant point id. ``int`` and ``str`` are both
            accepted; the latter must be a valid UUID per Qdrant's
            id rules. The Memory_RAG_Layer prefers stable string ids
            derived from ``correlation_id`` or ``trade_id`` so the same
            entity yields the same point id across replays.
        vector: The dense embedding. Accepts ``Sequence[float]`` or
            ``numpy.ndarray``; coerced to ``list[float]`` before the
            client call.
        payload: JSON-able metadata. Embedding bytes are *not* placed
            here automatically — callers wrap with
            :func:`hedge_memory_rag.qdrant.codec.attach_embedding_cbor`
            if they want the CBOR copy persisted alongside the indexed
            vector.

    The dataclass is frozen + slotted so it is hashable and cheap to
    pass around in async tasks.
    """

    point_id: PointId
    vector: Sequence[float] | np.ndarray
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KnnHit:
    """One result returned from :meth:`MemoryRagQdrant.knn_search`.

    Attributes:
        point_id: Identifier of the matched point.
        score: Similarity score returned by Qdrant. For Cosine it sits
            in ``[-1.0, 1.0]`` (higher = more similar); for Euclid it is
            negated distance (higher = closer); for Dot it is the raw
            inner product.
        payload: Stored payload for the hit, exactly as persisted
            (including the optional CBOR embedding under
            :data:`hedge_memory_rag.qdrant.codec.EMBEDDING_PAYLOAD_KEY`).
        vector: Indexed vector — populated only when
            ``with_vector=True`` is requested in :meth:`knn_search`.
    """

    point_id: PointId
    score: float
    payload: Mapping[str, Any]
    vector: list[float] | None = None


__all__ = [
    "KnnHit",
    "PointId",
    "VectorRecord",
]
