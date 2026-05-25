"""Canonical Qdrant collection specs for the Memory_RAG_Layer (R19.1, R19.2).

Five collections are provisioned, one per persisted vector domain:

* ``trades``              — closed-trade embeddings (entry / exit context).
* ``news``                — headline embeddings produced by News_Intelligence (R12).
* ``journal_entries``     — AI_Trade_Journal narratives embedded for retrieval (R18).
* ``market_memory``       — Previous_Day_Memory + intraday market-state vectors (R15).
* ``psychology_history``  — Trader_Psychology snapshots embedded for similarity search (R16).

Each collection's :class:`CollectionSpec` carries the vector dimensionality,
the distance metric, and the on-disk flag — keeping the spec next to the
collection name lets ``ensure_collections`` make idempotent
``recreate_collection``-free decisions.

Vector dimensions deliberately default to ``DEFAULT_VECTOR_DIM`` (768) to
match the DistilBERT mean-pooled hidden state produced by
:class:`hedge_warm_ai.onnx_runtime.nlp.DistilBERTEmbedding`. Operators can
override the dimension per collection through :class:`QdrantSettings.vector_dims`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final


#: DistilBERT mean-pooled hidden-state dimensionality. Documented in
#: :mod:`hedge_warm_ai.onnx_runtime.nlp`. Other Warm_AI_Pipeline embedders
#: (FinBERT classification head, Tiny LSTM trade encoder) emit different
#: shapes; per-collection overrides live in :class:`QdrantSettings`.
DEFAULT_VECTOR_DIM: Final[int] = 768


class DistanceMetric(str, Enum):
    """Subset of qdrant distance metrics relevant to the design.

    Cosine is the canonical choice for embedding similarity (the
    DistilBERT and FinBERT outputs are length-normalised). ``Euclid``
    and ``Dot`` are kept as escape hatches; other Qdrant metrics
    (``Manhattan``) are intentionally not exposed because they are not
    used by any Warm_AI_Pipeline embedder.
    """

    COSINE = "Cosine"
    EUCLID = "Euclid"
    DOT = "Dot"


class CollectionName(str, Enum):
    """Canonical names of every Memory_RAG_Layer Qdrant collection (R19.2)."""

    TRADES = "trades"
    NEWS = "news"
    JOURNAL_ENTRIES = "journal_entries"
    MARKET_MEMORY = "market_memory"
    PSYCHOLOGY_HISTORY = "psychology_history"


@dataclass(frozen=True, slots=True)
class CollectionSpec:
    """Provisioning spec for one Qdrant collection.

    Attributes:
        name: Canonical :class:`CollectionName` value. Stored as a plain
            ``str`` so the dataclass is hashable across process boundaries.
        vector_dim: Length of every embedding stored in the collection.
            Must match the embedder that produces the vectors; a mismatch
            on existing data raises :class:`CollectionDimensionMismatchError`.
        distance: Similarity metric used by the kNN index. Cosine for all
            normalised embeddings.
        on_disk: When ``True``, vectors are stored on disk to lower
            memory pressure; recommended for ``trades`` and
            ``journal_entries`` which grow unbounded with session count.

    Vectors are stored in a default (unnamed) vector space — Qdrant
    supports named vectors but the design does not require them.
    """

    name: str
    vector_dim: int
    distance: DistanceMetric = DistanceMetric.COSINE
    on_disk: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("collection name must be non-empty")
        if self.vector_dim <= 0:
            raise ValueError(
                f"collection {self.name!r}: vector_dim must be > 0, got {self.vector_dim!r}"
            )


def default_collection_specs(
    *,
    vector_dim: int = DEFAULT_VECTOR_DIM,
    distance: DistanceMetric = DistanceMetric.COSINE,
    overrides: dict[CollectionName, int] | None = None,
) -> dict[CollectionName, CollectionSpec]:
    """Return the canonical collection specs.

    Args:
        vector_dim: Default dimensionality applied to every collection
            unless ``overrides`` carries a per-collection value.
        distance: Default distance metric. Cosine for the normalised
            DistilBERT / FinBERT embedders.
        overrides: Optional per-collection dimensionality override —
            used when a service stores a non-default embedding (e.g.
            the Tiny LSTM trade encoder emits 256-dim vectors and would
            override ``CollectionName.TRADES`` accordingly).

    Returns:
        Mapping of every :class:`CollectionName` to its
        :class:`CollectionSpec`. Iteration order matches enum declaration
        order so log output is stable.
    """
    overrides = overrides or {}
    # ``trades`` and ``journal_entries`` grow unboundedly across sessions;
    # store them on disk to keep the working set bounded. ``news``,
    # ``market_memory``, and ``psychology_history`` are session-bounded
    # in volume and stay in memory for the lowest kNN latency.
    on_disk_collections: frozenset[CollectionName] = frozenset(
        {CollectionName.TRADES, CollectionName.JOURNAL_ENTRIES}
    )

    specs: dict[CollectionName, CollectionSpec] = {}
    for member in CollectionName:
        dim = overrides.get(member, vector_dim)
        specs[member] = CollectionSpec(
            name=member.value,
            vector_dim=dim,
            distance=distance,
            on_disk=member in on_disk_collections,
        )
    return specs


__all__ = [
    "DEFAULT_VECTOR_DIM",
    "CollectionName",
    "CollectionSpec",
    "DistanceMetric",
    "default_collection_specs",
]
