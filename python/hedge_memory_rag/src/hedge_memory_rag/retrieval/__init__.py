"""Memory_RAG_Layer retrieval pipeline (R19.5, R19.6, R19.7 — task 34.1).

Five-stage async pipeline composed via ``await``:

::

    trader_event_lookup
        └── memory_retrieval (Qdrant kNN ⊕ Timescale window — parallel)
            └── context_assembly (deterministic, no LLM)
                └── ollama_reasoning (OllamaClient.stream_generate)
                    └── recommendation_generation → Recommendation

Reachable from the **Warm_AI_Pipeline only** (R19.7). The pipeline never
subscribes to a Hot_Path subject — see :doc:`README.md` for the
machine-readable invariant. Configuration (collection names, hypertable
names, Ollama role, kNN ``k``, time-window length) flows in through
:class:`RetrievalSettings`; nothing is hardcoded.

Public surface::

    from hedge_memory_rag.retrieval import (
        Recommendation,
        RetrievalPipeline,
        RetrievalRequest,
        RetrievalSettings,
        TraderEvent,
    )

    pipeline = RetrievalPipeline(
        settings=RetrievalSettings.load(),
        qdrant=qdrant_store,
        timescale=timescale_reader,
        redis=redis_cache,
        ollama=ollama_client,
    )
    rec = await pipeline.run(request)
"""

from __future__ import annotations

from .config import (
    DEFAULT_K,
    DEFAULT_OLLAMA_ROLE,
    DEFAULT_RECENT_NEWS_PER_SYMBOL,
    DEFAULT_RECENT_TRADES_PER_SYMBOL,
    DEFAULT_REQUEST_TIMEOUT_S,
    DEFAULT_WINDOW_MINUTES,
    RetrievalSettings,
    load_retrieval_settings,
)
from .errors import (
    OllamaReasoningFailedError,
    RecommendationParseError,
    RetrievalConfigurationError,
    RetrievalError,
    RetrievalTimeoutError,
)
from .pipeline import RetrievalPipeline
from .recommendation import Recommendation
from .records import (
    AssembledContext,
    EventContext,
    MemoryHits,
    QdrantHitView,
    RetrievalRequest,
    StreamedReasoning,
    TraderEvent,
)

__all__ = [
    # Settings
    "DEFAULT_K",
    "DEFAULT_OLLAMA_ROLE",
    "DEFAULT_RECENT_NEWS_PER_SYMBOL",
    "DEFAULT_RECENT_TRADES_PER_SYMBOL",
    "DEFAULT_REQUEST_TIMEOUT_S",
    "DEFAULT_WINDOW_MINUTES",
    "RetrievalSettings",
    "load_retrieval_settings",
    # Errors
    "OllamaReasoningFailedError",
    "RecommendationParseError",
    "RetrievalConfigurationError",
    "RetrievalError",
    "RetrievalTimeoutError",
    # Records
    "AssembledContext",
    "EventContext",
    "MemoryHits",
    "QdrantHitView",
    "RetrievalRequest",
    "StreamedReasoning",
    "TraderEvent",
    # Recommendation + Pipeline
    "Recommendation",
    "RetrievalPipeline",
]
