"""News_Intelligence_Engine subpackage (R12, task 21.1 of PROJECT HEDGE).

This sub-package implements the design's *News_Intelligence_Engine*
(design § Components — News_Intelligence_Engine) and the requirements
12.1–12.6 from ``requirements.md``:

* Ingest content from Reuters, Moneycontrol, NSE filings, RBI
  announcements, Twitter/X, Telegram, Economic Times, and configured
  broker feeds (R12.1) via :class:`SourceAdapter` subclasses.
* Run a fast path on FinBERT/DistilBERT via ONNX (R12.2): entity
  extract → sentiment → impact score → symbol map →
  :class:`hedge_warm_ai.schemas.NewsImpact` payload.
* Dispatch slow-path reasoning to Ollama **asynchronously** so the
  fast path is never blocked (R12.3, design § Warm_AI_Pipeline
  Architecture).
* Emit ``ai.news.impact.<sym>`` to NATS tagged with ``symbol``,
  ``sentiment``, and ``impact_magnitude`` (R12.4) via a
  :class:`NewsPublisher`.
* Persist the headline embedding into the Qdrant ``news`` collection
  (R19.2) for downstream Memory_RAG_Layer retrieval.

The engine is **strictly off the Hot_Path**. It writes only to
``ai.news.*`` subjects and never attempts to publish on ``risk.*`` or
``exec.*``.

Pipeline (design § Components — News_Intelligence_Engine):

::

    Source_Adapter (per source)
        └── Headline
              └── Dedup (content-hash bounded LRU)
                    └── Fast_Path
                         { entity_extract,
                           finbert_sentiment,
                           impact_score,
                           symbol_map }
                              └── NewsImpact_v1 ─► ai.news.impact.<sym>
                              └── DistilBERT embed ─► Qdrant ``news``
                              └── asyncio.create_task(Slow_Path ollama_reasoning)

The slow path is fire-and-forget at the engine layer — the fast path
emits its :class:`NewsImpact` event without awaiting the Ollama
streaming response (R12.3). This invariant is encoded in
:meth:`NewsIntelligenceEngine.ingest` and exercised by the property
test in task 21.2.
"""

from __future__ import annotations

from .config import (
    DEFAULT_DEDUP_WINDOW,
    DEFAULT_FAST_PATH_BUDGET_MS,
    DEFAULT_NEWS_QDRANT_COLLECTION,
    DEFAULT_SLOW_PATH_ROLE,
    NewsConfig,
)
from .dedup import Dedup, content_hash
from .engine import NewsIntelligenceEngine, NewsIngestionResult
from .errors import (
    NewsConfigError,
    NewsEngineError,
    NewsIngestionError,
    NewsPublishError,
    NewsQdrantError,
)
from .fast_path import (
    EntityExtraction,
    FastPath,
    FastPathResult,
    SymbolMap,
    impact_score,
)
from .headline import Headline, HeadlineSource
from .publisher import (
    AI_NEWS_IMPACT_PREFIX,
    InMemoryNewsPublisher,
    NatsNewsPublisher,
    NewsPublisher,
    NoopNewsPublisher,
    news_impact_subject,
)
from .qdrant_sink import (
    InMemoryNewsEmbeddingSink,
    NewsEmbeddingSink,
    NoopNewsEmbeddingSink,
    QdrantNewsEmbeddingSink,
)
from .slow_path import (
    InMemorySlowPathSink,
    NoopSlowPathSink,
    OllamaSlowPath,
    SlowPath,
    SlowPathResult,
    SlowPathSink,
)
from .sources import (
    BrokerFeedAdapter,
    EconomicTimesAdapter,
    MoneycontrolAdapter,
    NseFilingsAdapter,
    RbiAnnouncementsAdapter,
    ReutersAdapter,
    SourceAdapter,
    TelegramAdapter,
    TwitterAdapter,
    default_source_adapters,
)


__all__ = [
    # config
    "DEFAULT_DEDUP_WINDOW",
    "DEFAULT_FAST_PATH_BUDGET_MS",
    "DEFAULT_NEWS_QDRANT_COLLECTION",
    "DEFAULT_SLOW_PATH_ROLE",
    "NewsConfig",
    # dedup
    "Dedup",
    "content_hash",
    # engine
    "NewsIngestionResult",
    "NewsIntelligenceEngine",
    # errors
    "NewsConfigError",
    "NewsEngineError",
    "NewsIngestionError",
    "NewsPublishError",
    "NewsQdrantError",
    # fast path
    "EntityExtraction",
    "FastPath",
    "FastPathResult",
    "SymbolMap",
    "impact_score",
    # headline
    "Headline",
    "HeadlineSource",
    # publisher
    "AI_NEWS_IMPACT_PREFIX",
    "InMemoryNewsPublisher",
    "NatsNewsPublisher",
    "NewsPublisher",
    "NoopNewsPublisher",
    "news_impact_subject",
    # qdrant sink
    "InMemoryNewsEmbeddingSink",
    "NewsEmbeddingSink",
    "NoopNewsEmbeddingSink",
    "QdrantNewsEmbeddingSink",
    # slow path
    "InMemorySlowPathSink",
    "NoopSlowPathSink",
    "OllamaSlowPath",
    "SlowPath",
    "SlowPathResult",
    "SlowPathSink",
    # sources
    "BrokerFeedAdapter",
    "EconomicTimesAdapter",
    "MoneycontrolAdapter",
    "NseFilingsAdapter",
    "RbiAnnouncementsAdapter",
    "ReutersAdapter",
    "SourceAdapter",
    "TelegramAdapter",
    "TwitterAdapter",
    "default_source_adapters",
]
