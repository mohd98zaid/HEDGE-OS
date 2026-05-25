"""News_Intelligence_Engine — task 21.1 (R12.1–R12.6).

The engine ties together the seven collaborators introduced in this
subpackage:

* :class:`~.sources.SourceAdapter` (one per source, R12.1) — the
  ingest lane.
* :class:`~.dedup.Dedup` — bounded LRU on :func:`content_hash`.
* :class:`~.fast_path.FastPath` — entity_extract → finbert_sentiment
  → impact_score → symbol_map (R12.2).
* :class:`~.publisher.NewsPublisher` — publishes
  ``ai.news.impact.<sym>`` (R12.4).
* :class:`~.qdrant_sink.NewsEmbeddingSink` — persists DistilBERT
  embeddings into the ``news`` Qdrant collection (R19.2).
* :class:`~.slow_path.SlowPath` — Ollama reasoning dispatched via
  :func:`asyncio.create_task` so the fast path is never blocked
  (R12.3).
* :class:`~.config.NewsConfig` — supplies the dedup window, fast-
  path budget, slow-path role, and Qdrant collection.

Critical invariants
-------------------

The engine MUST honour two design constraints, encoded in this
module's :meth:`NewsIntelligenceEngine.ingest`:

1. **Slow-path non-blocking (R12.3, Property 2).** When a headline
   produces a fast-path result, the slow-path coroutine is launched
   via :func:`asyncio.create_task` and the fast-path emits its
   :class:`hedge_warm_ai.schemas.NewsImpact` event without awaiting
   the slow path. The engine retains a strong reference to every
   spawned task in :attr:`_pending_tasks` so the asyncio runtime
   does not garbage-collect them mid-flight; tasks remove themselves
   from that set on completion.
2. **Bounded outputs (R12.4, Property 4).** The
   :class:`NewsImpact` payload's ``sentiment ∈ [-1, 1]`` and
   ``impact_magnitude ∈ [0, 1]`` are guaranteed structurally:

   * The fast path clamps sentiment in
     :class:`hedge_warm_ai.onnx_runtime.SentimentResult` and
     impact_magnitude in
     :func:`hedge_warm_ai.news.fast_path.impact_score`.
   * The :class:`NewsImpact` Pydantic model re-validates both
     fields at construction; a rogue value raises before any NATS
     emission.

The combination of the two invariants is what task 21.2's property
test will fuzz: every emitted event has bounded scores, and the
slow-path dispatch never delays the fast-path emission timestamp.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Final, Mapping, Optional

import structlog

from ..onnx_runtime import (
    DistilBERTEmbedding,
    correlation_id_from_bytes,
    new_correlation_id,
)
from ..schemas import NewsImpact
from .config import NewsConfig
from .dedup import Dedup
from .errors import NewsPublishError, NewsQdrantError
from .fast_path import FastPath, FastPathResult
from .headline import Headline, HeadlineSource
from .publisher import NewsPublisher, NoopNewsPublisher
from .qdrant_sink import NewsEmbeddingSink, NoopNewsEmbeddingSink
from .slow_path import (
    NoopSlowPathSink,
    SlowPath,
    SlowPathSink,
)
from .sources import SourceAdapter

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Result type ---------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NewsIngestionResult:
    """Outcome of one :meth:`NewsIntelligenceEngine.ingest` call.

    Attributes:
        deduped: ``True`` when :class:`Dedup` rejected the headline.
            All other fields are ``None`` in that case.
        impact: The published :class:`NewsImpact` event, or ``None``
            when the fast path mapped to no tracked symbol (the
            engine still runs the FinBERT call so the latency
            tracer fires, but emits nothing on the bus).
        fast: The :class:`FastPathResult` for white-box assertions.
            ``None`` only when ``deduped`` is ``True``.
        slow_path_scheduled: ``True`` iff the engine launched a
            background slow-path task. Always ``False`` when
            :attr:`NewsConfig.slow_path_enabled` is ``False`` or the
            fast path mapped to no tracked symbol.
        embedding_scheduled: ``True`` iff the engine launched a
            background Qdrant upsert task.
    """

    deduped: bool
    impact: Optional[NewsImpact]
    fast: Optional[FastPathResult]
    slow_path_scheduled: bool
    embedding_scheduled: bool


# ---------------------------------------------------------------------------
# Engine --------------------------------------------------------------------
# ---------------------------------------------------------------------------


class NewsIntelligenceEngine:
    """Orchestrator for the News_Intelligence_Engine subsystem (R12).

    Lifecycle::

        engine = NewsIntelligenceEngine(
            config=NewsConfig.from_yaml_path(...),
            sources=default_source_adapters(),
            fast_path=FastPath(finbert=FinBERTSentiment(...)),
            embedder=DistilBERTEmbedding(...),
            slow_path=OllamaSlowPath(client=ollama_client, role="deepseek"),
            publisher=NatsNewsPublisher(async_publish=nc.publish),
            embedding_sink=QdrantNewsEmbeddingSink(store=qdrant_store),
        )

        async for headline in engine.sources[HeadlineSource.REUTERS].stream():
            await engine.ingest(headline)

    Concurrency:

    * The engine is async-first; one event-loop task drives ingestion.
    * Background slow-path and embedding tasks are tracked in
      :attr:`_pending_tasks` so the asyncio runtime keeps them alive
      until completion (the runtime garbage-collects orphaned tasks).
      :meth:`drain` is a test/replay helper that awaits every
      currently-pending task.
    """

    def __init__(
        self,
        *,
        config: NewsConfig,
        sources: Mapping[HeadlineSource, SourceAdapter],
        fast_path: FastPath,
        embedder: Optional[DistilBERTEmbedding] = None,
        slow_path: Optional[SlowPath] = None,
        publisher: Optional[NewsPublisher] = None,
        embedding_sink: Optional[NewsEmbeddingSink] = None,
        slow_path_sink: Optional[SlowPathSink] = None,
        clock_ns: Optional["callable"] = None,
    ) -> None:
        """Construct the engine.

        Args:
            config: Resolved :class:`NewsConfig`.
            sources: Mapping of :class:`HeadlineSource` to
                :class:`SourceAdapter`. Every source the deployment
                actually consumes must appear here; the engine does
                not validate completeness against
                :class:`HeadlineSource` so a deployment that does not
                yet have credentials for one source can omit it.
            fast_path: Already-wired :class:`FastPath` (FinBERT
                session loaded). The engine wires its
                ``tracked_symbols_provider`` to read from
                :attr:`config.symbols` so a config reload propagates.
            embedder: Optional :class:`DistilBERTEmbedding`. When
                supplied, the engine schedules a background Qdrant
                upsert per ingested headline that mapped to a
                tracked symbol. ``None`` disables the embedding sink
                entirely (useful for fast-path-only smoke tests).
            slow_path: Optional :class:`SlowPath` implementation. When
                supplied **and** :attr:`NewsConfig.slow_path_enabled`
                is ``True``, every fast-path result that mapped to a
                tracked symbol triggers a background slow-path
                dispatch.
            publisher: NATS publisher. Defaults to
                :class:`NoopNewsPublisher`.
            embedding_sink: Qdrant sink. Defaults to
                :class:`NoopNewsEmbeddingSink`.
            slow_path_sink: Sink for :class:`SlowPathResult` values.
                Defaults to :class:`NoopSlowPathSink`.
            clock_ns: Wall-clock ns callable. Defaults to
                :mod:`time.time_ns`. Override in tests for
                determinism.
        """
        self._config = config
        # Force the fast path to read live config.symbols so a
        # service-layer config reload propagates without the engine
        # having to swap the FastPath instance.
        fast_path.tracked_symbols_provider = lambda: tuple(self._config.symbols)
        self._fast_path = fast_path
        self._sources: Mapping[HeadlineSource, SourceAdapter] = dict(sources)
        self._embedder = embedder
        self._slow_path = slow_path
        self._publisher: NewsPublisher = publisher or NoopNewsPublisher()
        self._embedding_sink: NewsEmbeddingSink = (
            embedding_sink or NoopNewsEmbeddingSink()
        )
        self._slow_path_sink: SlowPathSink = (
            slow_path_sink or NoopSlowPathSink()
        )
        self._clock_ns = clock_ns or time.time_ns

        self._dedup = Dedup(window=config.dedup_window)
        self._pending_tasks: set[asyncio.Task[Any]] = set()
        self._ingestion_count: int = 0
        self._dedup_drops: int = 0
        self._unmapped_count: int = 0

    # ------------------------------------------------------------------
    # Introspection ----------------------------------------------------
    # ------------------------------------------------------------------

    @property
    def config(self) -> NewsConfig:
        return self._config

    @property
    def sources(self) -> Mapping[HeadlineSource, SourceAdapter]:
        """Read-only view of the registered source adapters."""
        return dict(self._sources)

    @property
    def dedup(self) -> Dedup:
        """The internal dedup filter (read-only access for tests)."""
        return self._dedup

    @property
    def ingestion_count(self) -> int:
        """Total :meth:`ingest` calls completed (test helper)."""
        return self._ingestion_count

    @property
    def dedup_drops(self) -> int:
        """Total headlines dropped by :class:`Dedup` (test helper)."""
        return self._dedup_drops

    @property
    def unmapped_count(self) -> int:
        """Total headlines that did not map to a tracked symbol."""
        return self._unmapped_count

    @property
    def pending_task_count(self) -> int:
        """Number of currently in-flight background tasks."""
        return len(self._pending_tasks)

    # ------------------------------------------------------------------
    # Public API -------------------------------------------------------
    # ------------------------------------------------------------------

    async def ingest(self, headline: Headline) -> NewsIngestionResult:
        """Process one inbound :class:`Headline`.

        Steps:

        1. Dedup the headline by content hash. Duplicates short-
           circuit with ``deduped=True``.
        2. Run the fast path (FinBERT scoring + entity extract +
           impact score + symbol map). The fast path always runs so
           ONNX latency is observed even for headlines that do not
           map to a tracked symbol.
        3. If the fast path mapped to a tracked symbol:
           a. Build the :class:`NewsImpact` payload (bounded scores).
           b. Publish ``ai.news.impact.<sym>`` (R12.4).
           c. Schedule a background DistilBERT-embed + Qdrant-upsert
              task (R19.2) when an embedder is configured.
           d. Schedule a background Ollama slow-path task (R12.3)
              when :attr:`NewsConfig.slow_path_enabled` and a slow
              path is configured.

        The engine never awaits the slow-path or embedding tasks
        inline — the fast-path emission is non-blocking by design
        (R12.3, Property 2).
        """
        if not self._dedup.observe(headline):
            self._dedup_drops += 1
            self._ingestion_count += 1
            return NewsIngestionResult(
                deduped=True,
                impact=None,
                fast=None,
                slow_path_scheduled=False,
                embedding_scheduled=False,
            )

        fast = await self._fast_path.run(headline)

        primary = fast.mapping.primary
        if primary is None:
            self._unmapped_count += 1
            self._ingestion_count += 1
            return NewsIngestionResult(
                deduped=False,
                impact=None,
                fast=fast,
                slow_path_scheduled=False,
                embedding_scheduled=False,
            )

        impact = self._build_impact(headline, fast, primary)

        # Publish ai.news.impact.<sym> (R12.4). Failures are logged
        # and surfaced so the supervisor can reconnect; we still
        # schedule the slow-path / embedding work so the rest of the
        # pipeline does not stall behind a flaky NATS connection.
        try:
            await self._publisher.publish_news_impact(impact)
        except NewsPublishError as exc:
            _LOG.warning(
                "news_impact_publish_error",
                symbol=primary,
                headline_id=headline.headline_id,
                error=str(exc),
            )

        embedding_scheduled = self._maybe_schedule_embedding(headline, fast)
        slow_path_scheduled = self._maybe_schedule_slow_path(fast)

        self._ingestion_count += 1
        return NewsIngestionResult(
            deduped=False,
            impact=impact,
            fast=fast,
            slow_path_scheduled=slow_path_scheduled,
            embedding_scheduled=embedding_scheduled,
        )

    async def drain(self) -> None:
        """Await every currently-pending background task.

        Used by tests and the service-binary shutdown path. Cancels
        nothing — tasks complete on their own — but ensures every
        scheduled embedding and slow-path call has resolved before
        returning.
        """
        if not self._pending_tasks:
            return
        await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    # ------------------------------------------------------------------
    # Internals --------------------------------------------------------
    # ------------------------------------------------------------------

    def _build_impact(
        self,
        headline: Headline,
        fast: FastPathResult,
        primary: str,
    ) -> NewsImpact:
        """Compose the :class:`NewsImpact` payload from a fast-path result.

        The bounds on ``sentiment`` and ``impact_magnitude`` are
        already established by the fast path, but the
        :class:`NewsImpact` Pydantic model re-validates them at
        construction so a refactor that loosens the upstream bounds
        cannot accidentally publish an out-of-range payload.
        """
        cid_bytes = (
            correlation_id_from_bytes(headline.correlation_id)
            if headline.correlation_id
            else new_correlation_id()
        )
        ts_ns = headline.ts_ns if headline.ts_ns > 0 else int(self._clock_ns())
        return NewsImpact(
            correlation_id=cid_bytes.hex(),
            symbol=primary,
            headline_id=headline.headline_id,
            sentiment=float(fast.sentiment.sentiment),
            impact_magnitude=float(fast.impact_magnitude),
            fast_path=True,
            slow_path_pending=bool(self._slow_path_will_dispatch()),
            ts_ns=int(ts_ns),
        )

    def _slow_path_will_dispatch(self) -> bool:
        """True iff the slow path is configured and enabled."""
        return self._slow_path is not None and self._config.slow_path_enabled

    def _maybe_schedule_embedding(
        self,
        headline: Headline,
        fast: FastPathResult,
    ) -> bool:
        """Schedule the DistilBERT embed + Qdrant upsert as a bg task."""
        if self._embedder is None:
            return False
        task = asyncio.create_task(
            self._embed_and_persist(headline, fast),
            name=f"news.embed.{headline.headline_id}",
        )
        self._track(task)
        return True

    def _maybe_schedule_slow_path(self, fast: FastPathResult) -> bool:
        """Schedule the Ollama reasoning call as a bg task (R12.3)."""
        if not self._slow_path_will_dispatch():
            return False
        assert self._slow_path is not None  # narrowed by ``_slow_path_will_dispatch``
        task = asyncio.create_task(
            self._run_slow_path(fast),
            name=f"news.slow.{fast.headline.headline_id}",
        )
        self._track(task)
        return True

    def _track(self, task: asyncio.Task[Any]) -> None:
        """Retain a strong reference until the task completes."""
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _embed_and_persist(
        self,
        headline: Headline,
        fast: FastPathResult,
    ) -> None:
        """Background coroutine: embed + upsert to Qdrant ``news``."""
        if self._embedder is None:  # pragma: no cover - guarded by caller
            return
        try:
            embedding = await self._embedder.embed(headline.text)
        except Exception as exc:
            _LOG.warning(
                "news_embedding_failed",
                headline_id=headline.headline_id,
                error=str(exc),
            )
            return
        try:
            await self._embedding_sink.upsert_embedding(
                headline=headline,
                fast=fast,
                embedding=embedding,
            )
        except NewsQdrantError as exc:
            _LOG.warning(
                "news_qdrant_upsert_failed",
                headline_id=headline.headline_id,
                error=str(exc),
            )
        except Exception as exc:  # pragma: no cover - defensive
            _LOG.warning(
                "news_embedding_sink_failed",
                headline_id=headline.headline_id,
                error=str(exc),
            )

    async def _run_slow_path(self, fast: FastPathResult) -> None:
        """Background coroutine: dispatch Ollama reasoning + sink the result."""
        assert self._slow_path is not None  # narrowed by ``_maybe_schedule_slow_path``
        try:
            result = await self._slow_path.run(fast)
        except Exception as exc:  # pragma: no cover - safety net
            _LOG.warning(
                "news_slow_path_unhandled",
                headline_id=fast.headline.headline_id,
                error=str(exc),
            )
            return
        try:
            await self._slow_path_sink.submit(result)
        except Exception as exc:  # pragma: no cover - defensive
            _LOG.warning(
                "news_slow_path_sink_failed",
                headline_id=fast.headline.headline_id,
                error=str(exc),
            )


__all__ = [
    "NewsIngestionResult",
    "NewsIntelligenceEngine",
]
