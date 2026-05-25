"""Slow path: asynchronous Ollama reasoning over a fast-path result.

The design's pipeline (Components § News_Intelligence_Engine):

::

    Fast_Path → NewsImpact_v1
              → optional Slow_Path { ollama_reasoning } → NewsImpactExtended_v1

The slow path performs richer reasoning over the headline. It uses
:meth:`hedge_warm_ai.ollama_client.OllamaClient.stream_generate`
(R10.7) so a long-running reasoning call streams tokens incrementally
and is cancellable. The role the engine routes to is configurable
(:attr:`hedge_warm_ai.news.config.NewsConfig.slow_path_role`) and
defaults to ``"deepseek"`` (DeepSeek-R1, the design's reasoning model).

**The fast path never awaits the slow path.** The engine wraps each
slow-path call in :func:`asyncio.create_task` (see
:meth:`NewsIntelligenceEngine.ingest`) so the fast path emits its
:class:`NewsImpact` event immediately and the slow path completes in
the background — satisfying R12.3 and Property 2 (Hot_Path purity)
verbatim.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import RLock
from typing import Final, Optional, Protocol

import structlog

from ..ollama_client import OllamaClient
from ..ollama_client.endpoint import OllamaRoleKey
from ..ollama_client.errors import OllamaClientError
from .fast_path import FastPathResult

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Result types --------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SlowPathResult:
    """Outcome of one slow-path Ollama dispatch.

    Attributes:
        headline_id: Source headline id (echoed for trace).
        symbol: Primary symbol the slow path was reasoning about.
            ``""`` when the headline did not map to a tracked symbol.
        reasoning: Full text of the streamed Ollama response, joined
            in arrival order. Empty on failure.
        tokens: Number of tokens emitted by the daemon (when present
            in the trailing ``done`` chunk's metrics). ``0`` when the
            metric was missing.
        role: Ollama role the dispatch landed on after fallback (may
            differ from the engine's configured role if a fallback
            hop occurred).
        model: GGUF tag of the responding daemon.
        ts_ns: Wall-clock ns at which the slow path completed.
        error: ``None`` on success; the typed Ollama error otherwise.
    """

    headline_id: str
    symbol: str
    reasoning: str
    tokens: int
    role: str
    model: str
    ts_ns: int
    error: Optional[OllamaClientError] = None


# ---------------------------------------------------------------------------
# Sink protocol -------------------------------------------------------------
# ---------------------------------------------------------------------------


class SlowPathSink(Protocol):
    """Sink that receives :class:`SlowPathResult` values.

    The slow path is fire-and-forget at the engine layer — its result
    is *not* returned to the inbound headline producer. Instead, the
    engine routes the result into a sink so:

    * Tests assert on the captured results without spinning up the
      full ``NewsImpactExtended_v1`` consumer.
    * Production wires a future ``NatsSlowPathSink`` (task 21.x in a
      follow-up) that publishes ``ai.news.impact.extended.<sym>``.
    """

    async def submit(self, result: SlowPathResult) -> None: ...


class NoopSlowPathSink:
    """Discards every result. Default while no consumer is wired."""

    async def submit(self, result: SlowPathResult) -> None:  # noqa: D401
        return


class InMemorySlowPathSink:
    """Captures every result for assertion in tests.

    Thread-safe: the underlying list is guarded by an :class:`RLock`.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._results: list[SlowPathResult] = []

    async def submit(self, result: SlowPathResult) -> None:
        with self._lock:
            self._results.append(result)

    @property
    def results(self) -> list[SlowPathResult]:
        with self._lock:
            return list(self._results)

    def reset(self) -> None:
        with self._lock:
            self._results.clear()


# ---------------------------------------------------------------------------
# Slow path -----------------------------------------------------------------
# ---------------------------------------------------------------------------


class SlowPath(Protocol):
    """Slow-path callable invoked by the engine in a background task."""

    async def run(self, fast: FastPathResult) -> SlowPathResult: ...


def _build_prompt(fast: FastPathResult) -> str:
    """Compose the Ollama prompt for one fast-path result.

    The prompt is deliberately compact so the slow path does not
    inflate the streaming budget. It carries:

    * the source provenance (editorial trust signal),
    * the symbol the fast path mapped (focuses the reasoning), and
    * the bounded fast-path scores (anchors the model's response).
    """
    headline = fast.headline
    primary = fast.mapping.primary or "<unmapped>"
    keywords = ", ".join(fast.entities.keywords_hit) if fast.entities.keywords_hit else "none"
    return (
        "You are the Warm_AI_Pipeline news reasoner. "
        "Given the fast-path summary below, explain in 3 short sentences "
        "what this headline likely means for the Indian equity market and "
        "the named symbol, and call out any second-order risks.\n\n"
        f"source: {headline.source.value}\n"
        f"symbol: {primary}\n"
        f"sentiment: {fast.sentiment.sentiment:+.3f} "
        f"({fast.sentiment.label}, p={fast.sentiment.score:.3f})\n"
        f"impact_magnitude: {fast.impact_magnitude:.3f}\n"
        f"keywords: {keywords}\n"
        f"text: {headline.text}\n"
    )


@dataclass
class OllamaSlowPath:
    """Concrete :class:`SlowPath` backed by :class:`OllamaClient` (R12.3).

    Construction:

    * ``client`` — already-started :class:`OllamaClient`.
    * ``role`` — Ollama role key the slow path dispatches to. Must
      be present in the client's registry; the engine validates this
      via :class:`hedge_warm_ai.news.config.NewsConfig.with_role_check`
      at startup.
    * ``request_timeout_s`` — optional per-call override; ``None``
      means use the role's registered default.
    * ``max_tokens`` — cap on the number of streamed tokens
      consumed. ``0`` means no cap.
    * ``clock_ns`` — wall-clock ns clock for stamping the result.
      Override in tests.

    The :meth:`run` coroutine **never raises**. Failures are
    captured into :attr:`SlowPathResult.error` so the engine's
    background task wrapper can route them to the supervisor without
    crashing the asyncio task.
    """

    client: OllamaClient
    role: OllamaRoleKey = "deepseek"
    request_timeout_s: Optional[float] = None
    max_tokens: int = 0
    clock_ns: "callable" = field(default=time.time_ns)

    async def run(self, fast: FastPathResult) -> SlowPathResult:
        symbol = fast.mapping.primary or ""
        prompt = _build_prompt(fast)
        reasoning_chunks: list[str] = []
        tokens_consumed = 0
        last_role: str = self.role
        last_model: str = ""
        error: Optional[OllamaClientError] = None
        try:
            async for chunk in self.client.stream_generate(
                self.role,
                prompt=prompt,
                request_timeout_s=self.request_timeout_s,
            ):
                last_role = chunk.role
                last_model = chunk.model
                if chunk.text:
                    reasoning_chunks.append(chunk.text)
                    tokens_consumed += 1
                if self.max_tokens and tokens_consumed >= self.max_tokens:
                    # Caller-imposed token budget — stop draining.
                    # The async generator will be closed implicitly
                    # when the loop exits.
                    break
                if chunk.done and chunk.metrics:
                    metric_count = chunk.metrics.get("eval_count")
                    if isinstance(metric_count, int) and metric_count > tokens_consumed:
                        tokens_consumed = metric_count
        except OllamaClientError as exc:
            error = exc
            _LOG.warning(
                "news_slow_path_failed",
                headline_id=fast.headline.headline_id,
                role=self.role,
                error=type(exc).__name__,
            )
        return SlowPathResult(
            headline_id=fast.headline.headline_id,
            symbol=symbol,
            reasoning="".join(reasoning_chunks),
            tokens=tokens_consumed,
            role=last_role,
            model=last_model,
            ts_ns=int(self.clock_ns()),
            error=error,
        )


__all__ = [
    "InMemorySlowPathSink",
    "NoopSlowPathSink",
    "OllamaSlowPath",
    "SlowPath",
    "SlowPathResult",
    "SlowPathSink",
]
