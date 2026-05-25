"""Fast path: entity_extract → finbert_sentiment → impact_score → symbol_map.

This is the design's ``Fast_Path { entity_extract, finbert_sentiment,
impact_score, symbol_map }`` block (Components §
News_Intelligence_Engine). It produces one
:class:`hedge_warm_ai.schemas.NewsImpact` value per inbound
:class:`hedge_warm_ai.news.headline.Headline` and is held to the
10 ms p95 latency budget (R12.2).

Design constraints:

* Sentiment scoring uses :class:`hedge_warm_ai.onnx_runtime.FinBERTSentiment`
  (R11.2, task 20.1). The latency tracer envelope around it is the
  same one every other ONNX call uses, so an
  ``obs.budget.breach.ai_finbert`` event fires on a 10 ms breach
  without any new wiring here.
* Sentiment is the FinBERT *positive − negative* delta clamped to
  ``[-1, 1]``. The clamp is structural — :class:`SentimentResult`
  already enforces it and :class:`NewsImpact` validates it again at
  payload construction.
* Impact magnitude is bounded to ``[0, 1]`` by construction
  (:func:`impact_score`) and validated again by :class:`NewsImpact`.
* Symbol mapping prefers the adapter-supplied
  :attr:`Headline.symbols_hint` and falls back to a deterministic
  text scan against the configured tracked-symbol universe.

The fast path is **pure async** — no blocking I/O, no thread
launching beyond the one ONNX Runtime thread pool the FinBERT call
already uses. The slow path (Ollama dispatch) and the embedding sink
(Qdrant upsert) live in their own modules and are scheduled as
background tasks by the engine, never awaited inline.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Final, Iterable, Optional, Sequence

import structlog

from ..onnx_runtime import FinBERTSentiment, SentimentResult
from .headline import Headline

_LOG: Final = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Entity extraction ---------------------------------------------------------
# ---------------------------------------------------------------------------

#: Lower-case keywords that boost a headline's impact magnitude.
#: The set is conservative and additive — every match contributes a
#: bounded amount, capped by :func:`impact_score`. The list is
#: stable across replays so the property test in 21.2 can fuzz against
#: it deterministically.
_IMPACT_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        # Macro / regulatory
        "rbi",
        "sebi",
        "rate hike",
        "rate cut",
        "policy",
        "circuit",
        "halt",
        # Earnings / corporate
        "earnings",
        "guidance",
        "downgrade",
        "upgrade",
        "merger",
        "acquisition",
        "ipo",
        "buyback",
        "dividend",
        "default",
        "fraud",
        "investigation",
        "raid",
        # Macro events
        "war",
        "sanctions",
        "tariff",
        "ban",
        # Market structure
        "limit up",
        "limit down",
        "circuit breaker",
        "trading halt",
    }
)


@dataclass(frozen=True, slots=True)
class EntityExtraction:
    """Result of the entity-extract step.

    Attributes:
        keywords_hit: Tuple of impact keywords found in the headline
            text. Used by :func:`impact_score` to compute the
            magnitude.
        symbols_hinted: Tuple of symbols supplied by the source
            adapter (``Headline.symbols_hint``). Re-exposed here so
            downstream stages do not have to re-read the source
            object.
    """

    keywords_hit: tuple[str, ...] = field(default_factory=tuple)
    symbols_hinted: tuple[str, ...] = field(default_factory=tuple)


def entity_extract(headline: Headline) -> EntityExtraction:
    """Extract impact keywords and propagate the source's symbol hints.

    The extractor is intentionally a fast keyword scan — the design
    holds the fast path to 10 ms p95 (R12.2) and budget for richer
    NER lives in the slow path's Ollama call (R12.3).
    """
    text_lc = headline.text.lower()
    hits = tuple(sorted(kw for kw in _IMPACT_KEYWORDS if kw in text_lc))
    return EntityExtraction(
        keywords_hit=hits,
        symbols_hinted=tuple(headline.symbols_hint),
    )


# ---------------------------------------------------------------------------
# Impact score --------------------------------------------------------------
# ---------------------------------------------------------------------------


def impact_score(
    sentiment: SentimentResult,
    entities: EntityExtraction,
    *,
    keyword_weight: float = 0.10,
    keyword_cap: float = 0.50,
) -> float:
    """Compute the headline's bounded impact magnitude.

    The score is a deterministic combination of:

    * the absolute FinBERT sentiment magnitude (a strongly negative
      or strongly positive headline carries more weight than a
      neutral one), capped at 1.0; and
    * a bounded keyword bonus — each :data:`_IMPACT_KEYWORDS` hit
      adds ``keyword_weight`` up to a maximum of ``keyword_cap``,
      so a barrage of hits cannot saturate the magnitude on text
      alone.

    The result is clipped to ``[0.0, 1.0]`` so it is a valid value
    for :class:`NewsImpact.impact_magnitude` regardless of the
    intermediate arithmetic.

    Args:
        sentiment: Output of :class:`FinBERTSentiment.score`.
        entities: Output of :func:`entity_extract`.
        keyword_weight: Per-keyword bonus contribution. Defaults to
            ``0.10`` so two hits add 0.20.
        keyword_cap: Upper bound for the keyword bonus. Defaults to
            ``0.50`` so even a heavily decorated headline cannot
            crowd out the FinBERT signal.

    Returns:
        Impact magnitude in ``[0.0, 1.0]``.
    """
    if keyword_weight < 0.0:
        raise ValueError(f"keyword_weight must be >= 0, got {keyword_weight!r}")
    if not (0.0 <= keyword_cap <= 1.0):
        raise ValueError(
            f"keyword_cap must be in [0.0, 1.0], got {keyword_cap!r}"
        )

    base = abs(sentiment.sentiment)  # already in [0, 1]
    bonus = min(keyword_cap, keyword_weight * len(entities.keywords_hit))
    raw = base + bonus
    if raw < 0.0:
        return 0.0
    if raw > 1.0:
        return 1.0
    return float(raw)


# ---------------------------------------------------------------------------
# Symbol mapping ------------------------------------------------------------
# ---------------------------------------------------------------------------


_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"[A-Z][A-Z0-9_\-]{1,31}")


@dataclass(frozen=True, slots=True)
class SymbolMap:
    """Outcome of the symbol-map step.

    Attributes:
        symbols: Tuple of symbols this headline maps to. Empty when
            no tracked symbol matches; the engine still emits
            :class:`NewsImpact` for the fan-out logic upstream when
            the source adapter supplied an explicit hint.
        primary: The "best" single symbol used for the
            ``ai.news.impact.<sym>`` subject. ``None`` when
            :attr:`symbols` is empty.
    """

    symbols: tuple[str, ...] = field(default_factory=tuple)
    primary: Optional[str] = None


def symbol_map(
    headline: Headline,
    entities: EntityExtraction,
    *,
    tracked_symbols: Sequence[str] | Iterable[str],
) -> SymbolMap:
    """Map the headline to one or more tracked symbols.

    Resolution order:

    1. Source-supplied :attr:`Headline.symbols_hint` (intersected
       with ``tracked_symbols``). Many adapters know their headline's
       symbol natively — the NSE filings poller, for example,
       carries a ``symbol`` field on every announcement.
    2. Word-level scan of the headline text against
       ``tracked_symbols``. Symbols that appear as standalone
       all-caps tokens are matched. The match is case-sensitive on
       the symbol side (NSE tickers are uppercase) and applies to
       the upper-case rendering of the headline.

    The first symbol of the resolved list is exposed as
    :attr:`SymbolMap.primary`. The engine uses this for the
    ``ai.news.impact.<sym>`` subject; a future fan-out enhancement
    can publish on every member of :attr:`symbols`.
    """
    universe = {s for s in tracked_symbols if s}
    if not universe:
        # Without a tracked-symbol universe we cannot map anything;
        # fall back to whatever the adapter hinted at as a best-effort
        # passthrough.
        hinted = tuple(s for s in entities.symbols_hinted if s)
        if hinted:
            return SymbolMap(symbols=hinted, primary=hinted[0])
        return SymbolMap()

    matched: list[str] = []
    seen: set[str] = set()

    for hint in entities.symbols_hinted:
        if hint in universe and hint not in seen:
            matched.append(hint)
            seen.add(hint)

    if not matched:
        upper_text = headline.text.upper()
        for token in _TOKEN_RE.findall(upper_text):
            if token in universe and token not in seen:
                matched.append(token)
                seen.add(token)

    if not matched:
        return SymbolMap()
    return SymbolMap(symbols=tuple(matched), primary=matched[0])


# ---------------------------------------------------------------------------
# Fast-path orchestrator ----------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FastPathResult:
    """Bundle returned by :meth:`FastPath.run`.

    Attributes:
        headline:      Echoed for traceability.
        sentiment:     :class:`SentimentResult` from FinBERT.
        entities:      :class:`EntityExtraction` for downstream stages.
        impact_magnitude: Bounded magnitude in ``[0.0, 1.0]``.
        mapping:       :class:`SymbolMap` of tracked symbols hit by
                       the headline.
    """

    headline: Headline
    sentiment: SentimentResult
    entities: EntityExtraction
    impact_magnitude: float
    mapping: SymbolMap


@dataclass
class FastPath:
    """Compose entity_extract + finbert_sentiment + impact_score + symbol_map.

    Construction:

    * ``finbert`` — already-loaded
      :class:`hedge_warm_ai.onnx_runtime.FinBERTSentiment`. The
      session is loaded by the service binary at startup; the
      :class:`FastPath` does not load weights itself.
    * ``tracked_symbols_provider`` — callable returning the current
      tracked-symbol universe. The engine passes a closure over its
      live :class:`hedge_warm_ai.news.config.NewsConfig`'s ``symbols``
      tuple so an in-flight config reload immediately propagates.

    The class is intentionally minimal — the fast-path orchestration
    is a one-line composition of four pure functions plus the FinBERT
    call. Everything sequential, everything bounded.
    """

    finbert: FinBERTSentiment
    tracked_symbols_provider: "callable" = field(default=lambda: ())

    async def run(self, headline: Headline) -> FastPathResult:
        """Execute the four fast-path stages on *headline*."""
        entities = entity_extract(headline)
        sentiment = await self.finbert.score(headline.text)
        magnitude = impact_score(sentiment, entities)
        tracked = self.tracked_symbols_provider() or ()
        mapping = symbol_map(headline, entities, tracked_symbols=tracked)
        return FastPathResult(
            headline=headline,
            sentiment=sentiment,
            entities=entities,
            impact_magnitude=magnitude,
            mapping=mapping,
        )


__all__ = [
    "EntityExtraction",
    "FastPath",
    "FastPathResult",
    "SymbolMap",
    "entity_extract",
    "impact_score",
    "symbol_map",
]
