"""Domain types for the News_Intelligence_Engine.

Source adapters yield :class:`Headline` records with a uniform shape;
all downstream stages (dedup, fast path, slow path, embedding sink)
operate on the same type so a new adapter only needs to translate its
upstream feed payload into a :class:`Headline` and inherits the entire
pipeline for free.

Both :class:`Headline` and :class:`HeadlineSource` are immutable —
``slots=True`` and ``frozen=True`` — so accidental mutation is
impossible and the engine's allocation profile is bounded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Final


class HeadlineSource(str, Enum):
    """Canonical identifier for each configured news source (R12.1).

    The string values are echoed verbatim into:

    * :class:`Headline.source` for traceability,
    * the slow-path Ollama prompt so the model sees source provenance,
    * the Qdrant payload (``payload["source"]``) for filtered kNN,
    * the structured-log ``source`` field on every emission.
    """

    REUTERS = "reuters"
    MONEYCONTROL = "moneycontrol"
    NSE_FILINGS = "nse_filings"
    RBI = "rbi"
    TWITTER = "twitter"
    TELEGRAM = "telegram"
    ECONOMIC_TIMES = "economic_times"
    BROKER_FEED = "broker_feed"


#: The complete set of source identifiers (R12.1). Used by the source
#: adapter registry to validate that every configured adapter has a
#: known canonical source label.
ALL_HEADLINE_SOURCES: Final[frozenset[HeadlineSource]] = frozenset(HeadlineSource)


@dataclass(frozen=True, slots=True)
class Headline:
    """One normalised headline observed by the engine.

    Attributes:
        headline_id: Stable id supplied by the source adapter.
            Adapters that lack a native id MUST construct a stable
            content-derived id (e.g. ``f"{source}:{sha1(text)[:16]}"``)
            so a re-fetch of the same item maps to the same id and the
            :class:`hedge_warm_ai.news.dedup.Dedup` filter behaves
            deterministically across replays.
        source: One of :class:`HeadlineSource`.
        text: The headline body the fast path scores. Whitespace is
            preserved verbatim so the FinBERT tokenizer reproduces
            the same byte sequence on replay.
        url: Optional canonical URL for the article. Embedded in the
            Qdrant payload for downstream UI rendering. Empty string
            when the upstream feed has none (e.g. broker feed
            heartbeats).
        symbols_hint: Optional pre-extracted symbols supplied by the
            source adapter. The fast path's :func:`symbol_map` step
            falls back to text-based extraction when this list is
            empty. Stored as a tuple so the dataclass remains
            hashable.
        ts_ns: Wall-clock ns timestamp the source observed the
            headline at. Forwarded into the resulting
            :class:`hedge_warm_ai.schemas.NewsImpact` payload's
            ``ts_ns`` field unchanged.
        correlation_id: Optional 16-byte correlation id for trace
            propagation. ``b""`` when the engine should mint a fresh
            one (the engine consults
            :func:`hedge_warm_ai.onnx_runtime.new_correlation_id`).
    """

    headline_id: str
    source: HeadlineSource
    text: str
    url: str = ""
    symbols_hint: tuple[str, ...] = field(default_factory=tuple)
    ts_ns: int = 0
    correlation_id: bytes = b""

    def __post_init__(self) -> None:
        if not self.headline_id:
            raise ValueError("headline_id must be non-empty")
        if not self.text:
            raise ValueError("text must be non-empty")
        if self.ts_ns < 0:
            raise ValueError(f"ts_ns must be >= 0, got {self.ts_ns!r}")
        if self.correlation_id and len(self.correlation_id) != 16:
            raise ValueError(
                "correlation_id must be exactly 16 bytes when provided; "
                f"got {len(self.correlation_id)} bytes"
            )


__all__ = [
    "ALL_HEADLINE_SOURCES",
    "Headline",
    "HeadlineSource",
]
