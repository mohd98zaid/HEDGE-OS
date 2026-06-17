"""Property-based tests for News_Intelligence_Engine (task 21.2).

Validates:
    - Impact score bounded in [0, 1]
    - Keyword bonus capped
    - Entity extraction is deterministic
    - Symbol mapping respects universe

**Validates: Requirements 12.1, 12.2, 12.3**
"""

from __future__ import annotations

from hypothesis import given, assume, settings
from hypothesis import strategies as st

from hedge_warm_ai.news.fast_path import (
    EntityExtraction,
    SymbolMap,
    entity_extract,
    impact_score,
    symbol_map,
)
from hedge_warm_ai.news.headline import Headline, HeadlineSource
from hedge_warm_ai.onnx_runtime.nlp import SentimentResult


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

def arb_sentiment() -> st.SearchStrategy[SentimentResult]:
    return st.builds(
        SentimentResult,
        label=st.sampled_from(["positive", "negative", "neutral"]),
        score=st.floats(min_value=0.0, max_value=1.0),
        sentiment=st.floats(min_value=-1.0, max_value=1.0),
        probabilities=st.just({"positive": 0.33, "negative": 0.33, "neutral": 0.34}),
    )


def arb_headline() -> st.SearchStrategy[Headline]:
    return st.builds(
        Headline,
        headline_id=st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"),
        source=st.just(HeadlineSource.REUTERS),
        text=st.text(min_size=1, max_size=200),
        symbols_hint=st.lists(st.text(min_size=1, max_size=10, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"), max_size=5),
        ts_ns=st.integers(min_value=0, max_value=10_000_000_000),
    )


# ---------------------------------------------------------------------------
# Impact score properties
# ---------------------------------------------------------------------------

@given(sentiment=arb_sentiment())
def test_impact_score_bounded_for_empty_entities(sentiment: SentimentResult) -> None:
    """Property: impact score is in [0, 1] when no keywords are hit."""
    entities = EntityExtraction()
    score = impact_score(sentiment, entities)
    assert 0.0 <= score <= 1.0, f"impact_score={score} out of [0, 1]"


@given(
    sentiment_magnitude=st.floats(min_value=0.0, max_value=1.0),
    num_keywords=st.integers(min_value=0, max_value=20),
)
def test_impact_score_bounded_overall(
    sentiment_magnitude: float,
    num_keywords: int,
) -> None:
    """Property: impact score is always in [0, 1] regardless of inputs."""
    sentiment = SentimentResult(
        label="positive",
        score=sentiment_magnitude,
        sentiment=sentiment_magnitude,
        probabilities={"positive": sentiment_magnitude, "negative": 0.0, "neutral": 1.0 - sentiment_magnitude},
    )
    entities = EntityExtraction(
        keywords_hit=tuple(f"kw{i}" for i in range(num_keywords)),
    )
    score = impact_score(sentiment, entities)
    assert 0.0 <= score <= 1.0, f"impact_score={score} out of [0, 1]"


@given(
    sentiment_magnitude=st.floats(min_value=0.0, max_value=1.0),
    num_keywords=st.integers(min_value=0, max_value=50),
)
def test_keyword_bonus_capped(
    sentiment_magnitude: float,
    num_keywords: int,
) -> None:
    """Property: keyword bonus never exceeds keyword_cap (default 0.50)."""
    sentiment = SentimentResult(
        label="positive",
        score=sentiment_magnitude,
        sentiment=sentiment_magnitude,
        probabilities={"positive": sentiment_magnitude, "negative": 0.0, "neutral": 1.0 - sentiment_magnitude},
    )
    entities = EntityExtraction(
        keywords_hit=tuple(f"kw{i}" for i in range(num_keywords)),
    )
    score = impact_score(sentiment, entities)
    assert score <= 1.0, f"impact_score={score} exceeded 1.0 with cap"
    bonus_part = score - abs(sentiment_magnitude)
    assert bonus_part <= 0.50 + 1e-10, f"bonus={bonus_part} exceeded cap"


@given(sentiment_magnitude=st.floats(min_value=0.0, max_value=1.0))
def test_zero_keywords_gives_bare_sentiment(sentiment_magnitude: float) -> None:
    """Property: with zero keywords, impact_score equals abs(sentiment)."""
    sentiment = SentimentResult(
        label="positive",
        score=sentiment_magnitude,
        sentiment=sentiment_magnitude,
        probabilities={"positive": sentiment_magnitude, "negative": 0.0, "neutral": 1.0 - sentiment_magnitude},
    )
    entities = EntityExtraction()
    score = impact_score(sentiment, entities)
    assert abs(score - sentiment_magnitude) < 1e-10


# ---------------------------------------------------------------------------
# Entity extraction determinism
# ---------------------------------------------------------------------------

@given(text=st.text(min_size=1, max_size=500))
def test_entity_extract_is_deterministic(text: str) -> None:
    """Property: same headline always produces the same entity extraction."""
    headline = Headline(headline_id="h1", source=HeadlineSource.REUTERS, text=text)
    result1 = entity_extract(headline)
    result2 = entity_extract(headline)
    assert result1.keywords_hit == result2.keywords_hit
    assert result1.symbols_hinted == result2.symbols_hinted


# ---------------------------------------------------------------------------
# Symbol mapping properties
# ---------------------------------------------------------------------------

@given(
    hint=st.text(min_size=1, max_size=10, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
)
def test_symbol_map_prefers_hint(hint: str) -> None:
    """Property: when hint is in universe, it appears in the result."""
    headline = Headline(headline_id="h1", source=HeadlineSource.REUTERS, text="random text", symbols_hint=(hint,))
    entities = entity_extract(headline)
    result = symbol_map(headline, entities, tracked_symbols=[hint])
    assert hint in result.symbols
    assert result.primary == hint


def test_symbol_map_empty_universe_returns_empty() -> None:
    """Property: empty tracked_symbols produces empty result (no hints)."""
    headline = Headline(headline_id="h1", source=HeadlineSource.REUTERS, text="RANDOM STOCK")
    entities = entity_extract(headline)
    result = symbol_map(headline, entities, tracked_symbols=[])
    assert result.symbols == ()
    assert result.primary is None
