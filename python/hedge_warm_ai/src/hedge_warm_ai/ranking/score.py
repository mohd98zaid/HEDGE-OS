"""Trade_Confidence_Score formula and ``RankingFactors`` value type.

This module is the source of truth for the ``Trade_Confidence_Score``
closed-form expression. It is invoked by
:class:`hedge_warm_ai.ranking.AiTradeRankingEngine` on every
``sig.emitted`` event seen by the engine.

Formula (R17.1, R17.2 — Property 4 — Score and Formula Equivalence)::

    Trade_Confidence_Score =
        clamp(0.30×Orderflow + 0.25×TechnicalStrength
              + 0.20×NewsSentiment + 0.15×MarketRegime
              + 0.10×TraderDiscipline,
              0.0, 1.0)

The five weights sum to 1.0 by design so each factor in [0.0, 1.0]
already produces an unclamped raw value in [0.0, 1.0]; the explicit
clamp is **kept** because:

1. R17.2 calls it out *verbatim* ("THE AI_Trade_Ranking_Engine SHALL
   constrain Trade_Confidence_Score to the range [0.0, 1.0]").
2. Float-arithmetic round-off can produce values like
   ``1.0000000000000002`` for ``O=T=N=M=D=1.0`` — the clamp keeps the
   wire payload schema-valid (``ai_rank.schema.json`` has
   ``maximum: 1.0``).
3. We accept inputs *outside* [0.0, 1.0] without raising; the clamp is
   the bound contract (matches Property 4 — "outputs are bound, not
   the inputs").

Property 4 is verified by task 26.2 against this exact function (the
test imports :func:`compute_trade_confidence_score` directly), so the
formula's audit-trail is unambiguous: the design specifies it, this
module implements it as named module constants, and the property test
asserts equivalence over the full input space.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# ---------------------------------------------------------------------------
# Formula constants ---------------------------------------------------------
# ---------------------------------------------------------------------------

#: Weight of the ``Orderflow`` factor in the
#: :func:`compute_trade_confidence_score` formula (R17.1, R17.2 —
#: Property 4 — Score and Formula Equivalence).
ORDERFLOW_WEIGHT: Final[float] = 0.30

#: Weight of the ``TechnicalStrength`` factor.
TECHNICAL_STRENGTH_WEIGHT: Final[float] = 0.25

#: Weight of the ``NewsSentiment`` factor.
NEWS_SENTIMENT_WEIGHT: Final[float] = 0.20

#: Weight of the ``MarketRegime`` factor.
MARKET_REGIME_WEIGHT: Final[float] = 0.15

#: Weight of the ``TraderDiscipline`` factor.
TRADER_DISCIPLINE_WEIGHT: Final[float] = 0.10


# Sanity check: the five weights must sum to exactly 1.0 (the design
# specifies them as 0.30 + 0.25 + 0.20 + 0.15 + 0.10). This is asserted
# at module import time so a future edit that breaks the formula fails
# immediately rather than at score-emission time.
_WEIGHT_SUM: Final[float] = (
    ORDERFLOW_WEIGHT
    + TECHNICAL_STRENGTH_WEIGHT
    + NEWS_SENTIMENT_WEIGHT
    + MARKET_REGIME_WEIGHT
    + TRADER_DISCIPLINE_WEIGHT
)
assert _WEIGHT_SUM == 1.0, (
    f"Trade_Confidence_Score weights must sum to 1.0 "
    f"(R17.1 / R17.2 / Property 4); got {_WEIGHT_SUM!r}"
)


# ---------------------------------------------------------------------------
# Factor bundle -------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RankingFactors:
    """Bundle of factor inputs to :func:`compute_trade_confidence_score`.

    The five fields are kept clipped to ``[0.0, 1.0]`` by the engine —
    the formula's clamp will keep the *output* in range even if a
    caller feeds out-of-range inputs, but the raw score is more
    meaningful when the inputs are normalised. The dataclass is
    frozen + slots so accidental mutation is impossible and the engine
    can pass instances cheaply between async tasks.

    Attributes:
        orderflow:          Most recent ``feat.update.<sym>``-derived
                            orderflow strength signal in ``[0.0, 1.0]``.
        technical_strength: Most recent ``feat.update.<sym>``-derived
                            technical-strength signal in ``[0.0, 1.0]``.
        news_sentiment:     Most recent ``ai.news.impact.<sym>``-derived
                            sentiment magnitude in ``[0.0, 1.0]`` (the
                            absolute value of the signed sentiment, so
                            strong negative sentiment counts toward the
                            score the same as strong positive).
        market_regime:      WarmCache ``MarketStability`` factor
                            (task 22.1) in ``[0.0, 1.0]``.
        trader_discipline:  Most recent ``ai.psych.stability``
                            ``components.discipline`` in ``[0.0, 1.0]``.
    """

    orderflow: float = 0.0
    technical_strength: float = 0.0
    news_sentiment: float = 0.0
    market_regime: float = 0.0
    trader_discipline: float = 0.0


# ---------------------------------------------------------------------------
# Formula -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _clamp_unit(value: float) -> float:
    """Clamp *value* to ``[0.0, 1.0]`` (NaN-safe)."""
    if value != value:  # NaN check: NaN != NaN
        return 0.0
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def compute_trade_confidence_score(c: RankingFactors) -> float:
    """Return ``Trade_Confidence_Score`` exactly as specified in R17.1.

    The implementation mirrors the design pseudo-code byte-for-byte::

        raw = (
            0.30 * c.orderflow
            + 0.25 * c.technical_strength
            + 0.20 * c.news_sentiment
            + 0.15 * c.market_regime
            + 0.10 * c.trader_discipline
        )
        return clamp(raw, 0.0, 1.0)   # R17.2

    The named module-level weights (:data:`ORDERFLOW_WEIGHT` etc.) are
    used so the formula's audit-trail is unambiguous and a property
    test (task 26.2) can re-import the same constants to verify
    equivalence over the full input space (Property 4).

    Args:
        c: Live :class:`RankingFactors` — the five component factors.

    Returns:
        The clamped score in ``[0.0, 1.0]``.
    """
    raw = (
        ORDERFLOW_WEIGHT * c.orderflow
        + TECHNICAL_STRENGTH_WEIGHT * c.technical_strength
        + NEWS_SENTIMENT_WEIGHT * c.news_sentiment
        + MARKET_REGIME_WEIGHT * c.market_regime
        + TRADER_DISCIPLINE_WEIGHT * c.trader_discipline
    )
    return _clamp_unit(raw)


__all__ = [
    "MARKET_REGIME_WEIGHT",
    "NEWS_SENTIMENT_WEIGHT",
    "ORDERFLOW_WEIGHT",
    "RankingFactors",
    "TECHNICAL_STRENGTH_WEIGHT",
    "TRADER_DISCIPLINE_WEIGHT",
    "compute_trade_confidence_score",
]
