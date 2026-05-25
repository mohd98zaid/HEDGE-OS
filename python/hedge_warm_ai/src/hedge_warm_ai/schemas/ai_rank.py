"""Pydantic mirror of ``ai_rank.schema.json`` — ``ai.rank.<correlation_id>``.

Subject: ``ai.rank.<cid>``. Producer: ``AI_Trade_Ranking_Engine``.
Consumers: Risk_Engine (via WarmCache), Signal_Engine, UI.
"""

from __future__ import annotations

from typing import ClassVar, Annotated

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

# `[0.0, 1.0]` factor; reused for every ranking factor and the final score.
UnitFactor = Annotated[float, Field(ge=0.0, le=1.0)]


class RankingFactors(BaseModel):
    """Component factors of `Trade_Confidence_Score` (R17.1)."""

    model_config = ConfigDict(extra="forbid")

    orderflow: UnitFactor
    technical_strength: UnitFactor
    news_sentiment: UnitFactor
    market_regime: UnitFactor
    trader_discipline: UnitFactor


class RankedSignal(BaseModel):
    """`ai.rank.<correlation_id>` event."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    signal_id: Annotated[str, Field(min_length=1, max_length=64)]
    trade_confidence_score: UnitFactor
    factors: RankingFactors
    shadow: bool
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_rank")
