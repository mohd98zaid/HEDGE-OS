"""Pydantic mirror of ``trader_intent_strategy_toggle.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

StrategyName = Literal[
    "OpeningRangeBreakout",
    "VwapPullback",
    "MomentumBreakout",
    "LiquiditySweepReversal",
    "OptionsOiExpansionBreakout",
    "VolatilityCompressionBreakout",
]


class TraderIntentStrategyToggle(BaseModel):
    """Trader toggling a Signal_Engine strategy on or off (R20.7)."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    strategy: StrategyName
    enabled: bool
    actor: Annotated[str, Field(min_length=1, max_length=64)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("trader_intent_strategy_toggle")
