"""Pydantic mirror of ``ai_regime_changed.schema.json`` — ``ai.regime.changed``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

Regime = Literal[
    "Trending",
    "Sideways",
    "Panic",
    "HighVolatility",
    "NewsDriven",
    "LiquidityCrisis",
    "LowParticipation",
]


class RegimeChanged(BaseModel):
    """`Market_Regime_Engine` edge-triggered regime change (R12)."""

    model_config = ConfigDict(extra="forbid")

    from_: Regime = Field(alias="from")
    to: Regime
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_regime_changed")
