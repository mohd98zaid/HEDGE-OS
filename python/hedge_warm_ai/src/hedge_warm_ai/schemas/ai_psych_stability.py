"""Pydantic mirror of ``ai_psych_stability.schema.json`` — ``ai.psych.stability``."""

from __future__ import annotations

from typing import ClassVar, Annotated, List

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

UnitFactor = Annotated[float, Field(ge=0.0, le=1.0)]


class StabilityComponents(BaseModel):
    """Component factors of `Trader_Stability_Score` (R16.2)."""

    model_config = ConfigDict(extra="forbid")

    discipline: UnitFactor
    emotional_control: UnitFactor
    risk_consistency: UnitFactor
    patience: UnitFactor


class PsychStability(BaseModel):
    """`Trader_Psychology_Engine` stability snapshot (R16)."""

    model_config = ConfigDict(extra="forbid")

    score: UnitFactor
    components: StabilityComponents
    behaviors: Annotated[List[Annotated[str, Field(min_length=1, max_length=64)]], Field(max_length=32)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_psych_stability")
