"""Pydantic mirror of ``ai_psych_intervention.schema.json`` — ``ai.psych.intervention``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class PsychIntervention(BaseModel):
    """`Trader_Psychology_Engine` intervention recommendation."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["cooldown", "size_reduction", "kill_switch", "warning"]
    trigger_score: Annotated[float, Field(ge=0.0, le=1.0)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_psych_intervention")
