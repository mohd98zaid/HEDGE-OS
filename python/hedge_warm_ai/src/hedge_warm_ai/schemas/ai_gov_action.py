"""Pydantic mirror of ``ai_gov_action.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class AiGovAction(BaseModel):
    """`AI_Governance` action when a Warm_AI component breaches a threshold."""

    model_config = ConfigDict(extra="forbid")

    component: Annotated[str, Field(min_length=1, max_length=64)]
    action: Literal["reduce_influence", "shadow_mode", "freeze", "rollback"]
    metric: Literal["drift", "accuracy", "latency", "error_rate"]
    value: float
    threshold: float
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_gov_action")
