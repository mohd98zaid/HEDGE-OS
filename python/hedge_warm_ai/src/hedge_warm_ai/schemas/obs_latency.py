"""Pydantic mirror of ``obs_latency.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

Stage = Literal[
    "TickIngest",
    "FeatureExtraction",
    "AiScoringFetch",
    "RiskCheck",
    "ExecutionRouting",
    "BrokerSubmit",
]


class LatencyRecordJson(BaseModel):
    """JSON mirror of ``LatencyRecord_v1`` for non-Hot_Path consumers (R27.4)."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    stage: Stage
    nanos: Annotated[int, Field(ge=0)]
    budget_nanos: Annotated[int, Field(ge=0)]
    breach: bool

    JSON_SCHEMA: ClassVar[str] = load_schema("obs_latency")
