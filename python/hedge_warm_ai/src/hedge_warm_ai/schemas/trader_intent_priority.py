"""Pydantic mirror of ``trader_intent_priority.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

PriorityTier = Literal["P1", "P2", "P3", "P4"]


class TraderIntentPriority(BaseModel):
    """Trader-issued symbol priority change (R20.8)."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    to: PriorityTier
    actor: Annotated[str, Field(min_length=1, max_length=64)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("trader_intent_priority")
