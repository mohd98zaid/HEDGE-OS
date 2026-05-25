"""Pydantic mirror of ``ai_priority_changed.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

PriorityTier = Literal["P1", "P2", "P3", "P4"]


class PriorityChanged(BaseModel):
    """`Symbol_Priority_Engine` edge-triggered priority change (R14)."""

    model_config = ConfigDict(extra="forbid")

    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    from_: PriorityTier = Field(alias="from")
    to: PriorityTier
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_priority_changed")
