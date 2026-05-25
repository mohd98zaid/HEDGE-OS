"""Pydantic mirror of ``ai_news_impact.schema.json`` — ``ai.news.impact.<symbol>``."""

from __future__ import annotations

from typing import ClassVar, Annotated

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class NewsImpact(BaseModel):
    """`News_Intelligence` per-symbol impact emission (R13)."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    headline_id: Annotated[str, Field(min_length=1, max_length=128)]
    sentiment: Annotated[float, Field(ge=-1.0, le=1.0)]
    impact_magnitude: Annotated[float, Field(ge=0.0, le=1.0)]
    fast_path: bool
    slow_path_pending: bool
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_news_impact")
