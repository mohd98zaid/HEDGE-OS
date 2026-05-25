"""Pydantic mirror of ``ops_warmode.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class OpsWarMode(BaseModel):
    """Market_Open_War_Mode start/end announcement (R26)."""

    model_config = ConfigDict(extra="forbid")

    session_id: Annotated[int, Field(ge=0)]
    phase: Literal["start", "end"]
    min_confidence: Annotated[Optional[float], Field(ge=0.0, le=1.0)] = None
    scan_multiplier: Annotated[Optional[float], Field(ge=0.0, le=100.0)] = None
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ops_warmode")
