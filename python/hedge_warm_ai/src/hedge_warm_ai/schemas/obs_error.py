"""Pydantic mirror of ``obs_error.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class ObsError(BaseModel):
    """Typed error event from Hot_Path stages and Warm_AI services."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    source: Annotated[str, Field(min_length=1, max_length=64)]
    code: Annotated[str, Field(min_length=1, max_length=64)]
    severity: Literal["info", "warn", "error", "critical"]
    message: Annotated[str, Field(min_length=1, max_length=4096)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("obs_error")
