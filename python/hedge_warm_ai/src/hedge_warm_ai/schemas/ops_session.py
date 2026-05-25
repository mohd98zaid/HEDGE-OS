"""Pydantic mirror of ``ops_session.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class OpsSession(BaseModel):
    """Session manager session-start / session-end announcement (R31)."""

    model_config = ConfigDict(extra="forbid")

    session_id: Annotated[int, Field(ge=0)]
    phase: Literal["start", "end"]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ops_session")
