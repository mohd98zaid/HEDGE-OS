"""Pydantic mirror of ``ops_action.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class OpsAction(BaseModel):
    """Self_Healing_Supervisor remedial action (R25)."""

    model_config = ConfigDict(extra="forbid")

    target: Annotated[str, Field(min_length=1, max_length=64)]
    action: Literal["restart", "failover", "drain", "isolate", "reconnect", "warn"]
    reason: Annotated[str, Field(min_length=1, max_length=512)]
    attempt: Annotated[Optional[int], Field(ge=0, le=1000)] = None
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ops_action")
