"""Pydantic mirror of ``trader_intent_killswitch.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Optional

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class TraderIntentKillSwitch(BaseModel):
    """Trader-issued kill-switch toggle (R20.6)."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    engaged: bool
    reason: Annotated[Optional[str], Field(max_length=512)] = None
    actor: Annotated[str, Field(min_length=1, max_length=64)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("trader_intent_killswitch")
