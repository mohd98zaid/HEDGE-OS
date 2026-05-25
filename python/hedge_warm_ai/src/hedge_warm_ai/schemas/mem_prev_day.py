"""Pydantic mirror of ``mem_prev_day.schema.json`` — ``mem.prev_day.<symbol>``."""

from __future__ import annotations

from typing import ClassVar, Annotated, List, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema

KeyLevelKind = Literal["support", "resistance", "swing_high", "swing_low", "vwap", "open", "close"]


class KeyLevel(BaseModel):
    """A single previous-day key price level."""

    model_config = ConfigDict(extra="forbid")

    kind: KeyLevelKind
    price_paise: int


class PreviousDayMemory(BaseModel):
    """`Previous_Day_Memory_Engine` summary for a symbol (R15)."""

    model_config = ConfigDict(extra="forbid")

    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    session_date: str  # ISO 8601 date — full validation belongs to the consumer
    open_paise: int
    high_paise: int
    low_paise: int
    close_paise: int
    vwap_paise: int
    key_levels: Annotated[List[KeyLevel], Field(max_length=16)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("mem_prev_day")
