"""Pydantic mirror of ``ai_journal_entry.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class AiJournalEntry(BaseModel):
    """`AI_Trade_Journal` post-trade narrative entry (R18)."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    trade_id: Annotated[str, Field(min_length=1, max_length=64)]
    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    side: Literal["Buy", "Sell"]
    quantity: Annotated[int, Field(ge=0)]
    entry_paise: int
    exit_paise: int
    pnl_inr: float
    narrative: Annotated[str, Field(min_length=1, max_length=8192)]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_journal_entry")
