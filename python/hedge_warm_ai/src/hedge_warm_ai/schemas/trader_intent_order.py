"""Pydantic mirror of ``trader_intent_order.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ._loader import load_schema


class TraderIntentOrder(BaseModel):
    """Trader-issued manual order intent (R20)."""

    model_config = ConfigDict(extra="forbid")

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    symbol: Annotated[str, Field(min_length=1, max_length=32)]
    side: Literal["Buy", "Sell"]
    quantity: Annotated[int, Field(ge=1, le=1_000_000)]
    order_type: Literal["Market", "Limit"]
    limit_paise: Optional[int] = None
    exchange: Literal["NSE", "BSE"]
    actor: Annotated[str, Field(min_length=1, max_length=64)]
    ts_ns: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def _limit_requires_price(self) -> "TraderIntentOrder":
        # JSON Schema's `if/then` translated to a runtime guard so the same
        # invariant holds in pydantic-side validation.
        if self.order_type == "Limit" and self.limit_paise is None:
            raise ValueError("limit_paise is required when order_type == 'Limit'")
        return self

    JSON_SCHEMA: ClassVar[str] = load_schema("trader_intent_order")
