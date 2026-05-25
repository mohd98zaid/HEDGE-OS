"""Pydantic mirror of ``ai_ollama_degraded.schema.json``."""

from __future__ import annotations

from typing import ClassVar, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from ._loader import load_schema


class OllamaDegraded(BaseModel):
    """`Ollama_Infrastructure` degraded-state event (R10.9)."""

    model_config = ConfigDict(extra="forbid")

    model: Annotated[str, Field(min_length=1, max_length=128)]
    fallback_model: Annotated[str, Field(min_length=1, max_length=128)]
    reason: Literal["timeout", "unresponsive", "oom", "crashed", "version_mismatch"]
    ts_ns: Annotated[int, Field(ge=0)]

    JSON_SCHEMA: ClassVar[str] = load_schema("ai_ollama_degraded")
