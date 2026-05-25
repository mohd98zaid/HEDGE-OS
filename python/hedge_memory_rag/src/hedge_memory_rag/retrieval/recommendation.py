"""Typed :class:`Recommendation` returned by Stage 5 of the retrieval pipeline.

The design (R19.5) calls for a final "recommendation generation" step.
We model the recommendation as a strict pydantic record so the rest of
the Warm_AI_Pipeline (Trade_Ranking, Governance, Shadow_Mode, Journal)
can persist it and reason about it without re-parsing free-form text.

The shape mirrors the AI events committed under
``hedge_warm_ai.schemas`` (frozen, ``extra=forbid``, value-bounded
floats) so persisted recommendations round-trip cleanly back through
the existing JSON-Schema-validated event surface.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Final, Literal, Mapping

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    ValidationError,
)

#: Canonical action verbs the LLM is expected to emit. Keeping the set
#: small lets the parser fail closed on unexpected verbs rather than
#: silently shipping an unknown action through to the Risk_Engine.
RecommendationAction = Literal[
    "buy",
    "sell",
    "hold",
    "reduce",
    "exit",
    "abstain",
]


class Recommendation(BaseModel):
    """Structured outcome of one trader-event reasoning request.

    Attributes:
        correlation_id: Identifier copied verbatim from the originating
            :class:`RetrievalRequest`. Lets persistence layers join the
            recommendation back to the trader event without parsing the
            payload.
        action: One of the allowed action verbs. The LLM's free-form
            verb is mapped through a small alias table inside the
            parser; unknown verbs fail closed.
        symbol: Optional symbol the action applies to. ``None`` for
            account-wide recommendations (e.g. ``"abstain"``,
            ``"hold"``).
        confidence: Model-reported confidence in ``[0.0, 1.0]``.
            Bounded so downstream multipliers (Adaptive_Risk) never
            drift outside their domain.
        rationale: Short prose justification. Persisted alongside the
            recommendation so traders and the AI_Trade_Journal can
            inspect it.
        sources: References (point ids, table rows, news headlines)
            the LLM cited when generating the recommendation. Stored
            as a list of opaque strings; empty when the LLM did not
            cite anything.
        raw_text: The full streamed output from Ollama (post-
            concatenation). Useful for debugging unparseable outputs
            and for offline post-mortems.
        role: The Ollama role that ultimately served the response.
            Equal to the requested role unless the client routed to a
            fallback.
        model: GGUF tag of the responding daemon.
        metrics: Trailing-chunk metrics from the Ollama response
            (``eval_count``, ``eval_duration``, etc.). May be empty
            when the daemon did not emit any.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    correlation_id: Annotated[str, Field(min_length=1, max_length=64)]
    action: RecommendationAction
    symbol: Annotated[str, Field(min_length=1, max_length=32)] | None = None
    confidence: Annotated[float, Field(ge=0.0, le=1.0)] = 0.0
    rationale: Annotated[str, Field(max_length=8192)] = ""
    sources: tuple[str, ...] = ()
    raw_text: Annotated[str, Field(max_length=65536)] = ""
    role: Annotated[str, Field(min_length=1, max_length=32)] = "qwen"
    model: Annotated[str, Field(min_length=1, max_length=128)] = ""
    metrics: Mapping[str, Any] = Field(default_factory=dict)

    @field_validator("sources", mode="before")
    @classmethod
    def _coerce_sources(cls, v: Any) -> tuple[str, ...]:
        if v is None:
            return ()
        if isinstance(v, str):
            return (v,) if v else ()
        if isinstance(v, (list, tuple)):
            out: list[str] = []
            for item in v:
                if item is None:
                    continue
                out.append(str(item))
            return tuple(out)
        raise ValueError(f"sources must be a string or list of strings, got {type(v).__name__}")


#: Re-exported for callers that need to validate a JSON blob without
#: importing pydantic directly.
def parse_recommendation_json(blob: str) -> Recommendation:
    """Parse a JSON string into a :class:`Recommendation`.

    Raises:
        ValueError: ``blob`` is not valid JSON or fails pydantic
            validation. The caller wraps this into the typed
            :class:`hedge_memory_rag.retrieval.errors.RecommendationParseError`
            with the raw text attached.
    """
    try:
        obj = json.loads(blob)
    except json.JSONDecodeError as exc:
        raise ValueError(f"recommendation JSON is malformed: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError(
            f"recommendation JSON must decode to an object, got {type(obj).__name__}"
        )
    try:
        return Recommendation.model_validate(obj)
    except ValidationError as exc:
        raise ValueError(f"recommendation JSON failed validation: {exc}") from exc


_ACTION_ALIASES: Final[Mapping[str, RecommendationAction]] = {
    "buy": "buy",
    "long": "buy",
    "go_long": "buy",
    "enter_long": "buy",
    "sell": "sell",
    "short": "sell",
    "go_short": "sell",
    "enter_short": "sell",
    "hold": "hold",
    "wait": "hold",
    "stay": "hold",
    "reduce": "reduce",
    "trim": "reduce",
    "scale_out": "reduce",
    "exit": "exit",
    "close": "exit",
    "flatten": "exit",
    "abstain": "abstain",
    "skip": "abstain",
    "no_action": "abstain",
    "none": "abstain",
}


def normalise_action(raw: str) -> RecommendationAction:
    """Map a free-form action verb to one of the allowed literals.

    Raises:
        ValueError: The verb is not a known alias.
    """
    lowered = raw.strip().lower().replace("-", "_").replace(" ", "_")
    try:
        return _ACTION_ALIASES[lowered]
    except KeyError as exc:
        raise ValueError(
            f"unknown recommendation action {raw!r}; "
            f"expected one of {sorted(set(_ACTION_ALIASES.values()))}"
        ) from exc


__all__ = [
    "Recommendation",
    "RecommendationAction",
    "normalise_action",
    "parse_recommendation_json",
]
