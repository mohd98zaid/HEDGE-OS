"""Stage 5 of the retrieval pipeline — recommendation_generation (R19.5).

Parses the streamed Ollama text into a typed
:class:`~hedge_memory_rag.retrieval.recommendation.Recommendation`.

The instruction text emitted in Stage 3 asks the LLM to reply with a
single strict-JSON object; in practice models occasionally wrap the
JSON in markdown fences (``"```json ..."```"```) or precede it with a
short preamble. The parser is conservative:

1. Try to ``json.loads`` the entire text first.
2. If that fails, scan for the first balanced ``{ ... }`` substring
   and parse that. (Covers the markdown-fenced case without invoking
   a regex DSL.)
3. If parsing still fails, raise :class:`RecommendationParseError`
   with the raw text attached.

The parser also normalises the LLM's free-form action verb through
:func:`hedge_memory_rag.retrieval.recommendation.normalise_action`, so
a model that says ``"go_long"`` still produces ``action="buy"`` rather
than failing validation.
"""

from __future__ import annotations

import json
from typing import Any

import structlog

from .errors import RecommendationParseError
from .recommendation import Recommendation, normalise_action
from .records import StreamedReasoning

_LOG = structlog.get_logger(__name__)


def recommendation_generation(reasoning: StreamedReasoning) -> Recommendation:
    """Run Stage 5: parse the streamed text into a :class:`Recommendation`.

    Args:
        reasoning: Output of Stage 4.

    Raises:
        RecommendationParseError: the streamed text does not contain a
            parseable JSON object, or the JSON object fails the
            :class:`Recommendation` schema.

    Returns:
        The typed :class:`Recommendation`.
    """
    correlation_id = reasoning.correlation_id
    text = reasoning.text or ""
    payload = _extract_json_object(text)
    if payload is None:
        raise RecommendationParseError(
            "no JSON object found in Ollama reasoning output",
            correlation_id=correlation_id,
            raw_text=text,
        )

    # Normalise the action verb before validation so the small set of
    # canonical aliases doesn't have to live in the pydantic model.
    raw_action = payload.get("action")
    if isinstance(raw_action, str):
        try:
            payload["action"] = normalise_action(raw_action)
        except ValueError as exc:
            raise RecommendationParseError(
                f"recommendation action {raw_action!r} is not in the alias table: {exc}",
                correlation_id=correlation_id,
                raw_text=text,
            ) from exc

    # Stamp metadata that the LLM cannot reliably emit.
    payload["correlation_id"] = correlation_id
    payload["raw_text"] = text
    payload["role"] = reasoning.role
    payload["model"] = reasoning.model
    payload["metrics"] = dict(reasoning.metrics)

    # ``confidence`` and ``rationale`` may be missing in the LLM output;
    # the pydantic defaults handle that.
    try:
        return Recommendation.model_validate(payload)
    except Exception as exc:  # pydantic.ValidationError is also captured
        raise RecommendationParseError(
            f"recommendation JSON failed validation: {exc}",
            correlation_id=correlation_id,
            raw_text=text,
        ) from exc


# ---------------------------------------------------------------------------
# JSON-object extractor -----------------------------------------------------
# ---------------------------------------------------------------------------


def _extract_json_object(text: str) -> dict[str, Any] | None:
    """Return the first JSON object in ``text`` (loose-prose tolerant)."""
    if not text:
        return None
    stripped = text.strip()
    if not stripped:
        return None

    # Fast path: the whole thing already is one JSON object.
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, dict):
            return obj

    # Slow path: locate the first balanced ``{ ... }``. We scan
    # character-by-character so a stray ``{`` inside a string literal
    # doesn't fool us — :mod:`json` reports an error and we move on.
    start_indices = [i for i, ch in enumerate(stripped) if ch == "{"]
    for start in start_indices:
        depth = 0
        in_string = False
        escape = False
        for end in range(start, len(stripped)):
            ch = stripped[end]
            if escape:
                escape = False
                continue
            if ch == "\\" and in_string:
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = stripped[start : end + 1]
                    try:
                        obj = json.loads(candidate)
                    except json.JSONDecodeError:
                        break  # try next start
                    if isinstance(obj, dict):
                        return obj
                    break
    return None


__all__ = ["recommendation_generation"]
