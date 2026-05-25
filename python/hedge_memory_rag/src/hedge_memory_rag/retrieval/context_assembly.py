"""Stage 3 of the retrieval pipeline — context_assembly (R19.5).

Deterministic prompt construction from the trader event + retrieved
memories. **No LLM calls happen here** — the function is a pure
text-shaping step so prompt drift between runs is impossible without a
code change.

The resulting prompt is composed of the following labelled sections,
in this fixed order:

1. ``[EVENT]`` — the trader event kind, symbol, timestamp, and
   payload (rendered as canonical JSON).
2. ``[CACHE]`` — last-N trades + last-N news + current regime + current
   Trader_Stability_Score from Stage 1's hot-cache snapshot. Skipped
   when every field is empty.
3. ``[MEMORY]`` — Qdrant kNN hits grouped by collection, each line
   carrying ``score`` and the (truncated) payload preview. Skipped
   when no hits.
4. ``[HISTORY]`` — Timescale rows grouped by hypertable. Each row is
   rendered through :meth:`pydantic.BaseModel.model_dump_json` when
   possible; primitive rows fall back to ``json.dumps``. Skipped
   when no rows.
5. ``[ADDITIONAL]`` — caller-supplied ``request.extra`` mapping.
6. ``[INSTRUCTION]`` — trailing system-style instruction (default or
   caller-supplied) telling the model how to format the response.

Every line is bounded by a per-section character budget so a runaway
payload can never explode the prompt size.
"""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Final, Mapping, Sequence

from pydantic import BaseModel

from ..qdrant.collections import CollectionName
from .config import RetrievalSettings
from .records import AssembledContext, MemoryHits, QdrantHitView

#: Per-record character budget for prompt-rendered values. Long
#: payloads are truncated with an ellipsis so a single oversized
#: row cannot dominate the prompt.
_RECORD_BUDGET_CHARS: Final[int] = 320

#: Default system-style instruction for the LLM. Asks for a strict
#: JSON object so Stage 5 can parse it without language modelling.
_DEFAULT_INSTRUCTION: Final[str] = (
    "Reason about the trader event below using only the provided memories. "
    "Reply with a single strict-JSON object with keys: "
    '"action" (one of buy, sell, hold, reduce, exit, abstain), '
    '"symbol" (string or null), "confidence" (float in [0.0, 1.0]), '
    '"rationale" (short prose), "sources" (array of strings citing '
    "memory point ids or table names). Reply with the JSON object only — "
    "no markdown, no commentary."
)


def context_assembly(
    memory: MemoryHits,
    *,
    settings: RetrievalSettings,
) -> AssembledContext:
    """Run Stage 3: assemble the prompt.

    Args:
        memory: Output of Stage 2.
        settings: Resolved settings (used for read-only diagnostics
            such as the configured ``k`` and ``window_minutes``).

    Returns:
        :class:`AssembledContext` carrying the deterministic prompt
        plus the upstream :class:`MemoryHits`.
    """
    request = memory.event.request
    instruction = request.instruction or _DEFAULT_INSTRUCTION

    sections: list[str] = []
    sections.append(_render_event_section(memory))
    cache_section = _render_cache_section(memory)
    if cache_section is not None:
        sections.append(cache_section)
    memory_section = _render_memory_section(memory)
    if memory_section is not None:
        sections.append(memory_section)
    history_section = _render_history_section(memory, settings=settings)
    if history_section is not None:
        sections.append(history_section)
    extra_section = _render_extra_section(memory)
    if extra_section is not None:
        sections.append(extra_section)
    sections.append(f"[INSTRUCTION]\n{instruction}")

    prompt = "\n\n".join(sections)
    return AssembledContext(memory_hits=memory, prompt=prompt, instruction=instruction)


# ---------------------------------------------------------------------------
# Section renderers ---------------------------------------------------------
# ---------------------------------------------------------------------------


def _render_event_section(memory: MemoryHits) -> str:
    request = memory.event.request
    event = request.event
    payload_str = _render_payload(event.payload)
    body = (
        f"correlation_id: {request.correlation_id}\n"
        f"kind: {event.kind}\n"
        f"symbol: {event.symbol or '<account-wide>'}\n"
        f"ts: {_format_ts(event.ts)}\n"
        f"payload: {payload_str}"
    )
    return f"[EVENT]\n{body}"


def _render_cache_section(memory: MemoryHits) -> str | None:
    event = memory.event
    parts: list[str] = []
    if event.recent_trades:
        parts.append(
            "recent_trades:\n"
            + _render_sequence(event.recent_trades)
        )
    if event.recent_news:
        parts.append(
            "recent_news:\n"
            + _render_sequence(event.recent_news)
        )
    if event.current_regime is not None:
        parts.append(f"current_regime: {_render_value(event.current_regime)}")
    if event.current_stability_score is not None:
        parts.append(
            f"current_stability_score: {_render_value(event.current_stability_score)}"
        )
    if not parts:
        return None
    return "[CACHE]\n" + "\n".join(parts)


def _render_memory_section(memory: MemoryHits) -> str | None:
    if not memory.vector_hits:
        return None

    grouped: dict[CollectionName, list[QdrantHitView]] = {}
    for hit in memory.vector_hits:
        grouped.setdefault(hit.collection, []).append(hit)

    blocks: list[str] = []
    # Iterate over CollectionName declaration order for stable output.
    for member in CollectionName:
        hits = grouped.get(member, [])
        if not hits:
            continue
        lines = [f"  - id={hit.point_id} score={hit.score:.4f} {_render_payload(hit.payload)}"
                 for hit in hits]
        blocks.append(f"{member.value}:\n" + "\n".join(lines))
    return "[MEMORY]\n" + "\n\n".join(blocks)


def _render_history_section(
    memory: MemoryHits,
    *,
    settings: RetrievalSettings,
) -> str | None:
    if not memory.timescale_rows:
        return None
    blocks: list[str] = []
    # Stable ordering: follow the configured tables tuple.
    for table in settings.timescale_tables:
        rows = memory.timescale_rows.get(table, ())
        if not rows:
            continue
        rendered = _render_sequence(rows)
        blocks.append(f"{table}:\n{rendered}")
    if not blocks:
        return None
    header = (
        f"[HISTORY] (window={settings.window_minutes}m)\n"
    )
    return header + "\n\n".join(blocks)


def _render_extra_section(memory: MemoryHits) -> str | None:
    extra = memory.event.request.extra
    if not extra:
        return None
    return "[ADDITIONAL]\n" + _render_payload(extra)


# ---------------------------------------------------------------------------
# Value renderers -----------------------------------------------------------
# ---------------------------------------------------------------------------


def _render_sequence(items: Sequence[Any]) -> str:
    if not items:
        return "  (none)"
    return "\n".join(f"  - {_render_value(item)}" for item in items)


def _render_value(value: Any) -> str:
    """Render any retrieved record into a deterministic, bounded string."""
    if isinstance(value, BaseModel):
        try:
            text = value.model_dump_json(by_alias=True)
        except Exception:
            text = repr(value)
    elif is_dataclass(value) and not isinstance(value, type):
        try:
            text = json.dumps(asdict(value), default=_json_default, sort_keys=True)
        except Exception:
            text = repr(value)
    elif isinstance(value, Mapping):
        text = _render_payload(value)
    elif isinstance(value, (list, tuple)):
        rendered_items = [_render_value(v) for v in value]
        text = "[" + ", ".join(rendered_items) + "]"
    else:
        try:
            text = json.dumps(value, default=_json_default, sort_keys=True)
        except Exception:
            text = repr(value)
    return _truncate(text)


def _render_payload(payload: Mapping[str, Any]) -> str:
    if not payload:
        return "{}"
    try:
        text = json.dumps(payload, default=_json_default, sort_keys=True)
    except Exception:
        text = repr(payload)
    return _truncate(text)


def _truncate(text: str) -> str:
    if len(text) <= _RECORD_BUDGET_CHARS:
        return text
    return text[: _RECORD_BUDGET_CHARS - 1] + "…"


def _json_default(obj: Any) -> Any:
    if isinstance(obj, BaseModel):
        return json.loads(obj.model_dump_json(by_alias=True))
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return obj.hex()
    if isinstance(obj, set):
        return sorted(obj)
    return repr(obj)


def _format_ts(ts: datetime) -> str:
    if ts.tzinfo is None:
        return ts.isoformat() + "Z"
    return ts.isoformat()


__all__ = ["context_assembly"]
