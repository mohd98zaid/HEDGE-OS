"""Narrative + post-mortem assembly for the AI_Trade_Journal_Engine.

Two thin builders split the LLM responsibilities documented in design
§ Components § AI_Trade_Journal_Engine:

* :class:`NarrativeBuilder` — runs the standard narrative pass over
  Qwen2.5:14B (``OllamaRole.PRIMARY``). Always invoked.
* :class:`PostMortemBuilder` — runs the deeper reasoning pass over
  DeepSeek-R1 (``OllamaRole.DEEP``). Optional and gated on the
  outcome (default: only when ``pnl_inr < 0``).

Both builders consume the canonical :class:`TradeClosedEvent` and
return a string (the assembled narrative or the assembled
post-mortem). The composed narrative emitted on
``ai.journal.entry`` is::

    "<narrative>\n\nPost-mortem:\n<postmortem>"

(when the post-mortem is enabled and produced output) or just::

    "<narrative>"

(when the post-mortem is skipped or produced no usable text).

The builders are deliberately decoupled from the engine so the engine
test suite can stub them in-memory and the LLM contract is testable
on its own.

Streaming is dispatched through
:meth:`hedge_warm_ai.ollama_client.OllamaClient.stream_generate` —
tokens are accumulated into a string in memory rather than streamed
to a downstream consumer, because the journal is a one-shot fire-and-
forget artefact.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, Mapping, Protocol

import structlog

from ..ollama_client import OllamaClient
from .state import TradeClosedEvent

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Prompt assembly -----------------------------------------------------------
# ---------------------------------------------------------------------------


_NARRATIVE_SYSTEM_PROMPT: Final[str] = (
    "You are PROJECT HEDGE's post-trade analyst. Given the closed-trade "
    "context below, produce a concise, factual narrative covering: outcome "
    "(P&L, side, quantity, entry/exit), the contributing strategy and "
    "signal, the trader's emotional state at entry and exit, the prevailing "
    "market regime, identified missed opportunities, and execution quality. "
    "Avoid speculation. Avoid markdown headers. Limit to 5–8 short "
    "paragraphs."
)

_POSTMORTEM_SYSTEM_PROMPT: Final[str] = (
    "You are PROJECT HEDGE's deep post-trade analyst. The trade closed at a "
    "loss or otherwise warrants a deeper review. Critically examine: was "
    "the strategy mis-applied to the regime, did the trader's emotional "
    "state lead to a sub-optimal entry or exit, were the missed "
    "opportunities avoidable, and was execution quality a contributing "
    "factor. Conclude with one or two concrete behavioural adjustments "
    "for similar future setups. Avoid speculation. Limit to 4–6 short "
    "paragraphs."
)


def _format_emotional(label: str, snap) -> str:  # type: ignore[no-untyped-def]
    """Format an :class:`EmotionalSnapshot` as a single line."""
    return (
        f"{label} stability_score={snap.score:.3f} "
        f"discipline={snap.discipline:.3f} "
        f"emotional_control={snap.emotional_control:.3f} "
        f"risk_consistency={snap.risk_consistency:.3f} "
        f"patience={snap.patience:.3f}"
    )


def _format_execution_quality(event: TradeClosedEvent) -> str:
    eq = event.execution_quality
    parts: list[str] = []
    if eq.slippage_bps is not None:
        parts.append(f"slippage_bps={eq.slippage_bps:.2f}")
    if eq.latency_ms is not None:
        parts.append(f"latency_ms={eq.latency_ms:.2f}")
    if eq.fill_attempts is not None:
        parts.append(f"fill_attempts={eq.fill_attempts}")
    if eq.commission_paise is not None:
        parts.append(f"commission_paise={eq.commission_paise}")
    return ", ".join(parts) if parts else "(unavailable)"


def _format_iso_utc(ts_ns: int) -> str:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc).isoformat(
        timespec="seconds"
    )


def build_trade_context(event: TradeClosedEvent) -> str:
    """Render the trade context block consumed by both LLM passes.

    The block is a stable, machine-friendly enumeration of every
    field the design requires the journal to cover. Emitting a
    deterministic block keeps the LLM output reproducible across
    similar trades and makes the CBOR-embedded narrative
    round-trippable in the property test (task 27.2).
    """
    lines: list[str] = [
        "Trade Context:",
        f"- correlation_id: {event.correlation_id}",
        f"- trade_id: {event.trade_id}",
        f"- symbol: {event.symbol}",
        f"- side: {event.side}",
        f"- quantity: {event.quantity}",
        f"- entry_paise: {event.entry_paise}",
        f"- exit_paise: {event.exit_paise}",
        f"- pnl_inr: {event.pnl_inr:.2f}",
        f"- opened_at_utc: {_format_iso_utc(event.opened_ts_ns)}",
        f"- closed_at_utc: {_format_iso_utc(event.closed_ts_ns)}",
        f"- strategy_id: {event.strategy_id}",
        f"- signal_id: {event.signal_id}",
        f"- regime: {event.regime}",
        f"- {_format_emotional('emotional_at_entry', event.emotional_at_entry)}",
        f"- {_format_emotional('emotional_at_exit', event.emotional_at_exit)}",
        f"- execution_quality: {_format_execution_quality(event)}",
    ]
    if event.missed_opportunities:
        lines.append("- missed_opportunities:")
        for note in event.missed_opportunities:
            lines.append(f"  * {note}")
    else:
        lines.append("- missed_opportunities: (none reported)")
    return "\n".join(lines)


def build_narrative_prompt(event: TradeClosedEvent) -> str:
    """Compose the full narrative prompt sent to the primary model."""
    return f"{_NARRATIVE_SYSTEM_PROMPT}\n\n{build_trade_context(event)}\n\nNarrative:"


def build_postmortem_prompt(event: TradeClosedEvent, narrative: str) -> str:
    """Compose the full post-mortem prompt sent to the deep model.

    The narrative produced by the first pass is fed as additional
    context so the post-mortem can build on it without repeating the
    enumeration.
    """
    return (
        f"{_POSTMORTEM_SYSTEM_PROMPT}\n\n{build_trade_context(event)}\n\n"
        f"Initial narrative (Qwen2.5:14B):\n{narrative}\n\nPost-mortem:"
    )


# ---------------------------------------------------------------------------
# Builders ------------------------------------------------------------------
# ---------------------------------------------------------------------------


class NarrativeProvider(Protocol):
    """Async callable producing the narrative + optional post-mortem.

    Decoupled so unit tests can stub the LLM without touching
    httpx/Ollama. The production binding is :class:`OllamaNarrativeBuilder`.
    """

    async def build(self, event: TradeClosedEvent) -> str: ...


@dataclass
class OllamaNarrativeBuilder:
    """Production binding driving the two Ollama passes.

    Construction:
        client: Shared :class:`OllamaClient`. Must be ``start()``-ed
            before the engine processes its first event.
        narrative_role: Routing key for the standard narrative pass.
        postmortem_role: Routing key for the deeper post-mortem pass.
            ``None`` disables the post-mortem hop.
        narrative_max_tokens: Soft cap forwarded to Ollama via
            ``options.num_predict`` so the narrative stays bounded.
        narrative_request_timeout_s: Per-request override; ``None``
            falls through to the endpoint's configured ``timeout_s``.
        postmortem_request_timeout_s: Per-request override for the
            deep pass; same fall-through semantics.
        postmortem_on_loss_only: When ``True`` (the default), the
            deep pass is skipped for break-even or winning trades.
        narrative_options: Extra ``options`` map merged into both
            passes (e.g. temperature, top_p). Reserved keys
            (``model``, ``prompt``, ``stream``) are forbidden by the
            client and will raise ``ValueError``.
    """

    client: OllamaClient
    narrative_role: str
    postmortem_role: str | None = None
    narrative_max_tokens: int = 1024
    narrative_request_timeout_s: float | None = None
    postmortem_request_timeout_s: float | None = None
    postmortem_on_loss_only: bool = True
    narrative_options: Mapping[str, object] | None = None

    async def build(self, event: TradeClosedEvent) -> str:
        narrative = await self._stream(
            role=self.narrative_role,
            prompt=build_narrative_prompt(event),
            request_timeout_s=self.narrative_request_timeout_s,
        )
        narrative = narrative.strip()
        if not narrative:
            # The schema requires ``narrative`` ≥ 1 char; fall back to
            # a deterministic stub so persistence still succeeds even
            # when the LLM emits an empty body.
            narrative = self._fallback_narrative(event)

        if self._should_run_postmortem(event):
            try:
                postmortem = await self._stream(
                    role=self.postmortem_role,  # type: ignore[arg-type]
                    prompt=build_postmortem_prompt(event, narrative),
                    request_timeout_s=self.postmortem_request_timeout_s,
                )
            except Exception as exc:  # pragma: no cover - logged + dropped
                # A failed deep pass must not abort the journal entry.
                # The narrative alone still satisfies R18.1.
                _LOG.warning(
                    "journal_postmortem_failed",
                    correlation_id=event.correlation_id,
                    trade_id=event.trade_id,
                    role=self.postmortem_role,
                    error=str(exc),
                )
                postmortem = ""
            postmortem = postmortem.strip()
            if postmortem:
                return f"{narrative}\n\nPost-mortem:\n{postmortem}"
        return narrative

    # ----- helpers ---------------------------------------------------------

    def _should_run_postmortem(self, event: TradeClosedEvent) -> bool:
        if self.postmortem_role is None:
            return False
        if not self.postmortem_on_loss_only:
            return True
        return event.pnl_inr < 0.0

    async def _stream(
        self,
        *,
        role: str,
        prompt: str,
        request_timeout_s: float | None,
    ) -> str:
        options: dict[str, object] = {
            "num_predict": int(self.narrative_max_tokens),
        }
        if self.narrative_options:
            for k, v in self.narrative_options.items():
                # Caller-provided options take precedence over the default
                # token budget so callers can override per-pass.
                options[k] = v
        chunks: list[str] = []
        async for chunk in self.client.stream_generate(
            role,
            prompt=prompt,
            options=options,
            request_timeout_s=request_timeout_s,
        ):
            if chunk.text:
                chunks.append(chunk.text)
        return "".join(chunks)

    @staticmethod
    def _fallback_narrative(event: TradeClosedEvent) -> str:
        """Deterministic, schema-valid narrative when the LLM returns nothing."""
        return (
            f"Trade {event.trade_id} ({event.symbol}, {event.side}, "
            f"qty={event.quantity}) closed with pnl_inr={event.pnl_inr:.2f}. "
            "Narrative generation produced no output; fields persisted as-is."
        )


__all__ = [
    "NarrativeProvider",
    "OllamaNarrativeBuilder",
    "build_narrative_prompt",
    "build_postmortem_prompt",
    "build_trade_context",
]
