"""Stage 4 of the retrieval pipeline — ollama_reasoning (R19.5).

Calls :meth:`hedge_warm_ai.ollama_client.OllamaClient.stream_generate`
against the configured role and concatenates the streamed tokens. The
trailing ``done=True`` chunk supplies the model identity (which may
differ from the requested role if the client routed to a fallback)
plus aggregated metrics.

The function intentionally does **not** introduce direct ``httpx``
calls; every LLM call goes through the existing Ollama client so the
fallback chain, degraded-event publishing, and retry policy stay in
exactly one place (R10.7, R10.8, R10.9).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from .config import RetrievalSettings
from .errors import OllamaReasoningFailedError
from .records import AssembledContext, StreamedReasoning

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hedge_warm_ai.ollama_client import OllamaClient

_LOG = structlog.get_logger(__name__)


async def ollama_reasoning(
    context: AssembledContext,
    *,
    ollama: "OllamaClient",
    settings: RetrievalSettings,
) -> StreamedReasoning:
    """Run Stage 4: stream reasoning tokens from Ollama.

    Args:
        context: Output of Stage 3 (deterministic prompt).
        ollama: Connected :class:`OllamaClient`. Required — there is no
            sensible "skip" path for the reasoning step.
        settings: Resolved settings (provides
            :attr:`RetrievalSettings.ollama_role` and the per-request
            timeout).

    Raises:
        OllamaReasoningFailedError: every model in the configured
            fallback chain has failed.

    Returns:
        :class:`StreamedReasoning` with the concatenated text plus the
        identity of the daemon that ultimately served the response.
    """
    # Heavy import is deferred so importing :mod:`hedge_memory_rag` for
    # only its data classes does not pull in :mod:`httpx`.
    from hedge_warm_ai.ollama_client import (  # noqa: PLC0415 - lazy import
        OllamaAllFallbacksExhaustedError,
    )

    correlation_id = context.correlation_id
    role = settings.ollama_role
    pieces: list[str] = []
    final_role: str = role
    final_model: str = ""
    final_metrics: dict[str, object] = {}

    try:
        async for chunk in ollama.stream_generate(
            role,
            prompt=context.prompt,
        ):
            if chunk.text:
                pieces.append(chunk.text)
            # The ``role``/``model`` reflect the responding daemon —
            # they may differ from the requested role if the client
            # rerouted to a fallback. Track the *latest* values so the
            # final identity always matches the daemon that produced
            # the trailing chunk.
            final_role = chunk.role
            final_model = chunk.model
            if chunk.done and chunk.metrics:
                final_metrics = dict(chunk.metrics)
    except OllamaAllFallbacksExhaustedError as exc:
        _LOG.error(
            "ollama_reasoning.exhausted",
            correlation_id=correlation_id,
            role=role,
            error=str(exc),
        )
        raise OllamaReasoningFailedError(
            f"every Ollama fallback failed for role={role!r}: {exc}",
            correlation_id=correlation_id,
            role=role,
        ) from exc

    text = "".join(pieces)
    return StreamedReasoning(
        context=context,
        role=final_role,
        model=final_model,
        text=text,
        metrics=final_metrics,
    )


__all__ = ["ollama_reasoning"]
