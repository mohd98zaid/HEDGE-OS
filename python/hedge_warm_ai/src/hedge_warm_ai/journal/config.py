"""Configuration surface for the AI_Trade_Journal_Engine (task 27.1).

The engine reads everything from the live :class:`HedgeConfig` so
nothing is hardcoded — the spec brief explicitly requires this. Only
two knobs concern the journal directly:

* The narrative Ollama role (Qwen2.5:14B per design § Components §
  AI_Trade_Journal_Engine).
* The deeper post-mortem Ollama role (DeepSeek-R1 per design).

The narrative role is *always* invoked. The post-mortem role is
optional — it kicks in for trades that meet a configured *trigger*
(e.g. losing trades, large drawdowns) so the engine doesn't burn deep
reasoning compute on every closed trade. The default trigger is
"any trade with negative P&L"; see :class:`JournalConfig` below.

The embedding dimensionality is read from
:class:`hedge_memory_rag.qdrant.QdrantSettings` so the value matches the
``journal_entries`` collection's vector spec — no second source of
truth for the dim.

References:
- Spec brief — task 27.1: "Configuration (Ollama roles for narrative +
  post-mortem, embedding dimensionality, persistence sinks) MUST come
  from the existing config loader; nothing hardcoded."
- Design § Components § AI_Trade_Journal_Engine.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from ..config import HedgeConfig, OllamaRole

# ---------------------------------------------------------------------------
# Default routing-key mapping ----------------------------------------------
# ---------------------------------------------------------------------------

#: Mapping of :class:`OllamaRole` (logical role from
#: :mod:`hedge_warm_ai.config`) to :data:`OllamaRoleKey` (the routing
#: key consumed by :class:`hedge_warm_ai.ollama_client.OllamaClient`).
#:
#: The four registered keys mirror :func:`default_endpoints` and the
#: container names in ``docker-compose.ollama.yml``. Both sides are
#: deliberately decoupled so a deployment can multiplex multiple
#: endpoints under one logical role; here we use the canonical 1:1
#: mapping. The engine looks up the role at construction time and
#: caches the routing key — config reload requires re-construction.
_DEFAULT_ROLE_KEY_BY_ROLE: Final[dict[OllamaRole, str]] = {
    OllamaRole.PRIMARY: "qwen",
    OllamaRole.FAST: "mistral",
    OllamaRole.DEEP: "deepseek",
    OllamaRole.LIGHTWEIGHT: "phi",
}


@dataclass(frozen=True, slots=True)
class JournalConfig:
    """Resolved per-engine configuration extracted from :class:`HedgeConfig`.

    Attributes:
        narrative_role_key: Routing key for the standard narrative
            generation step (Qwen2.5:14B per design). Read from the
            config loader's :class:`OllamaConfig` model registry.
        postmortem_role_key: Routing key for the optional deeper
            post-mortem step (DeepSeek-R1 per design). ``None`` when
            no DeepSeek-R1 endpoint is registered in
            :class:`OllamaConfig`; the engine then skips the
            post-mortem step entirely.
        narrative_max_tokens: Soft cap on narrative tokens. The
            actual narrative length is bounded by the
            ``ai_journal_entry`` schema's ``narrative.maxLength``
            (8192 chars); this knob exists for callers that want a
            shorter narrative for UI summaries.
        narrative_request_timeout_s: Override for the per-request
            wall-clock budget on the narrative call. ``None`` falls
            through to the endpoint's configured ``timeout_s``.
        postmortem_request_timeout_s: Same as above but for the
            post-mortem call. ``None`` falls through.
        postmortem_on_loss_only: When ``True`` (default), the
            post-mortem hop runs only on trades with ``pnl_inr < 0``.
            Setting to ``False`` runs it on every closed trade
            regardless of outcome.
        embedding_dim: Vector dimensionality used by the
            ``journal_entries`` Qdrant collection. Sourced from the
            :class:`QdrantSettings` configured at engine construction
            (see :class:`AiTradeJournalEngine`); cached here so
            callers don't have to thread the QdrantSettings through.
    """

    narrative_role_key: str
    postmortem_role_key: str | None
    narrative_max_tokens: int
    narrative_request_timeout_s: float | None
    postmortem_request_timeout_s: float | None
    postmortem_on_loss_only: bool
    embedding_dim: int

    def __post_init__(self) -> None:
        if not self.narrative_role_key:
            raise ValueError("narrative_role_key must be non-empty")
        if self.narrative_max_tokens <= 0:
            raise ValueError(
                f"narrative_max_tokens must be > 0, got {self.narrative_max_tokens!r}"
            )
        if self.narrative_request_timeout_s is not None and self.narrative_request_timeout_s <= 0:
            raise ValueError(
                "narrative_request_timeout_s must be > 0 or None, "
                f"got {self.narrative_request_timeout_s!r}"
            )
        if self.postmortem_request_timeout_s is not None and self.postmortem_request_timeout_s <= 0:
            raise ValueError(
                "postmortem_request_timeout_s must be > 0 or None, "
                f"got {self.postmortem_request_timeout_s!r}"
            )
        if self.embedding_dim <= 0:
            raise ValueError(f"embedding_dim must be > 0, got {self.embedding_dim!r}")


# ---------------------------------------------------------------------------
# Loader --------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _resolve_role_key(
    *,
    cfg: HedgeConfig,
    role: OllamaRole,
    role_key_by_role: dict[OllamaRole, str],
) -> str | None:
    """Return the routing key for ``role`` if any model in the registry has it.

    The registry in :class:`OllamaConfig` may legitimately omit a role
    if the deployment doesn't run that container — the deeper
    post-mortem can be disabled by simply not registering DeepSeek-R1.
    The engine treats a missing role as "skip the post-mortem step",
    not as a hard error.
    """
    for model in cfg.ollama.models:
        if model.role == role:
            return role_key_by_role.get(role)
    return None


def load_journal_config(
    cfg: HedgeConfig,
    *,
    embedding_dim: int,
    role_key_by_role: dict[OllamaRole, str] | None = None,
    narrative_max_tokens: int = 1024,
    narrative_request_timeout_s: float | None = None,
    postmortem_request_timeout_s: float | None = None,
    postmortem_on_loss_only: bool = True,
) -> JournalConfig:
    """Build a :class:`JournalConfig` from a live :class:`HedgeConfig`.

    Args:
        cfg: Loaded :class:`HedgeConfig`. The engine reads the
            ``ollama.models`` registry to discover which routing keys
            exist; nothing is hardcoded.
        embedding_dim: Vector dimensionality used by the
            ``journal_entries`` Qdrant collection. Pass
            ``QdrantSettings.vector_dim_for(CollectionName.JOURNAL_ENTRIES)``
            so the engine and the collection stay in lock-step.
        role_key_by_role: Optional override for the
            :class:`OllamaRole` → routing-key map. Defaults to
            :data:`_DEFAULT_ROLE_KEY_BY_ROLE` which mirrors
            :func:`hedge_warm_ai.ollama_client.endpoint.default_endpoints`.
        narrative_max_tokens: See :class:`JournalConfig`.
        narrative_request_timeout_s: See :class:`JournalConfig`.
        postmortem_request_timeout_s: See :class:`JournalConfig`.
        postmortem_on_loss_only: See :class:`JournalConfig`.

    Returns:
        Resolved :class:`JournalConfig`.

    Raises:
        ValueError: ``cfg`` does not register an Ollama model with the
            ``primary`` role (Qwen2.5:14B). The engine cannot operate
            without a narrative model, so this is a fail-fast at
            engine startup.
    """
    table = role_key_by_role or _DEFAULT_ROLE_KEY_BY_ROLE
    narrative = _resolve_role_key(cfg=cfg, role=OllamaRole.PRIMARY, role_key_by_role=table)
    if narrative is None:
        raise ValueError(
            "AI_Trade_Journal_Engine requires an Ollama model with role=primary "
            "(Qwen2.5:14B per design § Components § AI_Trade_Journal_Engine); "
            "none registered in HedgeConfig.ollama.models"
        )
    postmortem = _resolve_role_key(cfg=cfg, role=OllamaRole.DEEP, role_key_by_role=table)
    return JournalConfig(
        narrative_role_key=narrative,
        postmortem_role_key=postmortem,
        narrative_max_tokens=int(narrative_max_tokens),
        narrative_request_timeout_s=narrative_request_timeout_s,
        postmortem_request_timeout_s=postmortem_request_timeout_s,
        postmortem_on_loss_only=bool(postmortem_on_loss_only),
        embedding_dim=int(embedding_dim),
    )


__all__ = [
    "JournalConfig",
    "load_journal_config",
]
