"""Configuration surface for the News_Intelligence_Engine (task 21.1).

The engine's tunables live in three groups:

1. **Dedup window** — the bounded LRU size for content-hash-keyed
   duplicate detection. The window must be large enough to absorb a
   burst of identical headlines from competing sources but small
   enough to keep the working set inside a single Warm_AI_Pipeline
   process.
2. **Fast-path budget** — the design's 10 ms p95 target for FinBERT
   scoring (R12.2). Surfaced here so deployments can tighten the
   budget for budget-breach alerting on slower hardware.
3. **Slow-path Ollama role** — which Ollama endpoint the slow path
   dispatches to (R12.3). Defaults to ``"deepseek"`` (the design's
   reasoning model). Overrideable via YAML for local testing.

All three groups are pydantic models with ``extra="forbid"`` and
``validate_assignment=True``. This mirrors the discipline used by
:class:`hedge_warm_ai.config.HedgeConfig` so a misconfigured
deployment fails closed at construction time.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final, Iterable

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    ValidationError,
)

from ..config import HedgeConfig, OllamaConfig, OllamaRole
from .errors import NewsConfigError


# ---------------------------------------------------------------------------
# Defaults ------------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default content-hash-keyed dedup window. 4096 headlines is enough
#: to cover several minutes of cross-source noise during a major news
#: burst (e.g. RBI policy day) while staying well under 1 MB of
#: process memory. Override per deployment via :class:`NewsConfig`.
DEFAULT_DEDUP_WINDOW: Final[int] = 4096

#: Default fast-path latency budget in milliseconds (R12.2).
#: Matches the design's 10 ms p95 target; alerts fire when the engine
#: trips the underlying :class:`hedge_warm_ai.onnx_runtime.LatencyTracer`
#: at this threshold.
DEFAULT_FAST_PATH_BUDGET_MS: Final[float] = 10.0

#: Default Ollama role the slow path dispatches to (R12.3). The
#: design uses ``"deepseek"`` (DeepSeek-R1) for reasoning-heavy
#: news interpretation. The role must exist in the active
#: :class:`hedge_warm_ai.ollama_client.OllamaClient` registry; the
#: :meth:`NewsConfig.with_role_check` validator confirms that.
DEFAULT_SLOW_PATH_ROLE: Final[str] = "deepseek"

#: Default Qdrant collection for headline embeddings.
#: Mirrors :data:`hedge_memory_rag.qdrant.collections.CollectionName.NEWS`
#: (R19.2). The string is intentionally duplicated here so the news
#: subpackage can be imported in environments that do not have
#: ``hedge_memory_rag`` installed (e.g. unit tests that do not exercise
#: the embedding sink).
DEFAULT_NEWS_QDRANT_COLLECTION: Final[str] = "news"


class _StrictModel(BaseModel):
    """All news config models forbid unknown fields and re-validate on assignment."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=False)


# ---------------------------------------------------------------------------
# Top-level NewsConfig ------------------------------------------------------
# ---------------------------------------------------------------------------


class NewsConfig(_StrictModel):
    """Bundle of every tunable surfaced by the News_Intelligence_Engine.

    The engine takes one of these at construction. Defaults match the
    reference values documented in the module docstring and are safe
    in dev; production deployments should override via
    :meth:`NewsConfig.from_yaml`.

    Attributes:
        dedup_window: Maximum number of recent content hashes the
            :class:`hedge_warm_ai.news.dedup.Dedup` filter keeps.
            Headlines beyond this window are no longer recognised as
            duplicates.
        fast_path_budget_ms: p95 latency budget for the fast path
            (R12.2). Exceeding this triggers
            ``obs.budget.breach.ai_finbert`` via the existing
            :class:`hedge_warm_ai.onnx_runtime.LatencyTracer`.
        slow_path_role: Ollama role key the slow path dispatches to.
            Must be present in the
            :class:`hedge_warm_ai.ollama_client.OllamaClient`
            registry; otherwise :meth:`with_role_check` raises a
            :class:`NewsConfigError`.
        slow_path_request_timeout_s: Per-call timeout override
            forwarded to
            :meth:`hedge_warm_ai.ollama_client.OllamaClient.stream_generate`.
            ``None`` means use the role's registered default
            (typically 60 s for ``deepseek``).
        slow_path_max_tokens: Cap on the number of tokens the slow
            path consumes from the streaming Ollama response. ``0``
            means no cap (drain the full reasoning).
        qdrant_collection: Name of the Qdrant collection where
            headline embeddings are persisted (R19.2). Defaults to
            :data:`DEFAULT_NEWS_QDRANT_COLLECTION` (``"news"``).
        symbols: Tuple of tracked symbol identifiers used by the
            fast-path :class:`hedge_warm_ai.news.fast_path.SymbolMap`
            step. Empty means the engine maps symbols only via the
            adapter-supplied :attr:`Headline.symbols_hint`.
        slow_path_enabled: Master switch for the slow path. When
            ``False`` the engine still emits the fast-path
            :class:`hedge_warm_ai.schemas.NewsImpact` payload but
            never schedules an Ollama call (useful in offline
            replay).
    """

    dedup_window: PositiveInt = Field(default=DEFAULT_DEDUP_WINDOW)
    fast_path_budget_ms: PositiveFloat = Field(default=DEFAULT_FAST_PATH_BUDGET_MS)
    slow_path_role: str = Field(default=DEFAULT_SLOW_PATH_ROLE, min_length=1)
    slow_path_request_timeout_s: NonNegativeFloat | None = Field(default=None)
    slow_path_max_tokens: int = Field(default=0, ge=0)
    qdrant_collection: str = Field(
        default=DEFAULT_NEWS_QDRANT_COLLECTION, min_length=1
    )
    symbols: tuple[str, ...] = Field(default_factory=tuple)
    slow_path_enabled: bool = Field(default=True)

    # ----- alternate constructors -----------------------------------------

    @classmethod
    def from_yaml(cls, raw: str) -> "NewsConfig":
        """Parse a YAML body into a :class:`NewsConfig`.

        The YAML shape mirrors the field names of this model. Unknown
        fields raise :class:`NewsConfigError` (extra=forbid) so a
        typo cannot silently fall back to the default.
        """
        try:
            parsed = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise NewsConfigError(f"invalid YAML: {exc}") from exc
        if parsed is None:
            return cls()
        if not isinstance(parsed, dict):
            raise NewsConfigError(
                f"top-level news config must be a mapping, got {type(parsed).__name__}"
            )
        try:
            return cls.model_validate(parsed)
        except ValidationError as exc:
            raise NewsConfigError(str(exc)) from exc

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> "NewsConfig":
        """Load a :class:`NewsConfig` from disk via the same loader."""
        return cls.from_yaml(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def from_hedge_config(cls, hedge: HedgeConfig | None = None) -> "NewsConfig":
        """Adaptor: source defaults from a :class:`HedgeConfig` mirror.

        The Rust ``HedgeConfig`` schema does not yet carry a ``news``
        block (the canonical schema lives in
        ``crates/hedge-config/schema.json`` and adding to it requires
        a separate task that touches the Rust side). Until that lands
        this adaptor reads the supplied :class:`HedgeConfig` only to
        validate the ``ollama`` registry contains a model in the
        configured slow-path role; everything else uses the defaults.
        """
        cfg = cls()
        if hedge is not None:
            cfg.with_role_check(hedge.ollama)
        return cfg

    # ----- validation helpers ---------------------------------------------

    def with_role_check(self, ollama: OllamaConfig) -> "NewsConfig":
        """Validate that :attr:`slow_path_role` matches a configured model.

        The check is two-step:

        1. The role must be one of the closed
           :class:`hedge_warm_ai.config.OllamaRole` enum values
           (``primary``, ``fast``, ``deep``, ``lightweight``) **or**
           a model-name literal that appears in the registry.
        2. At least one configured model must carry the matching
           :class:`OllamaRole`. The
           :class:`hedge_warm_ai.ollama_client.OllamaClient`
           default-endpoint registry uses keys
           ``"qwen" | "mistral" | "deepseek" | "phi"`` which are
           model-name aliases, **not** the
           :class:`hedge_warm_ai.config.OllamaRole` literals; the
           service-layer wiring for the news engine resolves the
           alias by matching either the role enum value or the
           model-name prefix.

        Args:
            ollama: The :class:`OllamaConfig` block from the active
                :class:`HedgeConfig`.

        Returns:
            The same :class:`NewsConfig` (for chaining).

        Raises:
            NewsConfigError: when no configured Ollama model can
                satisfy the requested slow-path role.
        """
        role = self.slow_path_role.lower()

        # Direct role-enum match (e.g. ``primary``, ``fast``, ``deep``).
        try:
            target_role = OllamaRole(role)
        except ValueError:
            target_role = None

        for entry in ollama.models:
            if target_role is not None and entry.role == target_role:
                return self
            # Allow client-alias keys (``qwen`` / ``mistral`` / etc.) to
            # match the configured model name's left segment.
            name_left = entry.name.split(":", 1)[0].lower()
            if name_left.startswith(role) or role.startswith(name_left):
                return self
        configured = ", ".join(
            f"{m.name} ({m.role.value})" for m in ollama.models
        )
        raise NewsConfigError(
            f"slow_path_role={self.slow_path_role!r} does not match any "
            f"configured Ollama model; registry contains: {configured}"
        )

    def with_symbols(self, symbols: Iterable[str]) -> "NewsConfig":
        """Return a copy of this config with :attr:`symbols` replaced.

        Used by the service layer to inject the trader-tracked symbol
        universe at startup. The returned config is a new instance so
        the original (defaulted) config is not mutated.
        """
        clean = tuple(s for s in symbols if s)
        return self.model_copy(update={"symbols": clean})


__all__ = [
    "DEFAULT_DEDUP_WINDOW",
    "DEFAULT_FAST_PATH_BUDGET_MS",
    "DEFAULT_NEWS_QDRANT_COLLECTION",
    "DEFAULT_SLOW_PATH_ROLE",
    "NewsConfig",
]
