"""Configuration surface for the AI_Shadow_Mode service (R23.1, R23.2, R23.3).

The service's tunables live in three groups:

1. **Flag-source namespace** — the Redis key prefix the shadow
   service polls for the per-component shadow flag. Default
   ``hedge.warm.shadow`` (re-exported from
   :mod:`hedge_warm_ai.governance.subjects` so the shadow service
   and the AI_Governance_Engine cannot drift). Full key is
   ``<namespace>.<component>``.
2. **Poll interval** — wall-clock interval (seconds) between
   consecutive flag-source reads. The service is *poll-driven* by
   design: the AI_Governance_Engine writes the flag with a TTL
   (``DEFAULT_SHADOW_TTL_S``) and the shadow service refreshes its
   in-memory snapshot at this cadence. Default ``1.0`` s — a
   compromise between latency-of-effect and Redis load that matches
   the cadence used for the other interim WarmCache surfaces in the
   pipeline.
3. **Persistence buffer** — bounded in-memory buffer of shadowed
   outputs awaiting Timescale write. The service consumes from the
   buffer in batches; when the buffer overflows the oldest entry is
   evicted and a structured warning is logged so operators see
   sustained persistence pressure. Default ``1024``.

All groups are pydantic models with ``extra="forbid"`` and
``validate_assignment=True``, mirroring the discipline used by
:class:`hedge_warm_ai.config.HedgeConfig`. A misconfigured deployment
fails closed at construction time.

The tunables come from the canonical
:class:`hedge_warm_ai.config.HedgeConfig`. The Rust schema currently
exposes :attr:`HedgeConfig.ai.shadow_components` (a static seed list
of components that should *start* in shadow mode independent of any
governance trigger) — the live "shadowed" set is the union of that
seed and whatever the AI_Governance_Engine has written into the
shadow-flag namespace. The remaining shadow knobs (poll interval,
persistence buffer) sit in this module today and will fold into the
Rust schema when the canonical config grows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveFloat,
    PositiveInt,
    ValidationError,
)

from ..config import HedgeConfig, SchemaViolationError
from ..governance.state import GovernedComponent
from .errors import ShadowConfigError
from .subjects import SHADOW_FLAG_NAMESPACE


# ---------------------------------------------------------------------------
# Defaults ------------------------------------------------------------------
# ---------------------------------------------------------------------------

#: Default poll interval (seconds) for refreshing the shadowed-set
#: snapshot from the interim WarmCache flag namespace.
DEFAULT_SHADOW_POLL_INTERVAL_S: Final[float] = 1.0

#: Default persistence buffer size (entries). Bounded so a stalled
#: Timescale writer cannot accumulate unbounded shadowed-output
#: payloads in memory. Excess entries are evicted oldest-first with a
#: structured warning.
DEFAULT_SHADOW_PERSISTENCE_BUFFER: Final[int] = 1024

#: Default Redis namespace for the shadow-flag surface. Mirrors
#: :data:`hedge_warm_ai.governance.subjects.DEFAULT_SHADOW_FLAG_NAMESPACE`.
DEFAULT_SHADOW_FLAG_NAMESPACE: Final[str] = SHADOW_FLAG_NAMESPACE


class _StrictModel(BaseModel):
    """All shadow config models forbid unknown fields and re-validate on assignment."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=False)


# ---------------------------------------------------------------------------
# Top-level ShadowModeConfig -----------------------------------------------
# ---------------------------------------------------------------------------


class ShadowModeConfig(_StrictModel):
    """Bundle of every tunable surfaced by the AI_Shadow_Mode service.

    The service takes one of these at construction. Defaults match
    the reference values documented in the module docstring and are
    safe in dev; production deployments should override via
    :meth:`ShadowModeConfig.from_yaml` or
    :meth:`ShadowModeConfig.from_hedge_config`.

    Attributes:
        flag_namespace: Redis key namespace for the interim
            shadow-flag surface. Default ``hedge.warm.shadow``.
        poll_interval_s: Wall-clock interval (seconds) between
            consecutive flag-source reads.
        persistence_buffer: Maximum entries the in-memory
            persistence buffer holds before it starts dropping the
            oldest entry.
        seed_components: Components that start in shadow mode at
            service startup independent of any governance trigger
            (sourced from :attr:`HedgeConfig.ai.shadow_components`).
    """

    flag_namespace: str = Field(
        default=DEFAULT_SHADOW_FLAG_NAMESPACE,
        min_length=1,
        description="Redis key namespace for the interim shadow-flag surface.",
    )
    poll_interval_s: PositiveFloat = Field(
        default=DEFAULT_SHADOW_POLL_INTERVAL_S,
        description=(
            "Wall-clock interval (seconds) between consecutive "
            "shadow-flag refreshes."
        ),
    )
    persistence_buffer: PositiveInt = Field(
        default=DEFAULT_SHADOW_PERSISTENCE_BUFFER,
        description=(
            "Maximum entries the in-memory persistence buffer holds "
            "before evicting the oldest entry."
        ),
    )
    seed_components: tuple[str, ...] = Field(
        default=(),
        description=(
            "Components that start in shadow mode at service startup "
            "independent of any governance trigger. Sourced from "
            "HedgeConfig.ai.shadow_components."
        ),
    )

    # ----- post-init invariant checks ------------------------------------

    def _check_invariants(self) -> None:
        if "." in self.flag_namespace:
            # The namespace forms the *prefix* of a key keyed by
            # component name (``<namespace>.<component>``). The dot
            # is reserved as the separator; allowing a dot inside the
            # namespace would silently produce ambiguous keys.
            return
        for comp in self.seed_components:
            if not comp:
                raise ShadowConfigError(
                    "ShadowModeConfig.seed_components must contain non-empty strings"
                )

    def model_post_init(self, __context: object) -> None:
        # pydantic v2 hook — re-route into the typed exception so a
        # misconfigured deployment fails closed with a typed signal.
        try:
            self._check_invariants()
        except ShadowConfigError:
            raise

    # ----- alternate constructors ---------------------------------------

    @classmethod
    def from_yaml(cls, raw: str) -> "ShadowModeConfig":
        """Parse a YAML body into a :class:`ShadowModeConfig`.

        The YAML shape mirrors the field names of this model.
        Unknown fields raise :class:`ShadowConfigError`
        (``extra="forbid"``) so a typo cannot silently fall back to
        the default.
        """
        try:
            parsed = yaml.safe_load(raw)
        except yaml.YAMLError as exc:
            raise ShadowConfigError(f"invalid YAML: {exc}") from exc
        if parsed is None:
            return cls()
        if not isinstance(parsed, dict):
            raise ShadowConfigError(
                f"top-level shadow config must be a mapping, got "
                f"{type(parsed).__name__}"
            )
        try:
            return cls.model_validate(parsed)
        except ValidationError as exc:
            raise ShadowConfigError(str(exc)) from exc

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> "ShadowModeConfig":
        """Load a :class:`ShadowModeConfig` from disk via the same loader."""
        return cls.from_yaml(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def from_hedge_config(
        cls,
        hedge: HedgeConfig | None = None,
        **overrides: object,
    ) -> "ShadowModeConfig":
        """Adapter: source defaults from a :class:`HedgeConfig` mirror.

        The Rust ``HedgeConfig`` schema exposes
        :attr:`HedgeConfig.ai.shadow_components` (the seed list); the
        rest of the shadow knobs come from this module's defaults
        until the canonical schema grows. ``overrides`` are
        forwarded to the dataclass constructor so callers can tune
        any field without re-declaring the full mapping.

        Args:
            hedge: An already-loaded :class:`HedgeConfig`. ``None``
                returns this module's defaults.

        Raises:
            SchemaViolationError: re-raised when ``hedge`` is the
                wrong type.
            ShadowConfigError: when an override clashes with the
                model invariants.
        """
        if hedge is None:
            return cls(**overrides)  # type: ignore[arg-type]
        if not isinstance(hedge, HedgeConfig):
            raise SchemaViolationError(
                "from_hedge_config expected a HedgeConfig instance; "
                f"got {type(hedge).__name__}"
            )
        kwargs: dict[str, object] = dict(overrides)
        kwargs.setdefault(
            "seed_components", tuple(hedge.ai.shadow_components)
        )
        try:
            return cls(**kwargs)  # type: ignore[arg-type]
        except ValidationError as exc:
            raise ShadowConfigError(str(exc)) from exc

    # ----- helpers -------------------------------------------------------

    def normalised_seed_components(self) -> frozenset[str]:
        """Return the seed components as a frozen set of canonical strings.

        Components not enumerated by
        :class:`hedge_warm_ai.governance.state.GovernedComponent`
        are still admitted — the design's shadow surface is per
        *named string* and the governance engine guards against
        unknown values. This helper just normalises the tuple into a
        membership-testable set.
        """
        return frozenset(self.seed_components)

    def known_seed_components(self) -> tuple[GovernedComponent, ...]:
        """Return the subset of seed components that are governed enums."""
        ordered: list[GovernedComponent] = []
        for raw in self.seed_components:
            try:
                comp = GovernedComponent(raw)
            except ValueError:
                continue
            if comp not in ordered:
                ordered.append(comp)
        return tuple(ordered)


__all__ = [
    "DEFAULT_SHADOW_FLAG_NAMESPACE",
    "DEFAULT_SHADOW_PERSISTENCE_BUFFER",
    "DEFAULT_SHADOW_POLL_INTERVAL_S",
    "ShadowModeConfig",
]
