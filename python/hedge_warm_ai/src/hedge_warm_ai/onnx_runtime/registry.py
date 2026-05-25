"""Resolution of ONNX model artefact paths.

Task 20.1 forbids downloading weights at runtime. Every wrapper takes a
file path, but the *path itself* must come from the same configuration
surface the rest of the pipeline uses (``hedge-config`` in Rust,
:mod:`hedge_warm_ai.config` in Python).

The Rust ``HedgeConfig`` does not yet carry a model-artefact path table —
that is acceptable per the design's "Components § News_Intelligence_
Engine (fast path)" section, which lets each Warm_AI_Pipeline service
resolve its own paths under the conventional ``models/onnx/<name>``
layout. To keep the Python and Rust loaders in lockstep, this module
implements an additive resolver:

1. ``HEDGE_ONNX_MODELS_DIR`` env var overrides everything.
2. Otherwise, fall back to ``$HEDGE_HOME/models/onnx`` if set, else
   ``/var/lib/hedge/models/onnx`` (Linux deploy path).
3. The conversion CLI under :mod:`hedge_warm_ai.onnx_runtime.cli` writes
   into the resolved directory; the runtime reads from the same place.

This keeps task 20.1 honest:

* No internet access at runtime — the resolver only inspects the
  filesystem and the supplied :class:`HedgeConfig`.
* Paths originate from the existing config loader (``HedgeConfig`` is
  accepted as an argument; its ``observability`` /
  ``ai`` surface can be extended in a follow-up without breaking the
  callers below).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Iterable, Mapping, Optional

from hedge_warm_ai.config import HedgeConfig

# Default deploy locations. Linux package convention: artefacts under
# /var/lib for state, /etc for config. We pick /var/lib so the tree is
# writable for the post-conversion job. The CLI creates this directory
# on first run.
_DEFAULT_LINUX_DIR: Final[Path] = Path("/var/lib/hedge/models/onnx")
_HEDGE_HOME_ENV: Final[str] = "HEDGE_HOME"
_OVERRIDE_ENV: Final[str] = "HEDGE_ONNX_MODELS_DIR"

# Conventional artefact filenames inside the resolved root directory.
# The conversion CLI uses these too — keep the two in sync.
_ARTEFACT_FILENAMES: Final[Mapping[str, str]] = {
    "xgboost": "xgboost.onnx",
    "lightgbm": "lightgbm.onnx",
    "isolation_forest": "isolation_forest.onnx",
    "tiny_lstm": "tiny_lstm.onnx",
    "finbert": "finbert/model.onnx",
    "distilbert": "distilbert/model.onnx",
}

# Tokenizer subdirectories (NLP only). Returns ``None`` for non-NLP keys.
_TOKENIZER_DIRS: Final[Mapping[str, str]] = {
    "finbert": "finbert/tokenizer",
    "distilbert": "distilbert/tokenizer",
}


@dataclass(frozen=True, slots=True)
class ModelArtefactLayout:
    """Resolved on-disk layout for the Warm_AI_Pipeline ONNX artefacts."""

    root: Path

    def model_path(self, key: str) -> Path:
        """Return the file path for *key* (e.g. ``xgboost``, ``finbert``)."""
        try:
            rel = _ARTEFACT_FILENAMES[key]
        except KeyError as exc:
            raise KeyError(
                f"unknown ONNX artefact key {key!r}; "
                f"expected one of {sorted(_ARTEFACT_FILENAMES)}"
            ) from exc
        return self.root / rel

    def tokenizer_path(self, key: str) -> Optional[Path]:
        """Return the tokenizer directory for an NLP model, else ``None``."""
        rel = _TOKENIZER_DIRS.get(key)
        return self.root / rel if rel else None

    def keys(self) -> Iterable[str]:
        """All artefact keys this layout can resolve."""
        return _ARTEFACT_FILENAMES.keys()


def resolve_layout(
    config: Optional[HedgeConfig] = None,
    *,
    override_root: Optional[Path] = None,
) -> ModelArtefactLayout:
    """Return the on-disk :class:`ModelArtefactLayout` for the running pipeline.

    Args:
        config: The active :class:`HedgeConfig`. Reserved for future
            extension; the current loader does not yet carry an explicit
            ``models_dir`` field, so we honour environment overrides
            first. Passing the config keeps call sites future-proof.
        override_root: Explicit root override, used by the CLI and tests.

    Resolution order:

    1. *override_root* if given.
    2. ``HEDGE_ONNX_MODELS_DIR`` env var.
    3. ``$HEDGE_HOME/models/onnx`` if ``HEDGE_HOME`` is set.
    4. ``/var/lib/hedge/models/onnx`` (Linux deploy default).
    """
    _ = config  # placeholder for future extension; documented above
    if override_root is not None:
        return ModelArtefactLayout(root=Path(override_root).expanduser().resolve())

    env_override = os.environ.get(_OVERRIDE_ENV)
    if env_override:
        return ModelArtefactLayout(root=Path(env_override).expanduser().resolve())

    home = os.environ.get(_HEDGE_HOME_ENV)
    if home:
        return ModelArtefactLayout(root=(Path(home) / "models" / "onnx").expanduser().resolve())

    return ModelArtefactLayout(root=_DEFAULT_LINUX_DIR)


__all__ = [
    "ModelArtefactLayout",
    "resolve_layout",
]
