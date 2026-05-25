"""Command-line conversion utility for the Warm_AI_Pipeline ONNX artefacts.

Task 20.1 ships its conversion logic as a CLI alongside the runtime so
that operators can build the artefacts on a developer workstation (or in
a build container) and copy the resulting tree onto the production
nodes. **The runtime itself never downloads weights from the internet.**

Usage::

    python -m hedge_warm_ai.onnx_runtime.cli convert-finbert \\
        --checkpoint /path/to/finbert \\
        --target /var/lib/hedge/models/onnx/finbert

    python -m hedge_warm_ai.onnx_runtime.cli convert-distilbert \\
        --checkpoint /path/to/distilbert \\
        --target /var/lib/hedge/models/onnx/distilbert

    python -m hedge_warm_ai.onnx_runtime.cli list-paths

The conversion subcommands for XGBoost, LightGBM, Isolation Forest, and
Tiny LSTM are exposed as Python entry points (see ``convert_*_to_onnx``
functions in :mod:`hedge_warm_ai.onnx_runtime.classical`) rather than
as standalone CLI commands because the in-memory estimator typically
lives inside a training script. The CLI only handles the on-disk
checkpoint conversions for FinBERT and DistilBERT.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from .nlp import convert_distilbert_to_onnx, convert_finbert_to_onnx
from .registry import resolve_layout


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedge-onnx",
        description=(
            "Convert XGBoost / LightGBM / Isolation Forest / Tiny LSTM / "
            "FinBERT / DistilBERT models to ONNX for the Warm_AI_Pipeline."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_finbert = sub.add_parser(
        "convert-finbert",
        help="Convert a local FinBERT checkpoint to ONNX.",
    )
    p_finbert.add_argument("--checkpoint", required=True, type=Path)
    p_finbert.add_argument("--target", type=Path, default=None)
    p_finbert.add_argument("--max-length", type=int, default=128)
    p_finbert.add_argument("--opset", type=int, default=18)

    p_distil = sub.add_parser(
        "convert-distilbert",
        help="Convert a local DistilBERT checkpoint to ONNX.",
    )
    p_distil.add_argument("--checkpoint", required=True, type=Path)
    p_distil.add_argument("--target", type=Path, default=None)
    p_distil.add_argument("--max-length", type=int, default=256)
    p_distil.add_argument("--opset", type=int, default=18)

    p_list = sub.add_parser(
        "list-paths",
        help="Print the resolved on-disk layout of the ONNX artefact tree.",
    )
    p_list.add_argument("--root", type=Path, default=None)

    return parser


def _resolve_target(target: Optional[Path], key: str, root: Optional[Path]) -> Path:
    if target is not None:
        return target.expanduser().resolve()
    layout = resolve_layout(override_root=root)
    return layout.model_path(key).parent


def _cmd_convert_finbert(args: argparse.Namespace) -> int:
    target = _resolve_target(args.target, "finbert", root=None)
    out = convert_finbert_to_onnx(
        checkpoint_dir=args.checkpoint,
        target_dir=target,
        target_opset=args.opset,
        max_length=args.max_length,
    )
    print(f"finbert -> {out}")
    return 0


def _cmd_convert_distilbert(args: argparse.Namespace) -> int:
    target = _resolve_target(args.target, "distilbert", root=None)
    out = convert_distilbert_to_onnx(
        checkpoint_dir=args.checkpoint,
        target_dir=target,
        target_opset=args.opset,
        max_length=args.max_length,
    )
    print(f"distilbert -> {out}")
    return 0


def _cmd_list_paths(args: argparse.Namespace) -> int:
    layout = resolve_layout(override_root=args.root)
    print(f"models root: {layout.root}")
    for key in sorted(layout.keys()):
        model_path = layout.model_path(key)
        tokenizer_path = layout.tokenizer_path(key)
        if tokenizer_path is not None:
            print(f"  {key:>17}: model={model_path}  tokenizer={tokenizer_path}")
        else:
            print(f"  {key:>17}: model={model_path}")
    return 0


_DISPATCH = {
    "convert-finbert": _cmd_convert_finbert,
    "convert-distilbert": _cmd_convert_distilbert,
    "list-paths": _cmd_list_paths,
}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _DISPATCH[args.command](args)


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())


__all__ = ["main"]
