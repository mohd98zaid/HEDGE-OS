"""Console-script entry point for the ``hedge-regime`` microservice.

The actual service composes:

* :class:`MarketRegimeEngine` for classification + edge emission.
* A NATS publisher wrapping the canonical
  ``ai.regime.changed`` subject.
* A :class:`hedge_memory_rag.redis_cache.RedisHotCache` instance for
  the interim ``MarketStability`` write path (until the Rust
  WarmCache crate / task 44.x lands).
* An observation provider that subscribes to the relevant Hot_Path
  NATS subjects (``md.breadth.*``, ``feat.update.*``, etc.) and folds
  them into a :class:`RegimeObservation`.

Wiring those four pieces is intentionally **not** in scope for task
22.1 — the task explicitly limits this work to the engine itself, the
edge-triggered emission, and the WarmCache surface. Concrete
subscriber wiring depends on the Hot_Path NATS subject availability
and lands in a follow-up.

This module exists so:

1. :file:`pyproject.toml`'s ``hedge-regime = "hedge_warm_ai.regime.service:main"``
   console script resolves at install time. Without it, the
   docker-compose service definition for ``hedge-regime`` fails on
   import.
2. Operators can sanity-check the engine is wired by running
   ``hedge-regime --check`` locally; the command constructs a
   :class:`RegimeConfig` from defaults and prints the resolved
   thresholds and the canonical NATS subject.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .config import RegimeConfig


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedge-regime",
        description=(
            "Market_Regime_Engine entry point. Concrete subscriber and "
            "publisher wiring is provided by a follow-up task; this "
            "binary currently exposes a --check mode for operators."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Print the resolved RegimeConfig defaults and exit. Useful "
            "for verifying that the package imports cleanly inside the "
            "hedge-regime container."
        ),
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=None,
        help=(
            "Optional path to a YAML file mirroring the RegimeConfig "
            "model. When omitted, the engine starts with the documented "
            "defaults."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``hedge-regime`` console script."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.config:
        config = RegimeConfig.from_yaml_path(args.config)
    else:
        config = RegimeConfig()

    if args.check:
        snapshot = {
            "evaluation_interval_s": config.evaluation_interval_s,
            "nats_subject": config.nats_subject,
            "seed_regime": config.seed_regime,
            "thresholds": config.thresholds.model_dump(),
            "stability_factors": config.stability_factors.factors,
            "use_onnx_classifier": config.use_onnx_classifier,
            "publish_warmup_skip": config.publish_warmup_skip,
        }
        print(json.dumps(snapshot, indent=2, sort_keys=True))
        return 0

    print(
        "hedge-regime: subscriber/publisher wiring is provided by a "
        "follow-up task; run with --check to validate the loaded "
        "RegimeConfig.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
