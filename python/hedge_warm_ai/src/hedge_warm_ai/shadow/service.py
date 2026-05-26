"""Console-script entry point for the ``hedge-shadow`` microservice.

The service composes:

* :class:`ShadowModeService` for shadow-flag polling +
  persistence + governance forwarding.
* A :class:`RedisShadowFlagSource` reading from the
  AI_Governance_Engine's ``hedge.warm.shadow.<component>`` interim
  WarmCache namespace.
* A :class:`TimescaleShadowedOutputSink` wrapping
  :class:`hedge_memory_rag.timescale.TimescaleWriter` for the
  matching hypertables.
* A :class:`AiGovernanceEngineObserver` forwarding shadowed outputs
  into :meth:`AiGovernanceEngine.observe` so the engine's accuracy
  metrics include shadowed emissions (R23.3).

Wiring those four pieces end-to-end depends on the FlatBuffers
decoder + NATS subscriber stack shipping in tasks 4.2 / 36.1; that
integration lands in a follow-up. Task 29.1 is limited to the
service itself, the persistence sinks, the UI filter, and the
governance observer adapter.

This module exists so:

1. :file:`pyproject.toml`'s
   ``hedge-shadow = "hedge_warm_ai.shadow.service:main"`` console
   script resolves at install time. Without it, the docker-compose
   service definition for ``hedge-shadow`` fails on import.
2. Operators can sanity-check the service is wired by running
   ``hedge-shadow --check`` locally; the command constructs a
   :class:`ShadowModeConfig` from defaults and prints the resolved
   tunables, the canonical Redis namespace, and the canonical NATS
   subjects that the UI gateway filter applies to.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from ..governance.state import DEFAULT_COMPONENTS
from .config import (
    DEFAULT_SHADOW_FLAG_NAMESPACE,
    DEFAULT_SHADOW_PERSISTENCE_BUFFER,
    DEFAULT_SHADOW_POLL_INTERVAL_S,
    ShadowModeConfig,
)
from .subjects import (
    SUBJECT_AI_JOURNAL_ENTRY,
    SUBJECT_AI_NEWS_IMPACT_PREFIX,
    SUBJECT_AI_PRIORITY_CHANGED_PREFIX,
    SUBJECT_AI_PSYCH_STABILITY,
    SUBJECT_AI_RANK_PREFIX,
    SUBJECT_AI_REGIME_CHANGED,
    SUBJECT_MEM_PREV_DAY_PREFIX,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedge-shadow",
        description=(
            "AI_Shadow_Mode service entry point. Concrete subscriber and "
            "persistence wiring is provided by a follow-up task; this "
            "binary currently exposes a --check mode for operators."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Print the resolved ShadowModeConfig defaults and exit. "
            "Useful for verifying that the package imports cleanly "
            "inside the hedge-shadow container."
        ),
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=None,
        help=(
            "Optional path to a YAML file mirroring the ShadowModeConfig "
            "model. When omitted, the service starts with the documented "
            "defaults."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``hedge-shadow`` console script."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.config:
        config = ShadowModeConfig.from_yaml_path(args.config)
    else:
        config = ShadowModeConfig()

    if args.check:
        snapshot = {
            "config": {
                "flag_namespace": config.flag_namespace,
                "poll_interval_s": config.poll_interval_s,
                "persistence_buffer": config.persistence_buffer,
                "seed_components": list(config.seed_components),
            },
            "default_flag_namespace": DEFAULT_SHADOW_FLAG_NAMESPACE,
            "default_poll_interval_s": DEFAULT_SHADOW_POLL_INTERVAL_S,
            "default_persistence_buffer": DEFAULT_SHADOW_PERSISTENCE_BUFFER,
            "polled_components": [c.value for c in DEFAULT_COMPONENTS],
            "ui_filtered_subjects": [
                f"{SUBJECT_AI_RANK_PREFIX}.<correlation_id>",
                SUBJECT_AI_REGIME_CHANGED,
                SUBJECT_AI_PSYCH_STABILITY,
                f"{SUBJECT_AI_PRIORITY_CHANGED_PREFIX}.<symbol>",
                f"{SUBJECT_AI_NEWS_IMPACT_PREFIX}.<symbol>",
                SUBJECT_AI_JOURNAL_ENTRY,
                f"{SUBJECT_MEM_PREV_DAY_PREFIX}.<symbol>",
            ],
        }
        print(json.dumps(snapshot, indent=2, sort_keys=True))
        return 0

    print(
        "hedge-shadow: subscriber/persistence wiring is provided by a "
        "follow-up task; run with --check to validate the loaded "
        "ShadowModeConfig.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
