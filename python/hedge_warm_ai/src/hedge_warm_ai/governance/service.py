"""Console-script entry point for the ``hedge-governance`` microservice.

The service composes:

* :class:`AiGovernanceEngine` for metric tracking + edge emission.
* A NATS publisher wrapping the canonical :data:`SUBJECT_AI_GOV_ACTION`.
* Subscribers for each governed component's emission topic
  (``ai.rank.<cid>``, ``ai.regime.changed``, ``ai.psych.stability``,
  ``ai.priority.changed.<sym>``, ``ai.news.impact.<sym>``,
  ``mem.prev_day.<sym>``, ``ai.journal.entry``) plus the closed-trade
  trigger (``exec.trade.closed``) and the per-symbol position update
  prefix (``pos.update.*``).
* A :class:`hedge_memory_rag.timescale.TimescaleWriter` for persistence
  of the ``governance_metrics`` hypertable.
* A :class:`RedisGovernanceWarmCache` writing to the interim
  ``hedge.warm.governance.<component>`` and
  ``hedge.warm.shadow.<component>`` namespaces until the Rust
  ``hedge-warmcache`` crate (task 44.x) lands.

Wiring those four pieces end-to-end depends on the FlatBuffers
decoder shipping in task 4.2; that integration lands in a follow-up.
Task 28.1 is limited to the engine itself, the metric estimators,
the ladder, the publisher, the WarmCache surface, and the
persistence sink.

This module exists so:

1. :file:`pyproject.toml`'s
   ``hedge-governance = "hedge_warm_ai.governance.service:main"``
   console script resolves at install time. Without it, the
   docker-compose service definition for ``hedge-governance`` fails
   on import.
2. Operators can sanity-check the engine is wired by running
   ``hedge-governance --check`` locally; the command constructs a
   :class:`GovernanceConfig` from defaults and prints the resolved
   thresholds + the canonical subjects + Redis namespaces.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .config import (
    DEFAULT_CRITICAL_THRESHOLD,
    DEFAULT_DEGRADATION_THRESHOLD,
    DEFAULT_DRIFT_REFERENCE_WINDOW,
    DEFAULT_DRIFT_WINDOW,
    DEFAULT_HALLUCINATION_WINDOW,
    DEFAULT_PREDICTION_WINDOW,
    DEFAULT_STABILITY_WINDOW,
    GovernanceConfig,
)
from .ladder import DEFAULT_WEIGHT_BY_LEVEL
from .state import DEFAULT_COMPONENTS, MetricKind
from .subjects import (
    DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE,
    DEFAULT_SHADOW_FLAG_NAMESPACE,
    SUBJECT_AI_GOV_ACTION,
    SUBJECT_EXEC_TRADE_CLOSED,
    pos_update_subject_pattern,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedge-governance",
        description=(
            "AI_Governance_Engine entry point. Concrete subscriber and "
            "publisher wiring is provided by a follow-up task; this "
            "binary currently exposes a --check mode for operators."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Print the resolved GovernanceConfig defaults and exit. "
            "Useful for verifying that the package imports cleanly "
            "inside the hedge-governance container."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``hedge-governance`` console script."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    config = GovernanceConfig()

    if args.check:
        snapshot = {
            "components": [c.value for c in config.components],
            "thresholds": {
                kind.value: {
                    "degradation": config.thresholds[kind].degradation,
                    "critical": config.thresholds[kind].critical,
                }
                for kind in MetricKind
            },
            "windows": {
                "drift": config.drift_window,
                "drift_reference": config.drift_reference_window,
                "stability": config.stability_window,
                "hallucination": config.hallucination_window,
                "prediction": config.prediction_window,
            },
            "weights": {
                level.value: weight
                for level, weight in config.weights.items()
            },
            "subjects": {
                "ai_gov_action": SUBJECT_AI_GOV_ACTION,
                "exec_trade_closed": SUBJECT_EXEC_TRADE_CLOSED,
                "pos_update_pattern": pos_update_subject_pattern(),
            },
            "redis_namespaces": {
                "governance_weight": DEFAULT_GOVERNANCE_WEIGHT_NAMESPACE,
                "shadow_flag": DEFAULT_SHADOW_FLAG_NAMESPACE,
            },
            "default_threshold_values": {
                "degradation": DEFAULT_DEGRADATION_THRESHOLD,
                "critical": DEFAULT_CRITICAL_THRESHOLD,
            },
            "default_window_sizes": {
                "drift": DEFAULT_DRIFT_WINDOW,
                "drift_reference": DEFAULT_DRIFT_REFERENCE_WINDOW,
                "stability": DEFAULT_STABILITY_WINDOW,
                "hallucination": DEFAULT_HALLUCINATION_WINDOW,
                "prediction": DEFAULT_PREDICTION_WINDOW,
            },
            "default_weights_by_level": {
                level.value: weight
                for level, weight in DEFAULT_WEIGHT_BY_LEVEL.items()
            },
            "components_iteration_order": [c.value for c in DEFAULT_COMPONENTS],
        }
        print(json.dumps(snapshot, indent=2, sort_keys=True))
        return 0

    print(
        "hedge-governance: subscriber/publisher wiring is provided by "
        "a follow-up task; run with --check to validate the loaded "
        "GovernanceConfig.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
