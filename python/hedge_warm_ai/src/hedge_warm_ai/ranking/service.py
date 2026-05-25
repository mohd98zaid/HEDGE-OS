"""Console-script entry point for the ``hedge-rank`` microservice.

The actual service composes:

* :class:`AiTradeRankingEngine` for score computation + emission.
* A NATS subscriber on ``sig.emitted`` that decodes each
  :class:`hedge_v1.Signal_v1` FlatBuffers payload into a
  :class:`SignalEvent` and feeds it to the engine.
* A :class:`NatsRankPublisher` for ``ai.rank.<correlation_id>``.
* A :class:`hedge_memory_rag.redis_cache.RedisHotCache` instance that
  backs both the :class:`RankingFactorProvider` (factor reads) and
  the :class:`RedisAiRankCache` (interim WarmCache write path until
  the Rust WarmCache crate / task 44.x lands).

Wiring those four pieces end-to-end depends on the Hot_Path
``sig.emitted`` NATS topic availability and the FlatBuffers decoder
shipping in task 4.2; that integration lands in a follow-up. Task
26.1 is limited to the engine itself, the score, the publisher, the
factor-provider seam, and the interim WarmCache surface.

This module exists so:

1. :file:`pyproject.toml`'s ``hedge-rank = "hedge_warm_ai.ranking.service:main"``
   console script resolves at install time. Without it, the
   docker-compose service definition for ``hedge-rank`` fails on
   import.
2. Operators can sanity-check the engine is wired by running
   ``hedge-rank --check`` locally; the command constructs a
   :class:`RankingConfig` from defaults and prints the resolved
   thresholds and the canonical ``ai.rank`` subject prefix.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .config import RankingConfig
from .publisher import AI_RANK_PREFIX
from .score import (
    MARKET_REGIME_WEIGHT,
    NEWS_SENTIMENT_WEIGHT,
    ORDERFLOW_WEIGHT,
    TECHNICAL_STRENGTH_WEIGHT,
    TRADER_DISCIPLINE_WEIGHT,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedge-rank",
        description=(
            "AI_Trade_Ranking_Engine entry point. Concrete subscriber and "
            "publisher wiring is provided by a follow-up task; this "
            "binary currently exposes a --check mode for operators."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Print the resolved RankingConfig defaults and exit. Useful "
            "for verifying that the package imports cleanly inside the "
            "hedge-rank container."
        ),
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=None,
        help=(
            "Optional path to a YAML file mirroring the RankingConfig "
            "model. When omitted, the engine starts with the documented "
            "defaults."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``hedge-rank`` console script."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.config:
        config = RankingConfig.from_yaml_path(args.config)
    else:
        config = RankingConfig()

    if args.check:
        snapshot = {
            "rank_cache_namespace": config.rank_cache_namespace,
            "rank_cache_ttl_s": config.rank_cache_ttl_s,
            "factor_staleness_window_s": config.factor_staleness_window_s,
            "ranking_timeout_ms": config.ranking_timeout_ms,
            "ai_rank_subject_prefix": AI_RANK_PREFIX,
            "weights": {
                "orderflow": ORDERFLOW_WEIGHT,
                "technical_strength": TECHNICAL_STRENGTH_WEIGHT,
                "news_sentiment": NEWS_SENTIMENT_WEIGHT,
                "market_regime": MARKET_REGIME_WEIGHT,
                "trader_discipline": TRADER_DISCIPLINE_WEIGHT,
            },
        }
        print(json.dumps(snapshot, indent=2, sort_keys=True))
        return 0

    print(
        "hedge-rank: subscriber/publisher wiring is provided by a "
        "follow-up task; run with --check to validate the loaded "
        "RankingConfig.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
