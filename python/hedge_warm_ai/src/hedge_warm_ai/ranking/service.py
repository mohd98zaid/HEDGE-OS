"""Console-script entry point for the ``hedge-rank`` microservice (task C.7).

The service:

1. Connects to NATS (``HEDGE_NATS_URL``).
2. Subscribes to ``sig.emitted`` (the cockpit-shaped JSON the Signal_Engine
   and demo-synth both publish).
3. For each signal, resolves the five ranking factors, computes the
   ``Trade_Confidence_Score`` via the engine's verbatim R17.1 formula, and
   publishes ``ai.rank.<correlation_id>`` in the flat cockpit shape the
   ui-gateway's ``/signals`` joiner expects.

Graceful degradation
---------------------

The production factor source is a Redis-backed
:class:`hedge_warm_ai.ranking.factors.RankingFactorProvider`. When Redis is
unavailable (the common dev case), the service falls back to a
:class:`hedge_warm_ai.ranking.factors.RankingFactorProvider` over a
best-effort cache, and ultimately to neutral
:class:`hedge_warm_ai.ranking.factors.FactorDefaults` (0.5 each). The
service therefore *always* produces a usable rank so the AI Confidence
Scores panel renders even with no warm backends running.

Subject / join contract
------------------------

The ui-gateway joins ``sig.emitted`` with ``ai.rank.<cid>`` on the raw
``correlation_id`` *string* (see ``crates/hedge-ui-gateway/src/
signals_join.rs``). The demo-synth uses string ids like ``synth-<hex>``.
We therefore publish on ``ai.rank.<correlation_id-as-is>`` and carry the
same string in the payload's ``correlation_id`` field — NOT the hex
re-encoding the engine's ``NatsRankPublisher`` would apply. This keeps the
real ranking output joinable with both the Hot_Path Signal_Engine and the
demo-synth.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Optional, Sequence

import structlog

from ..service_runtime import (
    DEFAULT_SYMBOL_BASKET,
    NatsService,
    configure_logging,
    tracked_symbols_from_env,
)
from .config import RankingConfig
from .factors import FactorDefaults, FactorProvider, RankingFactorProvider, StubFactorProvider
from .score import RankingFactors, compute_trade_confidence_score
from .state import SignalEvent, Side

_LOG = structlog.get_logger(__name__)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hedge-rank",
        description=(
            "AI_Trade_Ranking_Engine service. Subscribes sig.emitted and "
            "publishes ai.rank.<correlation_id>. Use --check to validate "
            "config without connecting to NATS."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Print the resolved RankingConfig defaults and exit.",
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=None,
        help="Optional path to a YAML RankingConfig file.",
    )
    return parser


async def _build_factor_provider() -> tuple[FactorProvider, str]:
    """Return a factor provider plus a one-word description of the mode.

    Tries Redis first (production path); on any failure returns a stub
    provider seeded with neutral defaults so ranking still produces a
    mid-band score (which the cockpit renders and the Risk_Engine treats
    as a usable, non-zero confidence).
    """
    import os

    redis_url = os.environ.get("HEDGE_REDIS_URL", "redis://127.0.0.1:6379")
    try:
        from hedge_memory_rag.redis_cache import RedisHotCache  # type: ignore

        # RedisHotCache construction + start varies by version; probe the
        # common shapes and fall back to the stub on any mismatch.
        cache = RedisHotCache(url=redis_url)  # type: ignore[call-arg]
        start = getattr(cache, "start", None)
        if start is not None:
            await start()
        provider = RankingFactorProvider(
            hot_cache=cache,
            defaults=FactorDefaults(),
            staleness_window_s=5.0,
        )
        # Smoke the connection so we fail fast into the stub if Redis is down.
        await provider._read_market_regime()  # noqa: SLF001 - intentional probe
        return provider, "redis"
    except Exception as exc:  # noqa: BLE001
        _LOG.info(
            "ranking_factor_provider_degraded",
            reason=str(exc),
            mode="neutral-defaults",
        )
        # Neutral 0.5 factors → mid-band score; keeps the panel alive.
        return StubFactorProvider(factors=RankingFactors(0.5, 0.5, 0.5, 0.5, 0.5)), "stub"


class ParsedSignal:
    """A decoded ``sig.emitted`` payload preserving the full cid string.

    The engine's :class:`SignalEvent` stores ``correlation_id`` as 16 raw
    bytes, which would truncate the demo-synth's ``synth-<hex>`` ids; we
    keep the original string alongside the typed event so the published
    ``ai.rank.<cid>`` subject matches the gateway join key exactly.
    """

    __slots__ = ("event", "cid", "strategy")

    def __init__(self, event: SignalEvent, cid: str, strategy: str) -> None:
        self.event = event
        self.cid = cid
        self.strategy = strategy


def _parse_signal(data: bytes) -> Optional[ParsedSignal]:
    """Decode a cockpit-shaped ``sig.emitted`` JSON payload."""
    try:
        v = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(v, dict):
        return None
    symbol = v.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        return None
    cid = v.get("correlation_id")
    if not isinstance(cid, str) or not cid:
        return None
    side_raw = str(v.get("side", "buy")).lower()
    side = Side.SELL if side_raw == "sell" else Side.BUY
    event = SignalEvent(
        signal_id=cid[:64],
        correlation_id=cid.encode("utf-8")[:16].ljust(16, b"\x00"),
        symbol=symbol,
        side=side,
        base_probability=float(v.get("base_probability", 0.0) or 0.0),
        confidence=float(v.get("confidence", 0.0) or 0.0),
        ts_ns=int(v.get("ts_ns", 0) or 0),
        shadow=bool(v.get("shadow", False)),
    )
    strategy = str(v.get("strategy", "")) if isinstance(v.get("strategy"), str) else ""
    return ParsedSignal(event=event, cid=cid, strategy=strategy)


async def _run() -> int:
    configure_logging()
    parser = _build_arg_parser()
    args = parser.parse_args()

    config = (
        RankingConfig.from_yaml_path(args.config) if args.config else RankingConfig()
    )
    if args.check:
        from .publisher import AI_RANK_PREFIX

        print(
            json.dumps(
                {
                    "rank_cache_namespace": config.rank_cache_namespace,
                    "ranking_timeout_ms": config.ranking_timeout_ms,
                    "ai_rank_subject_prefix": AI_RANK_PREFIX,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    symbols = tracked_symbols_from_env(DEFAULT_SYMBOL_BASKET)
    provider, mode = await _build_factor_provider()
    _LOG.info("hedge_rank_starting", factor_mode=mode, symbols=list(symbols))

    svc = await NatsService.connect("hedge-rank")

    async def on_signal(subject: str, data: bytes) -> None:
        parsed = _parse_signal(data)
        if parsed is None:
            return
        event = parsed.event
        factors = await provider.factors_for(event)
        score = compute_trade_confidence_score(factors)
        cid = parsed.cid
        payload = {
            "correlation_id": cid,
            "signal_id": event.signal_id,
            "symbol": event.symbol,
            "strategy": parsed.strategy,
            "side": "sell" if event.side == Side.SELL else "buy",
            "base_probability": event.base_probability,
            "confidence": event.confidence,
            "trade_confidence_score": score,
            "factors": {
                "orderflow": factors.orderflow,
                "technical_strength": factors.technical_strength,
                "news_sentiment": factors.news_sentiment,
                "market_regime": factors.market_regime,
                "trader_discipline": factors.trader_discipline,
            },
            "shadow": event.shadow,
            "ts_ns": event.ts_ns if event.ts_ns > 0 else time.time_ns(),
        }
        await svc.publish(
            f"ai.rank.{cid}",
            json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )

    await svc.subscribe("sig.emitted", on_signal)
    await svc.run_forever()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``hedge-rank`` console script."""
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
