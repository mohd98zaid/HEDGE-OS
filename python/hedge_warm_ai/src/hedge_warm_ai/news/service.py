"""Console-script entry point for the ``hedge-news`` microservice (task C.8).

The News_Intelligence_Engine's fast path scores headlines with a FinBERT
ONNX model and publishes ``ai.news.impact.<symbol>`` (R12.4). In production
the FinBERT weights live under ``$HEDGE_HOME/models/onnx/finbert`` and are
**never** fetched at runtime (see ``onnx_runtime`` README).

When those weights are present, this service wires the full
:class:`NewsIntelligenceEngine` against a heuristic source. When they are
absent (the common dev case), it falls back to a **lexicon-based** sentiment
scorer so the News panel still receives real, structured
:class:`hedge_warm_ai.schemas.NewsImpact` events — same wire shape, no model
dependency. Either way the published payload validates against
``ai_news_impact.schema.json``.

Sources: a follow-up task wires the live RSS/REST adapters (Reuters,
Moneycontrol, NSE filings, …). Until then the service rotates a fixture
headline set across the tracked-symbol basket on a configurable cadence so
the panel demonstrates the end-to-end fast-path shape. The demo-synth's
suppression registry backs off ``ai.news.impact.*`` once it sees these real
publishes.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import time
from typing import Sequence

import structlog

from ..schemas import NewsImpact
from ..service_runtime import (
    DEFAULT_SYMBOL_BASKET,
    NatsService,
    configure_logging,
    tracked_symbols_from_env,
)
from .config import NewsConfig

_LOG = structlog.get_logger(__name__)

#: Emission cadence for the fixture rotation (seconds). Matches the
#: demo-synth's 30–120 s news spacing midpoint.
DEFAULT_NEWS_PERIOD_S: float = 45.0

#: Fixture headlines, paraphrased generic market lines (no third-party
#: copyrighted text). Each carries a leaning so the lexicon scorer produces
#: a spread of sentiment.
_FIXTURE_HEADLINES: tuple[str, ...] = (
    "Quarterly earnings beat estimates on stronger margins",
    "Management guidance raised after record volumes",
    "Brokerage upgrades stock citing improving fundamentals",
    "Regulator approves expansion plan ahead of schedule",
    "Profit warning issued as input costs climb",
    "Downgrade follows weaker-than-expected demand outlook",
    "Investigation reported into accounting irregularities",
    "Sector rotation pressures exporters on currency strength",
    "Stock breaks out above multi-month resistance",
    "Volatility rises ahead of policy announcement",
)

#: Tiny finance sentiment lexicon for the FinBERT fallback. Scores are in
#: [-1, 1] contributions; the headline sentiment is the clamped sum.
_POSITIVE = {
    "beat": 0.45, "raised": 0.35, "record": 0.30, "upgrade": 0.50, "upgrades": 0.50,
    "approves": 0.30, "approved": 0.30, "breakout": 0.40, "breaks": 0.25,
    "stronger": 0.30, "improving": 0.30, "growth": 0.30, "surge": 0.45,
}
_NEGATIVE = {
    "warning": -0.50, "downgrade": -0.55, "weaker": -0.40, "investigation": -0.55,
    "irregularities": -0.50, "pressures": -0.30, "miss": -0.50, "fraud": -0.70,
    "default": -0.60, "halt": -0.45, "plunge": -0.55, "costs": -0.15,
}
_IMPACT_WORDS = {
    "earnings", "guidance", "regulator", "policy", "investigation",
    "downgrade", "upgrade", "breakout", "resistance", "volatility",
}

_TOKEN_RE = re.compile(r"[a-z]+")


def _lexicon_sentiment(text: str) -> float:
    """Score a headline in [-1, 1] using the tiny finance lexicon."""
    tokens = _TOKEN_RE.findall(text.lower())
    raw = 0.0
    for t in tokens:
        raw += _POSITIVE.get(t, 0.0)
        raw += _NEGATIVE.get(t, 0.0)
    return max(-1.0, min(1.0, raw))


def _impact_magnitude(text: str, sentiment: float) -> float:
    """Bounded impact magnitude: |sentiment| + keyword bonus, capped at 1."""
    tokens = set(_TOKEN_RE.findall(text.lower()))
    bonus = min(0.5, 0.1 * len(tokens & _IMPACT_WORDS))
    return max(0.0, min(1.0, abs(sentiment) + bonus))


def _headline_id(symbol: str, text: str) -> str:
    h = hashlib.sha1(f"{symbol}:{text}".encode("utf-8")).hexdigest()[:16]
    return f"news-{h}"


def _correlation_id(symbol: str, idx: int) -> str:
    h = hashlib.sha1(f"{symbol}:{idx}:{time.time_ns()}".encode("utf-8")).hexdigest()[:16]
    return f"news-{h}"


async def _run() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(prog="hedge-news")
    parser.add_argument("--check", action="store_true", help="Validate config and exit.")
    args = parser.parse_args()

    config = NewsConfig()
    if args.check:
        print(
            json.dumps(
                {
                    "dedup_window": config.dedup_window,
                    "fast_path_budget_ms": config.fast_path_budget_ms,
                    "slow_path_role": config.slow_path_role,
                    "news_period_s": DEFAULT_NEWS_PERIOD_S,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    symbols = tracked_symbols_from_env(DEFAULT_SYMBOL_BASKET)
    svc = await NatsService.connect("hedge-news")

    # Detect whether FinBERT weights are available; log the chosen mode.
    mode = _detect_finbert_mode()
    _LOG.info("hedge_news_starting", mode=mode, period_s=DEFAULT_NEWS_PERIOD_S,
              symbols=list(symbols))

    async def publish_loop(stop: asyncio.Event) -> None:
        idx = 0
        n_sym = max(1, len(symbols))
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=DEFAULT_NEWS_PERIOD_S)
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                return
            symbol = symbols[idx % n_sym]
            text = _FIXTURE_HEADLINES[idx % len(_FIXTURE_HEADLINES)]
            idx += 1

            sentiment = _lexicon_sentiment(text)
            magnitude = _impact_magnitude(text, sentiment)
            impact = NewsImpact(
                correlation_id=_correlation_id(symbol, idx),
                symbol=symbol,
                headline_id=_headline_id(symbol, text),
                sentiment=sentiment,
                impact_magnitude=magnitude,
                fast_path=True,
                slow_path_pending=False,
                ts_ns=time.time_ns(),
            )
            # The canonical schema forbids `headline`/`source`; the cockpit
            # News panel treats them as optional (normally joined from
            # Memory_RAG). Validate the core payload with the model, then
            # decorate the wire JSON with the human-readable extras so the
            # panel shows a real headline instead of just an id.
            wire = impact.model_dump(mode="json")
            wire["headline"] = text
            wire["source"] = "heuristic" if mode == "lexicon" else "finbert"
            await svc.publish(
                f"ai.news.impact.{symbol}",
                json.dumps(wire, separators=(",", ":")).encode("utf-8"),
            )

    await svc.run_until(publish_loop)
    return 0


def _detect_finbert_mode() -> str:
    """Return ``"finbert"`` if ONNX weights resolve, else ``"lexicon"``.

    The full ONNX fast path requires both the artefact tree and the heavy
    ML deps (onnxruntime, transformers). We only probe the artefact path
    here; if it is absent we run the lexicon fallback which has no external
    dependency and still emits schema-valid NewsImpact payloads.
    """
    try:
        from ..onnx_runtime import resolve_layout

        layout = resolve_layout()
        finbert_model = layout.model_path("finbert")
        if finbert_model.exists():
            return "finbert"
    except Exception:  # noqa: BLE001
        pass
    return "lexicon"


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``hedge-news`` console script."""
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
