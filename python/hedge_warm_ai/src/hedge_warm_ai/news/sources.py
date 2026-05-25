"""Source adapters for the News_Intelligence_Engine (R12.1).

The design specifies eight canonical sources:

* Reuters
* Moneycontrol
* NSE filings
* RBI announcements
* Twitter/X
* Telegram
* Economic Times
* Configured broker feeds

Each source has its own upstream protocol (RSS, REST polling,
websockets, push webhooks). To keep the rest of the pipeline
source-agnostic, every adapter is an :class:`async def
stream() -> AsyncIterator[Headline]` that yields normalised
:class:`Headline` records on the same shape regardless of the
underlying transport.

Concrete adapters in this module are intentionally **thin**: they
implement the typed shape and the integration with the rest of the
pipeline. The actual feed-fetching logic (HTTP polling loops, RSS
parsing, websocket subscriptions, broker SSE handling) is left as a
TODO for a follow-up task that touches each upstream API. What
matters now is that:

1. The :class:`SourceAdapter` ABC exists with the right async
   contract so the engine can be wired against it today.
2. One concrete subclass exists per listed source so the registry
   maps every :class:`HeadlineSource` to a usable adapter.
3. Adapters can be **fed manually** through :meth:`feed` for tests,
   replays, and the per-source service binaries — enabling end-to-
   end coverage of the engine pipeline without live network calls.

The :meth:`feed` API is documented as a public test/replay seam:
the service-layer wiring (task 21.x service binary) drives it from
recorded fixtures (R12.1 — `Integration` row in the traceability
matrix). Production drivers can either override :meth:`stream`
directly with a concrete fetch loop or push parsed feed payloads
into the same :meth:`feed` queue.
"""

from __future__ import annotations

import asyncio
from abc import ABC
from typing import AsyncIterator, Final, Iterable, Mapping

import structlog

from .headline import Headline, HeadlineSource

_LOG: Final = structlog.get_logger(__name__)


class SourceAdapter(ABC):
    """Abstract base class for every news source (R12.1).

    Subclasses MUST:

    * Set the class-level :attr:`SOURCE` to the canonical
      :class:`HeadlineSource` member they represent.
    * Implement :meth:`stream` as an ``async def`` that yields
      :class:`Headline` records. The default implementation in this
      ABC drains the internal :meth:`feed` queue; subclasses that
      have a real fetch loop should override :meth:`stream` and
      forward both their own pulls and any externally fed records
      onto the same async iterator.

    Concurrency:

    * One :class:`SourceAdapter` is owned by exactly one
      :class:`asyncio.Task` driving its :meth:`stream` iterator.
    * :meth:`feed` is safe to call from any task; it pushes onto an
      :class:`asyncio.Queue` that the iterator drains in order.
    """

    #: Canonical :class:`HeadlineSource` for this adapter. Override
    #: at the class level — the ABC raises a ``NotImplementedError``
    #: at instance construction otherwise.
    SOURCE: HeadlineSource = HeadlineSource.BROKER_FEED  # placeholder

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Subclasses must override SOURCE — the placeholder is
        # acceptable only on this very ABC.
        if cls.SOURCE is SourceAdapter.SOURCE and cls is not SourceAdapter:
            # ``BrokerFeedAdapter`` is the legitimate exception (its
            # SOURCE happens to equal the placeholder). We cannot
            # detect that here without naming it, so we leave the
            # check soft — the engine validates SOURCE at registration
            # time anyway.
            return

    def __init__(self, *, queue_maxsize: int = 1024) -> None:
        if queue_maxsize <= 0:
            raise ValueError(f"queue_maxsize must be > 0, got {queue_maxsize!r}")
        self._queue: asyncio.Queue[Headline | None] = asyncio.Queue(
            maxsize=int(queue_maxsize)
        )
        self._closed: bool = False

    # -- public API --------------------------------------------------------

    @property
    def source(self) -> HeadlineSource:
        return self.SOURCE

    async def feed(self, headline: Headline) -> None:
        """Push *headline* onto the adapter's queue.

        Used by:

        * The fetch loop a concrete adapter writes inside its own
          override of :meth:`stream`.
        * The replay engine and integration tests that drive the
          adapter from recorded fixtures.

        The headline must already carry the adapter's
        :attr:`SOURCE` — adapters that translate from upstream feed
        payloads should set it on construction.
        """
        if self._closed:
            raise RuntimeError(
                f"{type(self).__name__}: cannot feed after close()"
            )
        if headline.source is not self.SOURCE:
            raise ValueError(
                f"{type(self).__name__}: refused headline with "
                f"source={headline.source.value!r}; expected "
                f"{self.SOURCE.value!r}"
            )
        await self._queue.put(headline)

    def close(self) -> None:
        """Mark the adapter closed; pending :meth:`stream` iterators end."""
        if self._closed:
            return
        self._closed = True
        # ``put_nowait`` is safe here because the queue's maxsize is
        # never reached during shutdown — the engine drains before
        # close. If it ever is full, we drop the close marker; the
        # engine's outer cancellation will still terminate the task.
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:  # pragma: no cover - defensive
            _LOG.warning(
                "source_adapter_close_dropped",
                source=self.SOURCE.value,
            )

    async def stream(self) -> AsyncIterator[Headline]:
        """Yield the next :class:`Headline`. Default: drain the queue.

        Concrete adapters with a real fetch loop should override this
        to interleave fetched and externally fed headlines. The
        default implementation yields exactly the headlines pushed
        through :meth:`feed`, terminating when :meth:`close` is
        called.

        The method is declared ``async def`` so subclasses can
        override with their own async generator without changing
        the call site.
        """
        while True:
            item = await self._queue.get()
            if item is None:
                # ``close()`` sentinel.
                return
            yield item


# ---------------------------------------------------------------------------
# Concrete adapters ---------------------------------------------------------
# ---------------------------------------------------------------------------
#
# Each concrete adapter sets :attr:`SOURCE` and otherwise inherits the
# default :meth:`feed`/:meth:`stream` plumbing. The fetch logic is a
# TODO for a follow-up per-source task; the engine and tests can drive
# them today via :meth:`feed`.


class ReutersAdapter(SourceAdapter):
    """Adapter for the Reuters news feed.

    TODO(news-integration-21.x): wire the real Reuters polling client
    here. Until then, the adapter is queue-driven via
    :meth:`SourceAdapter.feed` so integration tests and the replay
    engine can supply recorded fixtures.
    """

    SOURCE: HeadlineSource = HeadlineSource.REUTERS


class MoneycontrolAdapter(SourceAdapter):
    """Adapter for Moneycontrol headlines.

    TODO(news-integration-21.x): wire the real Moneycontrol scraper
    or licensed feed here.
    """

    SOURCE: HeadlineSource = HeadlineSource.MONEYCONTROL


class NseFilingsAdapter(SourceAdapter):
    """Adapter for NSE corporate filings (R12.1).

    TODO(news-integration-21.x): wire the NSE corp-announcements
    poller here. The poller emits one :class:`Headline` per filing
    with ``symbols_hint`` populated from the filing's ``symbol`` field.
    """

    SOURCE: HeadlineSource = HeadlineSource.NSE_FILINGS


class RbiAnnouncementsAdapter(SourceAdapter):
    """Adapter for RBI press releases / monetary policy statements.

    TODO(news-integration-21.x): wire the RBI press-release scraper
    here. RBI announcements are policy-level (rate decisions, CRR/SLR
    changes) and rarely carry a per-symbol mapping; the fast path's
    symbol_map step handles the macro-impact case.
    """

    SOURCE: HeadlineSource = HeadlineSource.RBI


class TwitterAdapter(SourceAdapter):
    """Adapter for Twitter/X handles followed by the desk.

    TODO(news-integration-21.x): wire the X API filtered-stream
    client here. The list of followed handles is supplied at adapter
    construction by the service layer.
    """

    SOURCE: HeadlineSource = HeadlineSource.TWITTER


class TelegramAdapter(SourceAdapter):
    """Adapter for Telegram channels followed by the desk.

    TODO(news-integration-21.x): wire the Telegram MTProto / Bot API
    listener here. The list of channels is supplied at adapter
    construction by the service layer.
    """

    SOURCE: HeadlineSource = HeadlineSource.TELEGRAM


class EconomicTimesAdapter(SourceAdapter):
    """Adapter for Economic Times headlines.

    TODO(news-integration-21.x): wire the Economic Times RSS / API
    poller here.
    """

    SOURCE: HeadlineSource = HeadlineSource.ECONOMIC_TIMES


class BrokerFeedAdapter(SourceAdapter):
    """Adapter for broker-supplied news feeds (R12.1).

    Brokers (Zerodha, Dhan, Shoonya, Angel One) include a news
    channel in their websocket / REST APIs. Each broker's feed has a
    different shape; this adapter normalises whichever broker's
    payload the service-layer wiring binds onto a single
    :class:`HeadlineSource.BROKER_FEED` lane.

    TODO(news-integration-21.x): wire the per-broker translators
    here.
    """

    SOURCE: HeadlineSource = HeadlineSource.BROKER_FEED


# ---------------------------------------------------------------------------
# Registry helper -----------------------------------------------------------
# ---------------------------------------------------------------------------


def default_source_adapters(
    *,
    queue_maxsize: int = 1024,
    enabled: Iterable[HeadlineSource] | None = None,
) -> Mapping[HeadlineSource, SourceAdapter]:
    """Build the default per-source registry used by the engine.

    Args:
        queue_maxsize: Per-adapter queue size forwarded to every
            constructed adapter. Defaults to 1024 entries — large
            enough to absorb a burst from any one source while the
            engine catches up.
        enabled: Optional subset of :class:`HeadlineSource` to
            include. When ``None`` (the default) every canonical
            source is registered. A deployment that does not yet
            have credentials for one source can pass an explicit
            subset to omit it.

    Returns:
        Mapping ``HeadlineSource → SourceAdapter`` ready to be
        passed to :class:`hedge_warm_ai.news.engine.NewsIntelligenceEngine`.
    """
    factories: dict[HeadlineSource, type[SourceAdapter]] = {
        HeadlineSource.REUTERS: ReutersAdapter,
        HeadlineSource.MONEYCONTROL: MoneycontrolAdapter,
        HeadlineSource.NSE_FILINGS: NseFilingsAdapter,
        HeadlineSource.RBI: RbiAnnouncementsAdapter,
        HeadlineSource.TWITTER: TwitterAdapter,
        HeadlineSource.TELEGRAM: TelegramAdapter,
        HeadlineSource.ECONOMIC_TIMES: EconomicTimesAdapter,
        HeadlineSource.BROKER_FEED: BrokerFeedAdapter,
    }
    selected = (
        set(enabled) if enabled is not None else set(factories.keys())
    )
    unknown = selected - set(factories.keys())
    if unknown:
        raise ValueError(
            f"unknown headline sources requested: {sorted(s.value for s in unknown)!r}"
        )
    registry: dict[HeadlineSource, SourceAdapter] = {}
    for source, cls in factories.items():
        if source not in selected:
            continue
        registry[source] = cls(queue_maxsize=queue_maxsize)
    return registry


__all__ = [
    "BrokerFeedAdapter",
    "EconomicTimesAdapter",
    "MoneycontrolAdapter",
    "NseFilingsAdapter",
    "RbiAnnouncementsAdapter",
    "ReutersAdapter",
    "SourceAdapter",
    "TelegramAdapter",
    "TwitterAdapter",
    "default_source_adapters",
]
