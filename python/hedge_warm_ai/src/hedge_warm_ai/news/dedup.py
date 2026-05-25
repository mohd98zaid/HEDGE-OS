"""Content-hash-keyed deduplication for inbound headlines.

The design's pipeline (Components § News_Intelligence_Engine) places a
``Dedup`` step between the source adapters and the fast path:

::

    Source_Adapter → Dedup → Fast_Path → NewsImpact_v1

Different sources frequently report the same event within seconds of
each other (e.g. Reuters and Economic Times republishing the same RBI
statement). Without deduplication every echo would trigger a fresh
FinBERT call, a new Ollama dispatch, and a duplicate Qdrant write —
wasted compute and noisy emissions on ``ai.news.impact.<sym>``.

The filter is a bounded-LRU keyed by a content hash. The hash is the
SHA-1 of the lowercased + whitespace-collapsed headline text — stable
under casing differences and whitespace, sensitive to material edits.
"""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from threading import RLock
from typing import Final

from .config import DEFAULT_DEDUP_WINDOW
from .headline import Headline


def content_hash(text: str) -> str:
    """Return a stable 40-char hex SHA-1 of *text*.

    The text is normalised before hashing so superficial differences
    (extra whitespace, trailing newlines, shouting case) do not break
    deduplication:

    * Strip leading and trailing whitespace.
    * Collapse internal whitespace runs to a single space.
    * Lowercase the result.

    SHA-1 is intentional here: the hash is a deduplication key, not a
    cryptographic credential, and SHA-1's 160-bit output gives us
    plenty of headroom against accidental collisions on a working set
    measured in thousands of headlines.
    """
    if not text:
        raise ValueError("content_hash: text must be non-empty")
    normalised = " ".join(text.split()).lower()
    return hashlib.sha1(normalised.encode("utf-8")).hexdigest()


class Dedup:
    """Bounded LRU keyed by :func:`content_hash`.

    ``Dedup.observe(headline)`` returns ``True`` the first time a
    given content hash is seen and ``False`` for every subsequent
    observation while the entry is still inside the window. Once the
    LRU evicts the entry (because :attr:`window` newer entries have
    been observed since), the next observation of the same hash is
    treated as fresh again — a deliberate behaviour: a long-stale
    headline that resurfaces hours later is editorially significant
    and should be re-evaluated.

    The implementation uses :class:`collections.OrderedDict` for O(1)
    insert / move-to-end / popitem. An :class:`RLock` guards the
    structure so the engine can be driven by multiple coroutines on
    the same thread (asyncio reschedules the same OS thread but the
    test harness occasionally fans out across threads).

    Concurrency:

    * The lock is reentrant so :meth:`observe` and :meth:`__contains__`
      can be called from within each other on the same thread without
      deadlock.
    * The lock is **not** an :class:`asyncio.Lock` — the dedup
      operations are O(1) and finish well inside a single event-loop
      tick, so a synchronous lock is sufficient and avoids the
      overhead of an awaitable acquisition path.
    """

    __slots__ = ("_lock", "_seen", "_window")

    def __init__(self, *, window: int = DEFAULT_DEDUP_WINDOW) -> None:
        if window <= 0:
            raise ValueError(f"window must be > 0, got {window!r}")
        self._lock = RLock()
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._window = int(window)

    @property
    def window(self) -> int:
        """The configured LRU capacity (read-only)."""
        return self._window

    def __len__(self) -> int:
        with self._lock:
            return len(self._seen)

    def __contains__(self, headline_or_hash: Headline | str) -> bool:
        key = self._key(headline_or_hash)
        with self._lock:
            return key in self._seen

    def observe(self, headline: Headline) -> bool:
        """Return ``True`` if *headline* is fresh; ``False`` if duplicate.

        On a fresh observation the content hash is inserted at the
        most-recently-used end of the LRU. On a duplicate observation
        the existing entry is moved to the most-recently-used end
        (refreshing it) and ``False`` is returned. The "refresh on
        duplicate" semantics prevent a slow but persistent stream of
        duplicates from being evicted out from underneath itself.
        """
        key = self._key(headline)
        with self._lock:
            if key in self._seen:
                # Move to the MRU end so a sustained duplicate stream
                # does not get aged out and re-classified as fresh.
                self._seen.move_to_end(key, last=True)
                return False
            self._seen[key] = None
            # Evict from the LRU end while we are above capacity.
            while len(self._seen) > self._window:
                self._seen.popitem(last=False)
            return True

    def reset(self) -> None:
        """Clear every cached entry (test helper)."""
        with self._lock:
            self._seen.clear()

    @staticmethod
    def _key(headline_or_hash: Headline | str) -> str:
        if isinstance(headline_or_hash, Headline):
            return content_hash(headline_or_hash.text)
        if isinstance(headline_or_hash, str):
            return headline_or_hash
        raise TypeError(
            "Dedup keys must be Headline or str (content hash); "
            f"got {type(headline_or_hash).__name__}"
        )


__all__: Final = ["Dedup", "content_hash"]
