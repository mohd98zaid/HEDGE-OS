"""Async streaming client for the Ollama_Infrastructure microservices.

This module is the single Warm_AI_Pipeline entry point for LLM
inference. It implements the contract demanded by task 19.1:

* Async streaming inference over Ollama's ``/api/generate`` HTTP API
  (R10.7). Tokens are yielded as they arrive — the client never
  buffers the full response in memory.
* Configurable per-model timeout (R10.9). Each
  :class:`~hedge_warm_ai.ollama_client.endpoint.OllamaModelEndpoint`
  carries its own wall-clock budget.
* Fallback routing on unresponsive service (R10.9). When a model
  times out, refuses the connection, or returns a 5xx status code,
  the client emits an ``ai.ollama.degraded`` event on NATS and
  re-issues the request against the configured fallback model.
* No outbound calls to public LLM providers (R10.8). The client
  honours the ``base_url`` it is configured with; egress to public
  providers is blocked at the host-level firewall (see
  ``infra/firewall/``).

Design notes
------------
* The client uses :mod:`httpx` (already in ``pyproject.toml``)
  configured with HTTP/1.1 (Ollama does not support HTTP/2 streaming
  reliably as of 0.3.x). One :class:`httpx.AsyncClient` is reused
  across requests; the daemon keeps the model warm in GPU memory
  thanks to ``OLLAMA_KEEP_ALIVE=30m`` set in the compose file.
* The fallback chain is followed eagerly on every retryable failure,
  but only **once per request** — a request that has already been
  re-routed once does not trigger a second fallback hop until it
  fails again. The chain is acyclic by construction (the default
  chain in :func:`default_fallback_chain` terminates at ``phi``).
* Cycle protection: if the supplied chain contains a cycle, the
  client treats the cycle as a terminal node and raises
  :class:`OllamaAllFallbacksExhaustedError` rather than looping
  forever.
* Exactly-once degraded-event emission: one ``ai.ollama.degraded``
  event per (originally-requested role, fallback role) pair per
  request — so a single stuck primary cannot fan out a storm of
  degraded events.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from types import TracebackType
from typing import Any, AsyncIterator, Final, Mapping, Optional, Type

import httpx
import structlog

from ..schemas import OllamaDegraded
from .endpoint import (
    OllamaModelEndpoint,
    OllamaRoleKey,
    default_endpoints,
    default_fallback_chain,
)
from .errors import (
    OllamaAllFallbacksExhaustedError,
    OllamaClientError,
    OllamaConnectError,
    OllamaHttpError,
    OllamaTimeoutError,
)
from .publisher import DegradedEventPublisher, NoopDegradedPublisher

_LOG: Final = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Public API ----------------------------------------------------------------
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OllamaResponseChunk:
    """One chunk of a streamed response.

    Mirrors the shape of an Ollama ``/api/generate`` NDJSON line so
    callers can inspect the trailing ``done=True`` chunk for the
    aggregated metrics (eval count, prompt eval duration, etc.).

    Attributes:
        role: Routing-key the chunk was served from. Equal to the
            originally-requested role unless the request was rerouted
            to a fallback — in which case it is the fallback role.
        model: GGUF tag of the responding daemon.
        text: Token text. Empty string on the trailing ``done`` chunk.
        done: ``True`` on the final chunk; metrics are populated.
        metrics: Optional aggregated metrics from the trailing chunk
            (``total_duration``, ``eval_count``, ``eval_duration``,
            etc.). ``None`` on every non-final chunk.
    """

    role: OllamaRoleKey
    model: str
    text: str
    done: bool
    metrics: Optional[Mapping[str, Any]] = None


class OllamaClient:
    """Async streaming client for the four Ollama daemons.

    Lifecycle:

        async with OllamaClient(endpoints=...) as client:
            async for chunk in client.stream_generate("qwen", prompt="..."):
                print(chunk.text, end="")

    Or manage the underlying HTTP client manually::

        client = OllamaClient(...)
        await client.start()
        try:
            ...
        finally:
            await client.aclose()

    The class is **async-safe** (multiple coroutines can call
    :meth:`stream_generate` concurrently against different roles) but
    not **thread-safe** — share one instance per :class:`asyncio.Task`
    tree, not across threads.
    """

    def __init__(
        self,
        endpoints: Mapping[OllamaRoleKey, OllamaModelEndpoint] | None = None,
        *,
        fallback_chain: Mapping[OllamaRoleKey, OllamaRoleKey] | None = None,
        publisher: DegradedEventPublisher | None = None,
        clock_ns: "Optional[Any]" = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """Construct the client.

        Args:
            endpoints: Role → endpoint registry. Defaults to
                :func:`default_endpoints` (the four containers in
                ``docker-compose.ollama.yml``).
            fallback_chain: Role → fallback-role mapping. A request to
                ``role`` is rerouted to ``fallback_chain[role]`` on
                unresponsive failure. Roles missing from the chain are
                terminal (no fallback). Defaults to
                :func:`default_fallback_chain`.
            publisher: Sink for the ``ai.ollama.degraded`` event.
                Defaults to :class:`NoopDegradedPublisher`. In
                production wire :class:`NatsDegradedPublisher`.
            clock_ns: Callable returning a monotonic ns timestamp.
                Defaults to :func:`time.monotonic_ns`. Override in
                tests for determinism.
            http_client: Pre-built :class:`httpx.AsyncClient`. If
                ``None`` (the common case), the client builds its own
                with HTTP/1.1, no proxy, and connection limits sized
                to the four daemons.
        """
        self._endpoints: dict[OllamaRoleKey, OllamaModelEndpoint] = dict(
            endpoints if endpoints is not None else default_endpoints()
        )
        if not self._endpoints:
            raise ValueError("endpoints must contain at least one entry")
        self._fallback_chain: dict[OllamaRoleKey, OllamaRoleKey] = dict(
            fallback_chain if fallback_chain is not None else default_fallback_chain()
        )
        self._validate_chain()

        self._publisher: DegradedEventPublisher = publisher or NoopDegradedPublisher()
        self._clock_ns = clock_ns or time.monotonic_ns
        self._wall_clock_ns = time.time_ns

        self._owns_client: bool = http_client is None
        self._http: Optional[httpx.AsyncClient] = http_client
        self._closed: bool = False

    # ----- context manager + lifecycle -------------------------------------

    async def __aenter__(self) -> "OllamaClient":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def start(self) -> None:
        """Create the underlying :class:`httpx.AsyncClient` if owned."""
        if self._http is None:
            limits = httpx.Limits(
                max_connections=max(8, len(self._endpoints) * 2),
                max_keepalive_connections=max(4, len(self._endpoints)),
            )
            # Per-request timeouts are applied at the call site via
            # `httpx.Timeout`, so we set generous client-level
            # defaults and override them per-request.
            self._http = httpx.AsyncClient(
                http2=False,
                follow_redirects=False,
                limits=limits,
                trust_env=False,  # ignore HTTPS_PROXY etc. — egress firewall handles policy
            )
            self._owns_client = True

    async def aclose(self) -> None:
        """Close the underlying HTTP client (if owned)."""
        if self._closed:
            return
        self._closed = True
        if self._http is not None and self._owns_client:
            await self._http.aclose()
            self._http = None

    # ----- public inference API --------------------------------------------

    async def stream_generate(
        self,
        role: OllamaRoleKey,
        *,
        prompt: str,
        options: Mapping[str, Any] | None = None,
        request_timeout_s: float | None = None,
    ) -> AsyncIterator[OllamaResponseChunk]:
        """Stream tokens from the Ollama daemon registered under ``role``.

        Yields one :class:`OllamaResponseChunk` per NDJSON line. The
        final chunk has ``done=True`` and carries the daemon's
        aggregated metrics.

        On unresponsive failure (timeout, connection refused, 5xx),
        emits one ``ai.ollama.degraded`` event and retries against the
        configured fallback role. The chunks yielded after rerouting
        carry the fallback ``role`` and ``model`` — callers that care
        about provenance must inspect the chunk fields.

        Args:
            role: Initial routing key.
            prompt: Prompt sent in the ``prompt`` field. The full
                prompt is one HTTP request body — Ollama does not
                support prompt streaming as of 0.3.x.
            options: Extra request fields (``options``, ``system``,
                ``template``, ``context``, ``raw``, ``format``,
                ``keep_alive``). Forwarded verbatim to the daemon.
                Reserved keys (``model``, ``prompt``, ``stream``)
                MUST NOT appear in ``options``; they are set by the
                client.
            request_timeout_s: Per-call override for the endpoint's
                ``timeout_s``. Useful when one News slow-path call
                wants to override the role's default budget without
                mutating the registry.

        Yields:
            :class:`OllamaResponseChunk` instances.

        Raises:
            KeyError: ``role`` is not registered.
            ValueError: ``options`` contains a reserved key.
            OllamaAllFallbacksExhaustedError: every model in the
                fallback chain has failed.
            OllamaHttpError: the daemon returned a 4xx (caller bug).
                4xx is **not** rerouted — looping would not help.
        """
        if self._http is None:
            await self.start()
        if role not in self._endpoints:
            raise KeyError(f"unknown Ollama role: {role!r}")
        if options is not None:
            for reserved in ("model", "prompt", "stream"):
                if reserved in options:
                    raise ValueError(
                        f"options must not contain reserved key {reserved!r}"
                    )

        # Walk the fallback chain.
        failures: list[tuple[OllamaRoleKey, OllamaClientError]] = []
        visited: set[OllamaRoleKey] = set()
        current_role: OllamaRoleKey = role
        original_role: OllamaRoleKey = role

        while True:
            if current_role in visited:
                # Cycle in the supplied chain — treat as terminal.
                _LOG.warning(
                    "ollama_fallback_cycle_detected",
                    role=current_role,
                    visited=sorted(visited),
                )
                raise OllamaAllFallbacksExhaustedError(
                    f"fallback cycle detected at role={current_role!r}",
                    role=original_role,
                    model=self._endpoints[original_role].model,
                    failures=failures,
                )
            visited.add(current_role)

            endpoint = self._endpoints[current_role]
            timeout_s = (
                request_timeout_s
                if request_timeout_s is not None
                else endpoint.timeout_s
            )

            try:
                async for chunk in self._stream_one(
                    role=current_role,
                    endpoint=endpoint,
                    prompt=prompt,
                    options=options,
                    timeout_s=timeout_s,
                ):
                    yield chunk
                return
            except OllamaHttpError as exc:
                if not exc.is_unresponsive:
                    # 4xx is a caller bug; do not fall back.
                    raise
                failures.append((current_role, exc))
                reason = "crashed" if exc.status_code >= 500 else "unresponsive"
            except OllamaTimeoutError as exc:
                failures.append((current_role, exc))
                reason = "timeout"
            except OllamaConnectError as exc:
                failures.append((current_role, exc))
                reason = "unresponsive"

            # Decide on the next hop.
            next_role = self._fallback_chain.get(current_role)
            if next_role is None or next_role not in self._endpoints:
                _LOG.error(
                    "ollama_no_fallback_available",
                    role=current_role,
                    next_role=next_role,
                    failures=[(r, type(e).__name__) for r, e in failures],
                )
                raise OllamaAllFallbacksExhaustedError(
                    (
                        f"role={current_role!r} failed and no fallback is configured "
                        f"(last error: {failures[-1][1]!s})"
                    ),
                    role=original_role,
                    model=self._endpoints[original_role].model,
                    failures=failures,
                )

            # Emit exactly one degraded event per fallback hop.
            await self._emit_degraded(
                from_role=current_role,
                to_role=next_role,
                reason=reason,
            )

            _LOG.info(
                "ollama_fallback",
                from_role=current_role,
                from_model=endpoint.model,
                to_role=next_role,
                to_model=self._endpoints[next_role].model,
                reason=reason,
            )
            current_role = next_role

    # ----- internal stream helper ------------------------------------------

    async def _stream_one(
        self,
        *,
        role: OllamaRoleKey,
        endpoint: OllamaModelEndpoint,
        prompt: str,
        options: Mapping[str, Any] | None,
        timeout_s: float | None,
    ) -> AsyncIterator[OllamaResponseChunk]:
        """Stream from a single endpoint. Translates httpx errors to ours."""
        assert self._http is not None  # ensured by start()
        body: dict[str, Any] = {
            "model": endpoint.model,
            "prompt": prompt,
            "stream": True,
        }
        if options:
            body.update(options)

        timeout = httpx.Timeout(
            timeout_s if timeout_s is not None else None,
            connect=min(
                endpoint.connect_timeout_s,
                timeout_s if timeout_s is not None else endpoint.connect_timeout_s,
            ),
        )

        attempt = 0
        last_exc: Optional[OllamaClientError] = None
        while attempt <= endpoint.max_retries:
            attempt += 1
            try:
                async with self._http.stream(
                    "POST",
                    endpoint.generate_url,
                    json=body,
                    timeout=timeout,
                ) as response:
                    if response.status_code >= 400:
                        # Drain to consume the body before httpx
                        # raises on context exit.
                        text = await response.aread()
                        raise OllamaHttpError(
                            (
                                f"role={role!r} model={endpoint.model!r} returned "
                                f"HTTP {response.status_code}: "
                                f"{text.decode('utf-8', errors='replace')[:200]}"
                            ),
                            role=role,
                            model=endpoint.model,
                            status_code=response.status_code,
                        )
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError as jexc:
                            # Malformed line — treat as a server fault.
                            raise OllamaHttpError(
                                f"role={role!r} sent non-JSON line: {jexc}",
                                role=role,
                                model=endpoint.model,
                                status_code=502,
                            ) from jexc

                        # ``done`` may be missing on early chunks; default False.
                        done = bool(obj.get("done", False))
                        text_chunk = str(obj.get("response", "") or "")
                        metrics: Optional[dict[str, Any]] = None
                        if done:
                            metrics = {
                                k: obj[k]
                                for k in (
                                    "total_duration",
                                    "load_duration",
                                    "prompt_eval_count",
                                    "prompt_eval_duration",
                                    "eval_count",
                                    "eval_duration",
                                    "context",
                                    "done_reason",
                                )
                                if k in obj
                            }
                        yield OllamaResponseChunk(
                            role=role,
                            model=endpoint.model,
                            text=text_chunk,
                            done=done,
                            metrics=metrics,
                        )
                # Successful stream completion.
                return
            except OllamaHttpError as exc:
                # 4xx → caller bug, never retry. 5xx → retry per endpoint.
                if not exc.is_unresponsive:
                    raise
                last_exc = exc
            except (httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout, httpx.ConnectTimeout) as exc:
                last_exc = OllamaTimeoutError(
                    f"role={role!r} model={endpoint.model!r} timed out: {exc}",
                    role=role,
                    model=endpoint.model,
                )
            except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError, httpx.NetworkError) as exc:
                last_exc = OllamaConnectError(
                    f"role={role!r} model={endpoint.model!r} unreachable: {exc}",
                    role=role,
                    model=endpoint.model,
                )

            # Per-endpoint retries before falling back to next role.
            if attempt > endpoint.max_retries:
                break

        # Exhausted retries on this endpoint.
        assert last_exc is not None
        raise last_exc

    # ----- helpers ---------------------------------------------------------

    async def _emit_degraded(
        self,
        *,
        from_role: OllamaRoleKey,
        to_role: OllamaRoleKey,
        reason: str,
    ) -> None:
        """Build and publish exactly one :class:`OllamaDegraded` event."""
        event = OllamaDegraded(
            model=self._endpoints[from_role].model,
            fallback_model=self._endpoints[to_role].model,
            reason=reason,  # type: ignore[arg-type]  # validated by enum
            ts_ns=int(self._wall_clock_ns()),
        )
        try:
            await self._publisher.publish_degraded(event)
        except Exception as exc:  # pragma: no cover - logged + dropped
            _LOG.warning(
                "ollama_degraded_publish_raised",
                error=str(exc),
                from_role=from_role,
                to_role=to_role,
                reason=reason,
            )

    def _validate_chain(self) -> None:
        """Validate that every fallback target is a registered role.

        Cycle detection is **not** performed here because a cycle is
        only fatal when the chain is actually walked end-to-end at
        request time — :meth:`stream_generate` handles that. Static
        validation only ensures every target is reachable.
        """
        for src, dst in self._fallback_chain.items():
            if src not in self._endpoints:
                raise ValueError(
                    f"fallback_chain references unregistered source role {src!r}"
                )
            if dst not in self._endpoints:
                raise ValueError(
                    f"fallback_chain references unregistered target role {dst!r}"
                )

    # ----- introspection ---------------------------------------------------

    @property
    def endpoints(self) -> Mapping[OllamaRoleKey, OllamaModelEndpoint]:
        """Read-only view of the registered endpoints."""
        return dict(self._endpoints)

    @property
    def fallback_chain(self) -> Mapping[OllamaRoleKey, OllamaRoleKey]:
        """Read-only view of the fallback chain."""
        return dict(self._fallback_chain)


__all__ = ["OllamaClient", "OllamaResponseChunk"]
