// PROJECT HEDGE Human_Control_UI — WebSocket subscription client.
//
// The cockpit connects to the `ui-gateway` Rust bridge over a single
// WebSocket. Channels are multiplexed via a topic-subscription protocol
// (see design.md § WebSocket Channels (UI Gateway), R20.2): every
// server frame is a JSON envelope `{ channel, payload, ... }`; the
// client subscribes/unsubscribes per channel and publishes trader
// intents on `/control`.
//
// This module is **the only** outbound dependency for live data — the
// React panels never read from NATS or Redis directly (R20.2).

import type {
  ChannelId,
  ClientMessage,
  ServerEnvelope,
  TraderIntent,
} from "../types";

export type ChannelHandler<T = unknown> = (envelope: ServerEnvelope<T>) => void;
export type StateHandler = (state: GatewayState) => void;

export type GatewayState =
  | "connecting"
  | "open"
  | "reconnecting"
  | "closed";

export interface GatewayClientOptions {
  url: string;
  /** Initial reconnect delay; doubles up to maxReconnectMs. */
  initialReconnectMs?: number;
  maxReconnectMs?: number;
  /** Symbols filter forwarded with /market + /orderflow subscribe ops. */
  symbols?: string[];
}

/**
 * Minimal, dependency-free WebSocket client tailored for the cockpit.
 *
 * Lifecycle:
 *  1. `connect()` opens the socket and (re)subscribes every channel that
 *     was previously requested via `subscribe()`.
 *  2. On unexpected close, an exponential backoff reconnect kicks in;
 *     subscriptions are replayed once the new socket opens.
 *  3. `sendIntent()` posts trader intents on the `/control` channel.
 */
export class GatewayClient {
  private readonly url: string;
  private readonly initialReconnectMs: number;
  private readonly maxReconnectMs: number;
  private readonly handlers = new Map<ChannelId, Set<ChannelHandler>>();
  private readonly stateHandlers = new Set<StateHandler>();
  private readonly subscribed = new Set<ChannelId>();
  private symbols: string[] | undefined;

  private socket: WebSocket | null = null;
  private state: GatewayState = "closed";
  private reconnectMs: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private shouldRun = false;

  constructor(opts: GatewayClientOptions) {
    this.url = opts.url;
    this.initialReconnectMs = opts.initialReconnectMs ?? 500;
    this.maxReconnectMs = opts.maxReconnectMs ?? 10_000;
    this.reconnectMs = this.initialReconnectMs;
    this.symbols = opts.symbols;
  }

  /** Open the socket and start receiving frames. */
  connect(): void {
    this.shouldRun = true;
    if (this.socket) return;
    this.openSocket();
  }

  /** Tear down the socket and stop reconnecting. */
  disconnect(): void {
    this.shouldRun = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.socket) {
      this.socket.onclose = null;
      this.socket.close(1000, "client disconnect");
      this.socket = null;
    }
    this.setState("closed");
  }

  /** Subscribe a handler to a channel; sends a subscribe op if first. */
  subscribe<T>(channel: ChannelId, handler: ChannelHandler<T>): () => void {
    let bucket = this.handlers.get(channel);
    if (!bucket) {
      bucket = new Set();
      this.handlers.set(channel, bucket);
    }
    bucket.add(handler as ChannelHandler);

    if (!this.subscribed.has(channel)) {
      this.subscribed.add(channel);
      this.sendSubscribe(channel);
    }

    return () => {
      const live = this.handlers.get(channel);
      if (!live) return;
      live.delete(handler as ChannelHandler);
      if (live.size === 0) {
        this.handlers.delete(channel);
        this.subscribed.delete(channel);
        this.send({ op: "unsubscribe", channel });
      }
    };
  }

  /** Subscribe to gateway connection-state transitions. */
  onState(handler: StateHandler): () => void {
    this.stateHandlers.add(handler);
    handler(this.state);
    return () => {
      this.stateHandlers.delete(handler);
    };
  }

  /** Push a trader intent onto the `/control` channel (R20.6/7/8). */
  sendIntent(intent: TraderIntent): boolean {
    return this.send({ op: "intent", channel: "control", payload: intent });
  }

  /** Replace symbol filter on /market and /orderflow channels. */
  setSymbols(symbols: string[]): void {
    this.symbols = symbols;
    if (this.subscribed.has("market")) this.sendSubscribe("market");
    if (this.subscribed.has("orderflow")) this.sendSubscribe("orderflow");
  }

  // ----- internals --------------------------------------------------------

  private openSocket(): void {
    this.setState(this.socket ? "reconnecting" : "connecting");
    let socket: WebSocket;
    try {
      socket = new WebSocket(this.url);
    } catch (err) {
      // URL invalid or runtime denied; treat as a closed transport and retry.
      this.scheduleReconnect();
      // eslint-disable-next-line no-console
      console.error("[ws] failed to open WebSocket", err);
      return;
    }
    this.socket = socket;

    socket.onopen = () => {
      this.reconnectMs = this.initialReconnectMs;
      this.setState("open");
      // Replay every active subscription so the gateway reflects our state.
      for (const channel of this.subscribed) this.sendSubscribe(channel);
    };

    socket.onmessage = (ev) => {
      this.handleFrame(ev.data);
    };

    socket.onerror = () => {
      // We don't surface errors directly; close → reconnect drives recovery.
    };

    socket.onclose = () => {
      this.socket = null;
      if (this.shouldRun) {
        this.scheduleReconnect();
      } else {
        this.setState("closed");
      }
    };
  }

  private scheduleReconnect(): void {
    this.setState("reconnecting");
    const delay = this.reconnectMs;
    this.reconnectMs = Math.min(this.maxReconnectMs, this.reconnectMs * 2);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.shouldRun) this.openSocket();
    }, delay);
  }

  private handleFrame(raw: unknown): void {
    if (typeof raw !== "string") return; // binary frames are not used by R20.2
    let env: ServerEnvelope;
    try {
      env = JSON.parse(raw) as ServerEnvelope;
    } catch {
      // eslint-disable-next-line no-console
      console.warn("[ws] non-JSON frame ignored", raw);
      return;
    }
    if (!env || typeof env.channel !== "string") return;
    const bucket = this.handlers.get(env.channel);
    if (!bucket) return;
    for (const fn of bucket) {
      try {
        fn(env);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error("[ws] handler threw on channel", env.channel, err);
      }
    }
  }

  private sendSubscribe(channel: ChannelId): void {
    const symbols =
      channel === "market" || channel === "orderflow" ? this.symbols : undefined;
    this.send({ op: "subscribe", channel, ...(symbols ? { symbols } : {}) });
  }

  private send(msg: ClientMessage): boolean {
    const sock = this.socket;
    if (!sock || sock.readyState !== WebSocket.OPEN) return false;
    try {
      sock.send(JSON.stringify(msg));
      return true;
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("[ws] send failed", err);
      return false;
    }
  }

  private setState(state: GatewayState): void {
    if (this.state === state) return;
    this.state = state;
    for (const fn of this.stateHandlers) fn(state);
  }
}
