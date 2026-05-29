// PROJECT HEDGE Human_Control_UI — WebSocket protocol envelope.
//
// The cockpit speaks a topic-subscription protocol against `ui-gateway` over a
// single WebSocket (design.md § "WebSocket Channels (UI Gateway)", R20.2).
//
// Wire format MUST match `crates/hedge-ui-gateway/src/protocol.rs`:
//
//   * Discriminator field name is `type` (not `op`).
//   * Subscribe filter list is `topics` (not `symbols`).
//   * Intents are emitted with `{type:"intent", kind, payload}` — `kind` is
//     a top-level discriminator, payload is the rest of the trader intent
//     fields.
//
// History note: the cockpit historically sent `{op:"subscribe",channel,symbols}`
// which the Rust side silently rejected as a bad frame. That mismatch was the
// root cause of the empty dashboard during the live-cockpit-data fix
// (live-cockpit-data spec, Phase 1, task 1.2).

import type { TraderIntent } from "./control";

export type ChannelId =
  | "market"
  | "orderflow"
  | "signals"
  | "risk"
  | "exec"
  | "news"
  | "psych"
  | "alerts"
  | "replay"
  | "latency"
  | "control";

// ---------------------------------------------------------------------------
// Server → Client envelopes
// ---------------------------------------------------------------------------

/** Outer envelope every server → client message conforms to.
 *
 * The Rust gateway tags each variant with `type`. The cockpit currently only
 * routes data for `type === "event"`; ack/error/pong/mode arrive on the same
 * socket but are surfaced through dedicated meta channels (see ws.ts). */
export interface ServerEnvelope<T = unknown> {
  /** Discriminator emitted by the gateway: `event | ack | error | mode | pong`. */
  type?: "event" | "ack" | "error" | "mode" | "pong";
  channel: ChannelId;
  /** NATS subject the gateway forwarded from, for diagnostics. */
  subject?: string;
  /** Server timestamp (ns since epoch) when the gateway emitted it. */
  ts_ns?: number;
  /** End-to-end correlation id, where applicable. */
  correlation_id?: string;
  payload: T;
}

// ---------------------------------------------------------------------------
// Client → Server messages (must match crates/hedge-ui-gateway/src/protocol.rs)
// ---------------------------------------------------------------------------

/** A subscription request is sent for every channel the cockpit needs.
 *
 * `topics` is an optional per-channel filter list (trading symbols for /market
 * and /orderflow, correlation ids for /signals). When empty/missing the
 * gateway forwards every event on that channel. */
export interface SubscribeMsg {
  type: "subscribe";
  channel: ChannelId;
  topics?: string[];
  request_id?: string;
}

export interface UnsubscribeMsg {
  type: "unsubscribe";
  channel: ChannelId;
  request_id?: string;
}

/** Trader → server intents flow on the /control channel.
 *
 * Wire shape per `protocol.rs` `ClientMsg::Intent`:
 *   `{type:"intent", kind: <IntentKind>, payload: <rest>}`.
 * `kind` is the snake_case intent discriminator (`killswitch`,
 * `strategy_toggle`, `priority`, `order`). The gateway forwards `payload`
 * verbatim onto the matching `trader.intent.<kind>` NATS subject. */
export interface IntentMsg {
  type: "intent";
  kind: "killswitch" | "strategy_toggle" | "priority" | "order" | "replay";
  payload: unknown;
  request_id?: string;
}

/** Application-level liveness probe (R20.2). */
export interface PingMsg {
  type: "ping";
  request_id?: string;
}

export type ClientMessage = SubscribeMsg | UnsubscribeMsg | IntentMsg | PingMsg;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Translate a cockpit-side `TraderIntent` into the wire `IntentMsg` shape.
 *
 * The cockpit's `TraderIntent` carries `{kind, ...rest}`. The gateway expects
 * `{type:"intent", kind, payload: {...rest}}`. This helper keeps the
 * cockpit-side panels untouched. */
export function intentToMessage(intent: TraderIntent): IntentMsg {
  const { kind, ...rest } = intent as { kind: IntentMsg["kind"] } & Record<string, unknown>;
  return { type: "intent", kind, payload: rest };
}
