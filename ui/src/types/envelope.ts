// PROJECT HEDGE Human_Control_UI — WebSocket protocol envelope.
//
// The cockpit speaks a topic-subscription protocol against `ui-gateway` over a
// single WebSocket (design.md § "WebSocket Channels (UI Gateway)", R20.2).
// Server → client frames are JSON envelopes carrying a `channel` discriminator
// + payload; client → server frames are subscribe/unsubscribe/intent ops.

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

/** Outer envelope every server → client message conforms to. */
export interface ServerEnvelope<T = unknown> {
  channel: ChannelId;
  /** NATS subject the gateway forwarded from, for diagnostics. */
  subject?: string;
  /** Server timestamp (ns since epoch) when the gateway emitted it. */
  ts_ns?: number;
  /** End-to-end correlation id, where applicable. */
  correlation_id?: string;
  payload: T;
}

/** A subscription request is sent for every channel the cockpit needs. */
export interface SubscribeMsg {
  op: "subscribe";
  channel: ChannelId;
  /** Optional symbol filter, accepted by /market and /orderflow. */
  symbols?: string[];
}

export interface UnsubscribeMsg {
  op: "unsubscribe";
  channel: ChannelId;
}

import type { TraderIntent } from "./control";

/** Trader → server intents flow on the /control channel. */
export interface IntentMsg {
  op: "intent";
  channel: "control";
  payload: TraderIntent;
}

export type ClientMessage = SubscribeMsg | UnsubscribeMsg | IntentMsg;
