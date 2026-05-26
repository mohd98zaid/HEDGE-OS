// /replay — control plane and frame stream (R20.3, R22.3).

export type ReplaySpeed = 1 | 10 | "max";

/** Server → client frames. */
export interface ReplayStatus {
  session_id?: string;
  playing: boolean;
  speed: ReplaySpeed;
  sequence_no: number;
  total_records?: number;
}

export interface ReplayFrame {
  sequence_no: number;
  record_kind: string;
  ts_ns?: number;
}

export type ReplayEvent =
  | { kind: "status"; data: ReplayStatus }
  | { kind: "frame"; data: ReplayFrame }
  | { kind: "list"; data: { sessions: ReplaySession[] } };

export interface ReplaySession {
  session_id: string;
  started_at_utc: string;
  records: number;
  size_bytes?: number;
}

/**
 * Client → server replay commands. Published as `trader.intent.replay.*` on
 * /control by the Replay Controls panel. The actual NATS subjects live behind
 * the gateway; the panel only emits these typed payloads.
 */
export type ReplayCommand =
  | { kind: "list" }
  | { kind: "load"; session_id: string }
  | { kind: "play"; speed: ReplaySpeed }
  | { kind: "pause" }
  | { kind: "step" }
  | { kind: "scrub"; sequence_no: number };
