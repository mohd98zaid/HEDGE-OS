// /latency — `obs.latency.*` aggregated for the Latency Dashboard
// (R20.3, R27.4). Mirror of LatencyRecord_v1.

export type LatencyStage =
  | "TickIngest"
  | "FeatureExtraction"
  | "AiScoringFetch"
  | "RiskCheck"
  | "ExecutionRouting"
  | "BrokerSubmit";

export interface LatencyRecord {
  correlation_id?: string;
  stage: LatencyStage;
  nanos: number;
  budget_nanos?: number;
  breach: boolean;
  ts_ns?: number;
}

/** Per-stage histogram bucket carried by the gateway aggregate stream. */
export interface LatencyAggregate {
  stage: LatencyStage;
  p50_nanos: number;
  p95_nanos: number;
  p99_nanos: number;
  budget_nanos: number;
  samples: number;
  breach_count: number;
  ts_ns?: number;
}

export type LatencyEvent =
  | { kind: "record"; data: LatencyRecord }
  | { kind: "aggregate"; data: LatencyAggregate };
