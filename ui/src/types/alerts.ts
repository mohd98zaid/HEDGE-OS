// /alerts — UI-formatted alerts. The Alerts panel orders entries
//   1. by severity (critical → high → medium → low → info)
//   2. then by recency (newest first)
// per R20.5 (see panels/Alerts.tsx for the rationale + comparator).

export type AlertSeverity = "critical" | "high" | "medium" | "low" | "info";

export const ALERT_SEVERITY_RANK: Record<AlertSeverity, number> = {
  critical: 0,
  high: 1,
  medium: 2,
  low: 3,
  info: 4,
};

export interface Alert {
  id: string;
  severity: AlertSeverity;
  title: string;
  body?: string;
  source?: string;
  ts_ns?: number;
}
