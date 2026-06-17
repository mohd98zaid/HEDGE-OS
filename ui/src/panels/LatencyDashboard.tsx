// Latency Dashboard — per-stage p50/p95/p99 from /latency aggregates
// (R20.3, R27.4). Critical panel: stays vivid in high-vol mode.

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { EmptyState } from "../components/EmptyState";
import { fmtNanos } from "../lib/format";
import type { LatencyStage } from "../types";

const STAGE_ORDER: LatencyStage[] = [
  "TickIngest",
  "FeatureExtraction",
  "AiScoringFetch",
  "RiskCheck",
  "ExecutionRouting",
  "BrokerSubmit",
];

export function LatencyDashboard(): JSX.Element {
  const aggregates = useCockpitStore((s) => s.latency.aggregates);
  const records = useCockpitStore((s) => s.latency.records);

  return (
    <Panel
      title="Latency"
      synthChannel="latency"
      critical
      status={<span>{records.length} samples</span>}
    >
      {records.length === 0 ? (
        <EmptyState isEngineBacked />
      ) : (
      <table className="w-full font-mono text-[11px]">
        <thead className="text-slate-500">
          <tr>
            <th className="text-left">Stage</th>
            <th className="text-right">p50</th>
            <th className="text-right">p95</th>
            <th className="text-right">p99</th>
            <th className="text-right">budget</th>
            <th className="text-right">breaches</th>
          </tr>
        </thead>
        <tbody>
          {STAGE_ORDER.map((stage) => {
            const a = aggregates[stage];
            return (
              <tr key={stage} className="border-t border-slate-800/40">
                <td className="text-slate-300 py-1">{stage}</td>
                <td className="text-right">{a ? fmtNanos(a.p50_nanos) : "—"}</td>
                <td className="text-right">{a ? fmtNanos(a.p95_nanos) : "—"}</td>
                <td className="text-right">{a ? fmtNanos(a.p99_nanos) : "—"}</td>
                <td className="text-right text-slate-500">
                  {a?.budget_nanos ? fmtNanos(a.budget_nanos) : "—"}
                </td>
                <td
                  className={`text-right ${
                    (a?.breach_count ?? 0) > 0 ? "text-hedge-danger" : "text-slate-500"
                  }`}
                >
                  {a?.breach_count ?? 0}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      )}
    </Panel>
  );
}
