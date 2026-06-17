// Trader_Stability_Score — gauge + 4-bar component breakdown driven by /psych.
//
// Score = 0.35×Discipline + 0.25×EmotionalControl
//       + 0.20×RiskConsistency + 0.20×Patience  (R16.2).
// Rendered as a 0–100% gauge with a four-bar component breakdown
// (Discipline / EmotionalControl / RiskConsistency / Patience).

import { useCockpitStore } from "../store/cockpitStore";
import { Panel } from "../components/Panel";
import { EmptyState } from "../components/EmptyState";
import { pct, tsAgo } from "../lib/format";

const COMPONENT_LABELS: { key: "discipline" | "emotional_control" | "risk_consistency" | "patience"; label: string; weight: number }[] = [
  { key: "discipline", label: "Discipline", weight: 0.35 },
  { key: "emotional_control", label: "Emotional Control", weight: 0.25 },
  { key: "risk_consistency", label: "Risk Consistency", weight: 0.20 },
  { key: "patience", label: "Patience", weight: 0.20 },
];

function Bar({ value }: { value: number }): JSX.Element {
  const w = `${Math.max(0, Math.min(1, value)) * 100}%`;
  const tone = value < 0.4 ? "bg-hedge-danger" : value < 0.6 ? "bg-hedge-warn" : "bg-hedge-ok";
  return (
    <div className="h-2 w-full bg-slate-800 rounded">
      <div className={`h-2 ${tone} rounded`} style={{ width: w }} />
    </div>
  );
}

function Gauge({ score }: { score: number }): JSX.Element {
  const tone =
    score < 0.4 ? "text-hedge-danger" : score < 0.6 ? "text-hedge-warn" : "text-hedge-ok";
  return (
    <div className="flex items-baseline gap-2">
      <span className={`text-3xl font-mono ${tone}`}>{pct(score, 0)}</span>
      <span className="text-xs uppercase tracking-wider text-slate-500">stability</span>
    </div>
  );
}

export function TraderStabilityScore(): JSX.Element {
  const stab = useCockpitStore((s) => s.psych.stability);
  const interventions = useCockpitStore((s) => s.psych.interventions);
  const latestIntervention = interventions[0];

  return (
    <Panel
      title="Trader Stability Score"
      synthChannel="psych"
      status={
        latestIntervention ? (
          <span className="text-hedge-warn">
            intervention: {latestIntervention.action}
          </span>
        ) : (
          <span>{tsAgo(stab?.ts_ns)}</span>
        )
      }
    >
      {!stab ? (
        <EmptyState isEngineBacked />
      ) : (
        <>
          <Gauge score={stab.score} />
          <ul className="mt-3 space-y-2">
            {COMPONENT_LABELS.map(({ key, label, weight }) => {
              const v = stab.components[key];
              return (
                <li key={key}>
                  <div className="flex items-baseline justify-between text-[11px] text-slate-400">
                    <span>
                      {label}{" "}
                      <span className="text-slate-600">×{weight.toFixed(2)}</span>
                    </span>
                    <span className="font-mono text-slate-300">{pct(v, 0)}</span>
                  </div>
                  <Bar value={v} />
                </li>
              );
            })}
          </ul>
          {stab.behaviors.length > 0 ? (
            <div className="mt-3 text-[10px] text-slate-500">
              recent: {stab.behaviors.slice(0, 6).join(", ")}
            </div>
          ) : null}
        </>
      )}
    </Panel>
  );
}
