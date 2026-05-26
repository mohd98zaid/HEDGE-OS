// /psych — `ai.psych.stability` and `ai.psych.intervention`,
// mirrored from crates/hedge-schemas/json_schemas/ai_psych_*.schema.json.
// Trader_Stability_Score = 0.35×Discipline + 0.25×EmotionalControl
//                        + 0.20×RiskConsistency + 0.20×Patience  (R16.2).

export interface PsychComponents {
  discipline: number;
  emotional_control: number;
  risk_consistency: number;
  patience: number;
}

export interface PsychStability {
  /** ∈ [0, 1] — surfaced as 0–100% in the cockpit gauge. */
  score: number;
  components: PsychComponents;
  /** Recent behaviour tags (revenge, fomo, tilt, ...). */
  behaviors: string[];
  ts_ns?: number;
}

export type PsychInterventionAction =
  | "warning"
  | "cooldown"
  | "size_reduction"
  | "kill_switch";

export interface PsychIntervention {
  action: PsychInterventionAction;
  trigger_score: number;
  ts_ns?: number;
}

export type PsychEvent =
  | { kind: "stability"; data: PsychStability }
  | { kind: "intervention"; data: PsychIntervention };
