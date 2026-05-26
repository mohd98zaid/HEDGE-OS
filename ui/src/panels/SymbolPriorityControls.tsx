// Symbol Priority Controls — per-symbol P1/P2/P3/P4 management grid (R20.8).
//
// Each row publishes `trader.intent.priority { symbol, to }` on /control via
// `sendIntent`. The Risk_Engine evaluates the intent under Authority_Hierarchy
// (design.md § Authority Hierarchy and Decision Flow) and, on accept, the
// Signal_Engine emits `ai.priority.changed.<sym>` which the cockpit store
// folds into `signals.priorities`. The grid renders that authoritative tier
// per symbol; clicking a tier button optimistically updates the local mirror
// (`setSymbolPriority`) so the trader gets responsive feedback while the
// intent travels UI → gateway → Risk_Engine → Signal_Engine.
//
// Empty-state fallback: when no symbols have been tiered yet (the very first
// session, or a fresh restart before any `ai.priority.changed.*` arrives),
// the trader can hand-enter a symbol and publish a tier for it. The same
// inline form also lets the trader add new symbols mid-session.

import { useMemo, useState } from "react";

import { Panel } from "../components/Panel";
import { useCockpitStore } from "../store/cockpitStore";
import {
  PRIORITY_TIERS,
  type PriorityTier,
  type TraderIntent,
} from "../types";

export interface SymbolPriorityControlsProps {
  sendIntent: (intent: TraderIntent) => boolean;
}

/** Color tone per tier — P1 is most-critical, P4 is idle. */
const TIER_TONE: Readonly<Record<PriorityTier, string>> = {
  P1: "border-hedge-danger/70 text-hedge-danger",
  P2: "border-hedge-warn/70 text-hedge-warn",
  P3: "border-hedge-accent/70 text-hedge-accent",
  P4: "border-slate-700 text-slate-400",
};

const TIER_TONE_ACTIVE: Readonly<Record<PriorityTier, string>> = {
  P1: "border-hedge-danger bg-hedge-danger/15 text-hedge-danger",
  P2: "border-hedge-warn bg-hedge-warn/15 text-hedge-warn",
  P3: "border-hedge-accent bg-hedge-accent/15 text-hedge-accent",
  P4: "border-slate-500 bg-slate-800 text-slate-200",
};

export function SymbolPriorityControls({
  sendIntent,
}: SymbolPriorityControlsProps): JSX.Element {
  const priorities = useCockpitStore((s) => s.signals.priorities);
  const setSymbolPriority = useCockpitStore((s) => s.setSymbolPriority);

  // Stable, alphabetised order so rows don't shuffle as the engine streams in
  // updates for already-tracked symbols.
  const rows = useMemo(
    () =>
      Object.entries(priorities)
        .map(([symbol, tier]) => ({ symbol, tier: tier as PriorityTier }))
        .sort((a, b) => a.symbol.localeCompare(b.symbol)),
    [priorities],
  );

  // Inline "add symbol" form — the only path to publish an intent before the
  // engine has streamed any `ai.priority.changed.*` for the symbol.
  const [draftSymbol, setDraftSymbol] = useState("");
  const [draftTier, setDraftTier] = useState<PriorityTier>("P2");

  const publish = (symbol: string, tier: PriorityTier): void => {
    const sym = symbol.trim().toUpperCase();
    if (!sym) return;
    // Optimistic mirror — the authoritative tier still lands via
    // `ai.priority.changed.<sym>` and overwrites this entry.
    setSymbolPriority(sym, tier);
    sendIntent({ kind: "priority", symbol: sym, to: tier });
  };

  const submitDraft = (): void => {
    if (!draftSymbol.trim()) return;
    publish(draftSymbol, draftTier);
    setDraftSymbol("");
  };

  return (
    <Panel title="Symbol Priority">
      {rows.length === 0 ? (
        <p className="mb-3 text-[10px] text-slate-500">
          No symbols tiered yet. Add one below to publish the first
          <code className="mx-1 font-mono">trader.intent.priority</code>.
        </p>
      ) : (
        <ul className="mb-3 divide-y divide-slate-800/60">
          {rows.map(({ symbol, tier }) => (
            <li
              key={symbol}
              className="flex items-center justify-between gap-3 py-2"
            >
              <div className="flex min-w-0 items-center gap-2">
                <span className="font-mono text-xs text-slate-200">
                  {symbol}
                </span>
                <span
                  className={`rounded border px-1.5 py-0.5 text-[10px] font-mono uppercase ${TIER_TONE_ACTIVE[tier]}`}
                  title="authoritative tier from ai.priority.changed"
                >
                  {tier}
                </span>
              </div>
              <div
                role="group"
                aria-label={`set priority for ${symbol}`}
                className="flex items-center gap-1"
              >
                {PRIORITY_TIERS.map((t) => {
                  const isActive = tier === t;
                  const tone = isActive ? TIER_TONE_ACTIVE[t] : TIER_TONE[t];
                  return (
                    <button
                      key={t}
                      type="button"
                      onClick={() => publish(symbol, t)}
                      aria-pressed={isActive}
                      className={`rounded border px-2 py-0.5 text-[10px] font-mono uppercase transition hover:bg-slate-800/60 ${tone}`}
                    >
                      {t}
                    </button>
                  );
                })}
              </div>
            </li>
          ))}
        </ul>
      )}

      <form
        onSubmit={(e) => {
          e.preventDefault();
          submitDraft();
        }}
        className="flex flex-wrap items-center gap-2 border-t border-slate-800/60 pt-3 text-xs"
        aria-label="add symbol priority"
      >
        <input
          type="text"
          placeholder="symbol"
          value={draftSymbol}
          onChange={(e) => setDraftSymbol(e.target.value)}
          maxLength={32}
          className="w-24 rounded bg-slate-900 px-2 py-1 font-mono uppercase placeholder:normal-case placeholder:text-slate-600"
          aria-label="symbol"
        />
        <select
          value={draftTier}
          onChange={(e) => setDraftTier(e.target.value as PriorityTier)}
          className="rounded bg-slate-900 px-2 py-1 font-mono"
          aria-label="tier"
        >
          {PRIORITY_TIERS.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        <button
          type="submit"
          disabled={!draftSymbol.trim()}
          className="rounded border border-slate-700 px-2 py-1 hover:border-hedge-accent disabled:opacity-40"
        >
          set priority
        </button>
      </form>

      <p className="mt-3 text-[10px] leading-snug text-slate-500">
        Buttons publish <code className="font-mono">trader.intent.priority</code>{" "}
        and are evaluated by the Risk_Engine under Authority_Hierarchy. The
        cockpit renders the authoritative tier from{" "}
        <code className="font-mono">ai.priority.changed.&lt;sym&gt;</code> — the
        click is just an optimistic mirror.
      </p>
    </Panel>
  );
}
