// Kill_Switch — prominent trader control (R20.6).
//
// Clicking the toggle publishes `trader.intent.killswitch { engaged }` on
// the /control channel via `sendIntent`. The Risk_Engine evaluates the
// intent under Authority_Hierarchy (design § Authority Hierarchy and
// Decision Flow) and, on accept, emits `risk.killswitch.activated`. The
// store mirrors that confirmation in `risk.killswitch`; this panel
// renders that authoritative state — never the optimistic intent.
//
// Engaging the kill-switch is high-impact (halts new entries, closes
// open positions per R5.6), so the action is gated behind a confirmation
// dialog. Disengaging skips the prompt because the side-effect is
// recoverable — the engine simply allows new approvals again.

import { useState } from "react";

import { Panel } from "../components/Panel";
import { useCockpitStore } from "../store/cockpitStore";
import { tsAgo } from "../lib/format";
import type { TraderIntent } from "../types";

export interface KillSwitchControlProps {
  sendIntent: (intent: TraderIntent) => boolean;
}

export function KillSwitchControl({ sendIntent }: KillSwitchControlProps): JSX.Element {
  const ks = useCockpitStore((s) => s.risk.killswitch);
  const [reason, setReason] = useState("");

  const engage = (): void => {
    const trimmed = reason.trim();
    const ok = window.confirm(
      "Engage the kill-switch?\n\n" +
        "This halts new Signal_Engine emissions and instructs the Risk_Engine " +
        "to deny every subsequent approval until disengaged. Open positions " +
        "are closed per R5.6.\n\n" +
        "Authority_Hierarchy still applies — the Risk_Engine will only " +
        "activate the kill-switch if its own preconditions hold.",
    );
    if (!ok) return;
    sendIntent({
      kind: "killswitch",
      engaged: true,
      ...(trimmed ? { reason: trimmed } : {}),
    });
  };

  const disengage = (): void => {
    sendIntent({ kind: "killswitch", engaged: false });
    setReason("");
  };

  const active = ks.active;
  const tone = active ? "text-hedge-danger" : "text-hedge-ok";
  const dotTone = active ? "bg-hedge-danger" : "bg-hedge-ok";

  return (
    <Panel
      title="Kill Switch"
      critical
      status={
        <span className={tone}>
          <span
            className={`mr-1 inline-block h-2 w-2 rounded-full ${dotTone}`}
            aria-hidden
          />
          {active ? "ENGAGED" : "ARMED"}
        </span>
      }
    >
      <div className="space-y-3">
        <div
          className={`rounded border p-3 text-xs ${
            active
              ? "border-hedge-danger/60 bg-hedge-danger/10 text-hedge-danger"
              : "border-slate-800 bg-slate-900/60 text-slate-300"
          }`}
        >
          {active ? (
            <>
              <div className="font-semibold uppercase tracking-wider">
                kill-switch engaged
              </div>
              <div className="mt-1">{ks.reason ?? "No reason recorded."}</div>
              <div className="mt-1 text-[10px] text-slate-400">{tsAgo(ks.ts_ns)}</div>
            </>
          ) : (
            <>
              <div className="font-semibold uppercase tracking-wider text-slate-400">
                kill-switch armed
              </div>
              <div className="mt-1 text-slate-500">
                Click to halt new entries. Open positions are closed by Risk_Engine.
              </div>
            </>
          )}
        </div>

        {!active ? (
          <label className="block text-xs">
            <span className="text-slate-500">reason (optional)</span>
            <input
              type="text"
              maxLength={512}
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="e.g. broker API latency spiking"
              className="mt-1 block w-full rounded bg-slate-900 px-2 py-1 font-mono text-xs"
            />
          </label>
        ) : null}

        {active ? (
          <button
            type="button"
            onClick={disengage}
            className="w-full rounded border border-hedge-ok/40 bg-hedge-ok/10 px-4 py-3 text-sm font-semibold uppercase tracking-wider text-hedge-ok transition hover:bg-hedge-ok/20"
          >
            disengage kill-switch
          </button>
        ) : (
          <button
            type="button"
            onClick={engage}
            className="w-full rounded border border-hedge-danger bg-hedge-danger/10 px-4 py-3 text-sm font-semibold uppercase tracking-wider text-hedge-danger transition hover:bg-hedge-danger/30"
          >
            engage kill-switch
          </button>
        )}

        <p className="text-[10px] leading-snug text-slate-500">
          Trader intents flow on <code className="font-mono">trader.intent.killswitch</code> and
          are evaluated by the Risk_Engine under Authority_Hierarchy. The cockpit
          renders the authoritative <code className="font-mono">risk.killswitch</code> state — not
          the optimistic intent.
        </p>
      </div>
    </Panel>
  );
}
