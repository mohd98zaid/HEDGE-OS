// Replay Controls — publishes `trader.intent.replay.*` on /control.
//
// Task 38.1 (trader-control widgets) will deepen this panel with full
// session-list / scrub UX. For 37.1 we wire start/pause/scrub against the
// gateway so the protocol surface is exercised end-to-end.

import { useState } from "react";

import { Panel } from "../components/Panel";
import { useCockpitStore } from "../store/cockpitStore";
import type { ReplayCommandIntent, TraderIntent } from "../types";

export interface ReplayControlsProps {
  sendIntent: (intent: TraderIntent) => boolean;
}

export function ReplayControls({ sendIntent }: ReplayControlsProps): JSX.Element {
  const status = useCockpitStore((s) => s.replay.status);
  const sessions = useCockpitStore((s) => s.replay.sessions);
  const [scrubTo, setScrubTo] = useState<number>(0);

  const send = (cmd: ReplayCommandIntent): void => {
    sendIntent({ kind: "replay", command: cmd });
  };

  const playing = status?.playing ?? false;

  return (
    <Panel title="Replay Controls" status={status ? <span>{status.session_id ?? "—"}</span> : null}>
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          className="rounded border border-slate-700 px-2 py-1 text-xs hover:border-hedge-accent"
          onClick={() => send({ op: "list" })}
        >
          list
        </button>
        <button
          type="button"
          className="rounded border border-slate-700 px-2 py-1 text-xs hover:border-hedge-accent"
          onClick={() => send({ op: "play", speed: 1 })}
          disabled={playing}
        >
          play 1×
        </button>
        <button
          type="button"
          className="rounded border border-slate-700 px-2 py-1 text-xs hover:border-hedge-accent"
          onClick={() => send({ op: "play", speed: 10 })}
          disabled={playing}
        >
          play 10×
        </button>
        <button
          type="button"
          className="rounded border border-slate-700 px-2 py-1 text-xs hover:border-hedge-accent"
          onClick={() => send({ op: "pause" })}
          disabled={!playing}
        >
          pause
        </button>
        <button
          type="button"
          className="rounded border border-slate-700 px-2 py-1 text-xs hover:border-hedge-accent"
          onClick={() => send({ op: "step" })}
        >
          step
        </button>
      </div>

      <div className="mt-3 flex items-center gap-2 text-xs">
        <label className="text-slate-500">scrub seq</label>
        <input
          type="number"
          min={0}
          value={scrubTo}
          onChange={(e) => setScrubTo(Number.parseInt(e.target.value || "0", 10))}
          className="w-24 rounded bg-slate-900 px-2 py-1 font-mono text-xs"
        />
        <button
          type="button"
          className="rounded border border-slate-700 px-2 py-1 text-xs hover:border-hedge-accent"
          onClick={() => send({ op: "scrub", sequence_no: scrubTo })}
        >
          go
        </button>
      </div>

      {sessions.length > 0 ? (
        <div className="mt-3">
          <div className="text-[10px] uppercase tracking-wider text-slate-500">
            sessions
          </div>
          <ul className="font-mono text-[11px] max-h-32 overflow-y-auto">
            {sessions.map((s) => (
              <li key={s.session_id} className="flex justify-between border-b border-slate-800/30 py-1">
                <button
                  type="button"
                  className="text-left text-slate-300 hover:text-hedge-accent"
                  onClick={() => send({ op: "load", session_id: s.session_id })}
                >
                  {s.session_id}
                </button>
                <span className="text-slate-500">{s.records.toLocaleString()} rec</span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {status ? (
        <div className="mt-3 grid grid-cols-3 gap-2 text-[11px] font-mono text-slate-400">
          <span>seq {status.sequence_no}</span>
          <span>{status.playing ? "playing" : "paused"}</span>
          <span>×{status.speed}</span>
        </div>
      ) : null}
    </Panel>
  );
}
