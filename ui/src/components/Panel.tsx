// Panel — shared chrome for every cockpit panel.
//
// `critical={true}` panels stay at full opacity in high-volatility mode and
// in War_Mode; all others dim per R20.4 (high-vol) and R26.3 (War_Mode
// reduced-clutter). See `useHighVolMode` and `useWarMode`.
//
// `synthChannel` opts the panel into the SynthBadge family (full-cockpit-data
// REQ-13): when the most recent envelope on that channel carried `_synth:
// true`, a small "synth" pill renders next to the title.

import type { ReactNode } from "react";

import { useHighVolMode } from "../hooks/useHighVolMode";
import { useWarMode } from "../hooks/useWarMode";
import { SynthBadge } from "./SynthBadge";
import type { ChannelId } from "../types";

export interface PanelProps {
  title: string;
  /** Critical panels stay vivid in high-vol mode (Live Market, Orderflow,
   *  Risk, Execution, Latency). Non-critical panels dim via opacity. */
  critical?: boolean;
  /** Render a small status string in the top-right corner. */
  status?: ReactNode;
  /** When set, renders a `SynthBadge` next to the title that flips on
   *  whenever the most-recent envelope on the named channel was synth. */
  synthChannel?: ChannelId;
  /** Tailwind utility classes appended to the wrapper div. */
  className?: string;
  children: ReactNode;
}

export function Panel({
  title,
  critical = false,
  status,
  synthChannel,
  className = "",
  children,
}: PanelProps): JSX.Element {
  const { dimClass: highVolDim, active: highVolActive } = useHighVolMode();
  const { dimClass: warModeDim, active: warModeActive } = useWarMode();
  const panelDim = !critical
    ? highVolActive
      ? highVolDim
      : warModeActive
        ? warModeDim
        : ""
    : "";
  return (
    <section
      className={`rounded-lg border border-slate-800 bg-hedge-panel p-4 transition-opacity duration-200 ${panelDim} ${className}`}
    >
      <header className="mb-3 flex items-baseline justify-between">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400">
          {title}
          {synthChannel ? <SynthBadge channel={synthChannel} /> : null}
        </h2>
        {status ? (
          <div className="text-[10px] font-mono text-slate-500">{status}</div>
        ) : null}
      </header>
      <div className="text-sm text-slate-200">{children}</div>
    </section>
  );
}
