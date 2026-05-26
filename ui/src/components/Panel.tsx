// Panel — shared chrome for every cockpit panel.
//
// `critical={true}` panels stay at full opacity in high-volatility mode and
// in War_Mode; all others dim per R20.4 (high-vol) and R26.3 (War_Mode
// reduced-clutter). See `useHighVolMode` and `useWarMode`.

import type { ReactNode } from "react";

import { useHighVolMode } from "../hooks/useHighVolMode";
import { useWarMode } from "../hooks/useWarMode";

export interface PanelProps {
  title: string;
  /** Critical panels stay vivid in high-vol mode (Live Market, Orderflow,
   *  Risk, Execution, Latency). Non-critical panels dim via opacity. */
  critical?: boolean;
  /** Render a small status string in the top-right corner. */
  status?: ReactNode;
  /** Tailwind utility classes appended to the wrapper div. */
  className?: string;
  children: ReactNode;
}

export function Panel({
  title,
  critical = false,
  status,
  className = "",
  children,
}: PanelProps): JSX.Element {
  const { dimClass: highVolDim, active: highVolActive } = useHighVolMode();
  const { dimClass: warModeDim, active: warModeActive } = useWarMode();
  // Reduced-clutter rule (R26.3): non-critical panels dim while War_Mode is
  // active. R20.4 (high-vol) takes precedence visually because that mode
  // signals an immediate alpha-affecting condition; War_Mode is a calmer
  // schedule-driven dim.
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
        </h2>
        {status ? (
          <div className="text-[10px] font-mono text-slate-500">{status}</div>
        ) : null}
      </header>
      <div className="text-sm text-slate-200">{children}</div>
    </section>
  );
}
