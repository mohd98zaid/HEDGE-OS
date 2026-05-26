// Pure formatting helpers used across cockpit panels.

export const paiseToInr = (paise: number): string =>
  `₹${(paise / 100).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

export const formatInr = (rupees: number): string =>
  `₹${rupees.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

export const pct = (x: number, digits = 2): string =>
  `${(x * 100).toFixed(digits)}%`;

export const fmtNanos = (ns: number): string => {
  if (ns < 1_000) return `${ns} ns`;
  if (ns < 1_000_000) return `${(ns / 1_000).toFixed(1)} µs`;
  if (ns < 1_000_000_000) return `${(ns / 1_000_000).toFixed(2)} ms`;
  return `${(ns / 1_000_000_000).toFixed(2)} s`;
};

export const tsAgo = (ts_ns: number | undefined): string => {
  if (!ts_ns) return "—";
  const ms = Date.now() - ts_ns / 1_000_000;
  if (ms < 1_000) return `${Math.max(0, Math.round(ms))} ms ago`;
  if (ms < 60_000) return `${Math.round(ms / 1_000)} s ago`;
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)} m ago`;
  return new Date(ts_ns / 1_000_000).toLocaleTimeString();
};
