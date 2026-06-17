export type FeedStatus =
  | "open"
  | "degraded"
  | "offline"
  | "token_expired"
  | "market_closed"
  | "demo_mode";

export interface FeedStatusInputs {
  gatewayState: "connecting" | "open" | "reconnecting" | "closed";
  /** ms timestamp of most recent md.tick.* applied to the store. */
  lastTickAt: number | undefined;
  /** Most recent ConnectionStatus for "upstox". */
  upstox: { status: "ok" | "degraded" | "down"; reason?: string } | undefined;
  /** Local IST clock in ms-since-epoch (Date.now() suffices). */
  nowMs: number;
  /** True iff Demo_Mode is active. */
  demoMode: boolean;
  /** ms duration the socket has been in `reconnecting` state. */
  reconnectingForMs: number;
}

function inHours(nowMs: number): boolean {
  const d = new Date(nowMs);
  const formatter = new Intl.DateTimeFormat("en-IN", {
    timeZone: "Asia/Kolkata",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
  });
  const parts = formatter.formatToParts(d);
  const hStr = parts.find((p) => p.type === "hour")?.value || "0";
  const mStr = parts.find((p) => p.type === "minute")?.value || "0";
  // Some Node versions return hour as '24' instead of '00'. Treat 24 as 0.
  let h = parseInt(hStr, 10);
  if (h === 24) h = 0;
  const m = parseInt(mStr, 10);
  const timeNum = h * 100 + m; // e.g., 915 for 09:15, 1530 for 15:30
  return timeNum >= 915 && timeNum <= 1530;
}

export function deriveFeedStatus(i: FeedStatusInputs): FeedStatus {
  if (i.demoMode) {
    return "demo_mode";
  }

  if (i.upstox?.status === "down" && /401|unauthorized/i.test(i.upstox.reason || "")) {
    return "token_expired";
  }

  if (i.gatewayState === "reconnecting" && i.reconnectingForMs >= 30_000) {
    return "offline";
  }

  const isMarketOpen = inHours(i.nowMs);
  const timeSinceLastTick = i.lastTickAt ? i.nowMs - i.lastTickAt : Infinity;

  if (i.upstox?.status === "down" || (timeSinceLastTick >= 30_000 && isMarketOpen)) {
    return "offline";
  }

  if (!isMarketOpen) {
    return "market_closed";
  }

  if (timeSinceLastTick >= 5_000) {
    return "degraded";
  }

  return "open";
}
