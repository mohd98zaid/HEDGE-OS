import { type FeedStatus } from "./feedStatus";

export type EmptyReason =
  | "gateway_offline"
  | "market_closed"
  | "token_expired"
  | "degraded"
  | "engine_not_running"
  | "no_events";

export function deriveEmptyReason(
  feedStatus: FeedStatus,
  isEngineBacked: boolean
): EmptyReason {
  if (isEngineBacked) {
    return "engine_not_running";
  }

  switch (feedStatus) {
    case "token_expired":
      return "token_expired";
    case "offline":
      return "gateway_offline";
    case "market_closed":
      return "market_closed";
    case "degraded":
      return "degraded";
    case "open":
    case "demo_mode":
    default:
      return "no_events";
  }
}
