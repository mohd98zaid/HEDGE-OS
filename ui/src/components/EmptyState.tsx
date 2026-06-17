import { useCockpitStore } from "../store/cockpitStore";
import { deriveEmptyReason } from "../lib/emptyReason";

export interface EmptyStateProps {
  isEngineBacked?: boolean;
}

export function EmptyState({ isEngineBacked = false }: EmptyStateProps) {
  const feedStatus = useCockpitStore((s) => s.meta.feedStatus);
  const reason = deriveEmptyReason(feedStatus, isEngineBacked);

  let message = "";
  switch (reason) {
    case "gateway_offline":
      message = "Gateway disconnected or unreachable.";
      break;
    case "market_closed":
      message = "Market is currently closed.";
      break;
    case "token_expired":
      message = "Upstox token expired. Please refresh the token in .env.";
      break;
    case "degraded":
      message = "Feed is degraded. Waiting for data...";
      break;
    case "engine_not_running":
      message = "Awaiting first event...";
      break;
    case "no_events":
      message = "Awaiting first event...";
      break;
  }

  return (
    <div className="flex h-full min-h-[120px] items-center justify-center p-4">
      <span className="text-sm text-slate-500 italic">{message}</span>
    </div>
  );
}
