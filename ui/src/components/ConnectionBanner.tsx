import { useCockpitStore } from "../store/cockpitStore";

export function ConnectionBanner() {
  const feedStatus = useCockpitStore((s) => s.meta.feedStatus);

  let color = "text-slate-500";
  let label = feedStatus.toUpperCase();
  let dot = "⚪";

  switch (feedStatus) {
    case "open":
      color = "text-hedge-ok";
      dot = "🟢";
      label = "LIVE";
      break;
    case "degraded":
      color = "text-hedge-warn";
      dot = "🟡";
      label = "DEGRADED";
      break;
    case "offline":
      color = "text-hedge-danger";
      dot = "🔴";
      label = "OFFLINE";
      break;
    case "token_expired":
      color = "text-hedge-danger";
      dot = "🔴";
      label = "EXPIRED";
      break;
    case "market_closed":
      color = "text-hedge-warn";
      dot = "🟡";
      label = "CLOSED";
      break;
    case "demo_mode":
      color = "text-fuchsia-400";
      dot = "🟣";
      label = "SIMULATED";
      break;
  }

  return (
    <span className={`inline-flex items-center gap-1.5 font-medium ${color}`}>
      <span className="text-[9px]">{dot}</span>
      {label}
    </span>
  );
}
