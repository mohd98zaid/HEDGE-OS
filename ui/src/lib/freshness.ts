import { useEffect, useState } from "react";
import { useCockpitStore } from "../store/cockpitStore";
import type { ChannelId } from "../types";

export type Freshness = "fresh" | "stale" | "dead" | "hidden";

export function usePanelFreshness(channel?: ChannelId): Freshness {
  const lastSeen = useCockpitStore((s) =>
    channel ? s.meta.lastSeenByChannel[channel] : undefined
  );
  const feedStatus = useCockpitStore((s) => s.meta.feedStatus);
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    if (!channel) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [channel]);

  if (!channel) return "hidden";
  if (feedStatus === "offline" || feedStatus === "token_expired") {
    return "hidden";
  }

  if (!lastSeen) return "dead";

  const elapsed = now - lastSeen;
  if (elapsed < 2000) return "fresh";
  if (elapsed < 10000) return "stale";
  return "dead";
}
