import { useEffect } from "react";
import { useCockpitStore } from "../store/cockpitStore";

/**
 * Triggers the store to recompute feed status every second.
 * This ensures that degraded/offline states transition even if the
 * websocket is completely silent.
 */
export function useFeedStatusTicker() {
  const recomputeFeedStatus = useCockpitStore((s) => s.recomputeFeedStatus);

  useEffect(() => {
    const timer = setInterval(() => {
      recomputeFeedStatus();
    }, 1000);

    return () => clearInterval(timer);
  }, [recomputeFeedStatus]);
}
