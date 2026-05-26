// useUiGatewaySocket — single typed WebSocket client against `ui-gateway`.
//
// Responsibilities (R20.2):
//   1. Connect once per app session, multiplex every channel through one socket.
//   2. Subscribe to every channel the cockpit needs and dispatch each inbound
//      envelope into the matching slice of `useCockpitStore`.
//   3. Reconnect with exponential backoff on drop, replaying subscriptions.
//   4. Expose `sendIntent` so trader-control widgets can publish on /control.
//
// **No REST polling anywhere.** This hook is the sole network boundary for
// live data; all panels read from the cockpit store, never from the network.

import { useEffect, useMemo, useRef } from "react";

import { GatewayClient, type ChannelHandler } from "../lib/ws";
import { loadConfig } from "../lib/config";
import { useCockpitStore } from "../store/cockpitStore";
import type { ChannelId, ServerEnvelope, TraderIntent } from "../types";

const ALL_CHANNELS: ChannelId[] = [
  "market",
  "orderflow",
  "signals",
  "risk",
  "exec",
  "news",
  "psych",
  "alerts",
  "replay",
  "latency",
];

export interface UseUiGatewaySocket {
  sendIntent: (intent: TraderIntent) => boolean;
  setSymbols: (symbols: string[]) => void;
}

export function useUiGatewaySocket(symbols?: string[]): UseUiGatewaySocket {
  const config = useMemo(loadConfig, []);
  const clientRef = useRef<GatewayClient | null>(null);

  // Stable callback identity — reads from the store via getState so we don't
  // re-create handlers per render.
  const dispatch: ChannelHandler = useMemo(() => {
    const apply = (env: ServerEnvelope): void => {
      useCockpitStore.getState().applyEnvelope(env);
    };
    return apply;
  }, []);

  useEffect(() => {
    const client = new GatewayClient({
      url: config.gatewayUrl,
      symbols,
    });
    clientRef.current = client;

    const offState = client.onState((state) => {
      useCockpitStore.getState().setGatewayState(state);
    });

    const offs = ALL_CHANNELS.map((ch) => client.subscribe(ch, dispatch));
    client.connect();

    return () => {
      offState();
      for (const off of offs) off();
      client.disconnect();
      clientRef.current = null;
    };
    // We intentionally exclude `symbols` from the deps; symbol filter changes
    // route through the imperative `setSymbols()` below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config.gatewayUrl, dispatch]);

  // Surface a tiny imperative API for trader-control widgets and symbol filters.
  return useMemo<UseUiGatewaySocket>(
    () => ({
      sendIntent: (intent: TraderIntent) =>
        clientRef.current?.sendIntent(intent) ?? false,
      setSymbols: (next: string[]) => clientRef.current?.setSymbols(next),
    }),
    [],
  );
}
