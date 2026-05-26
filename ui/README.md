# PROJECT HEDGE — Human_Control_UI

React 18 + TypeScript + Tailwind cockpit. The UI talks **only** to the
`hedge-ui-gateway` WebSocket bridge (design § WebSocket Channels; R20.2).

## Develop

```bash
cd ui
npm install
npm run dev    # vite dev server on :5173
npm run build  # tsc -b && vite build
npm run lint   # eslint .
```

The dev server proxies `/ws/*` to the gateway at `ws://hedge-ui-gateway:8080`
(see `vite.config.ts`).

## Layout

```
src/
  App.tsx                 — top-level grid layout; wires every panel.
  main.tsx                — React entry point.
  index.css               — Tailwind base.
  hooks/
    useUiGatewaySocket.ts — sole network boundary; multiplexes channels.
    useHighVolMode.ts     — R20.4 presentation-mode driver.
  store/
    cockpitStore.ts       — Zustand store, one slice per channel.
  types/                  — TypeScript interfaces mirroring the canonical
                            JSON schemas in crates/hedge-schemas/json_schemas.
  panels/                 — 16 React panels listed by R20.3.
  components/Panel.tsx    — shared chrome (titles, dim-on-high-vol).
  lib/
    config.ts             — env-var driven cockpit config.
    ws.ts                 — typed WebSocket client + reconnect.
    format.ts             — formatting helpers (paise→INR, latency, ago).
```

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `VITE_HEDGE_GATEWAY_URL` | `<window>://<host>/ws` | WebSocket base URL for the gateway. |
| `VITE_UI_HIGH_VOL_THRESHOLD` | `0.05` | Threshold on `md.breadth.volatility` that activates high-vol presentation mode. Mirrors `ui.high_vol_threshold` in the YAML config. |
| `VITE_UI_REFRESH_MS_NORMAL` | `250` | Critical-panel refresh tick in normal mode. |
| `VITE_UI_REFRESH_MS_HIGHVOL` | `100` | Critical-panel refresh tick in high-vol mode. |

## High-volatility presentation mode (R20.4)

When `md.breadth.volatility` exceeds `VITE_UI_HIGH_VOL_THRESHOLD`, the cockpit:

* Critical panels (Live Market, Orderflow Heatmap, Risk, Execution, Latency)
  refresh at `VITE_UI_REFRESH_MS_HIGHVOL`.
* Non-critical panels are dimmed via Tailwind `opacity-50` (see `Panel.tsx`).
* The `useHighVolMode` hook is the single source of truth; panels mark
  themselves `critical={true}` to opt out of dimming.

## Trader-control widgets (task 38.1)

`StrategyToggles`, `SymbolPriorityControls`, and `ReplayControls` are
scaffolds in this build — they exercise the `/control` channel protocol but
the deeper UX (per-strategy stats, P1/P2/P3/P4 management, scrub timeline)
lands in **task 38.1**.
