# PROJECT HEDGE — Human_Control_UI

React 18 + TypeScript + Tailwind cockpit, scaffolded with Vite. The cockpit
talks **only** to the `hedge-ui-gateway` WebSocket bridge (design § WebSocket
Channels; R20.2).

## Develop

```bash
cd ui
npm install
npm run dev
```

The dev server listens on `:5173` and proxies `/ws/*` to the gateway at
`ws://hedge-ui-gateway:8080` (configured in `vite.config.ts`).

## Why is `node_modules/` empty?

This scaffold ships only configuration files — the workspace bootstrap (task
1.1) does **not** run `npm install`. The task list installs dependencies in the
infrastructure setup step, not the scaffold step.
