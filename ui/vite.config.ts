import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// PROJECT HEDGE Human_Control_UI — Vite + React + TypeScript scaffold.
// The UI talks to the `hedge-ui-gateway` WebSocket bridge only (R20.2).

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    host: "0.0.0.0",
    proxy: {
      // ws://.../market, ws://.../orderflow, ... see design § WebSocket Channels.
      "/ws": {
        target: "ws://hedge-ui-gateway:8080",
        ws: true,
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
    target: "es2022",
  },
});
