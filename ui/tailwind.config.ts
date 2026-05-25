import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        // Cockpit semantic colours, refined in task E2.
        hedge: {
          bg: "#020617",
          panel: "#0f172a",
          accent: "#22d3ee",
          warn: "#f59e0b",
          danger: "#ef4444",
          ok: "#10b981",
        },
      },
      fontFamily: {
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
      },
    },
  },
  plugins: [],
};

export default config;
