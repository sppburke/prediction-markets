import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Dark analytics palette.
        bg: "#0b0f14",
        panel: "#121821",
        panelAlt: "#0f141b",
        border: "#1f2a37",
        muted: "#8b97a7",
        text: "#e6edf3",
        pos: "#2ecc71",
        neg: "#e74c3c",
        accent: "#4ea1ff",
      },
      fontFamily: {
        mono: [
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Consolas",
          "monospace",
        ],
      },
    },
  },
  plugins: [],
};

export default config;
