import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: { jsx: "automatic" },
  test: {
    // Pure helpers and server rendering need no DOM; node env keeps tests fast.
    environment: "node",
    include: ["lib/**/*.test.ts"],
  },
});
