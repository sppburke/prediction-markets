import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // The formatter contract test is pure (no DOM); node env keeps it fast.
    environment: "node",
    include: ["lib/**/*.test.ts"],
  },
});
