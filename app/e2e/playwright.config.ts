import { defineConfig, devices } from "@playwright/test";

import { resolveTarget } from "./target";

const target = resolveTarget();

export default defineConfig({
  testDir: "./tests",
  outputDir: "./test-results",
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: true,
  // The target is a shared, real deployment; keep concurrency modest.
  workers: 4,
  retries: 0,
  reporter: [["list"], ["html", { outputFolder: "./playwright-report", open: "never" }]],
  use: {
    baseURL: target.baseURL,
    screenshot: "only-on-failure",
    // Traces record request headers, which would persist the bearer token to disk.
    // Opt in per run with --trace on when debugging, and delete test-results/ afterwards.
    trace: "off",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
