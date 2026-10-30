import { defineConfig, devices } from "@playwright/test";

import { runTag } from "./support/names";
import { resolveTarget } from "./target";

const target = resolveTarget();
// Fix the run tag in the main process so every worker names objects the same way.
runTag();

const browser = { ...devices["Desktop Chrome"], locale: "en-US" };

export default defineConfig({
  testDir: "./tests",
  outputDir: "./test-results",
  timeout: 60_000,
  expect: { timeout: 15_000 },
  // The target is a shared, real deployment; keep concurrency modest.
  workers: 4,
  retries: 0,
  reporter: [["list"], ["html", { outputFolder: "./playwright-report", open: "never" }]],
  use: {
    baseURL: target.baseURL,
    // Fail fast when a control never appears instead of waiting out a long journey timeout.
    actionTimeout: 30_000,
    screenshot: "only-on-failure",
    // Traces record request headers, which would persist the bearer token to disk.
    // Opt in per run with --trace on when debugging, and delete test-results/ afterwards.
    trace: "off",
  },
  projects: [
    // Read-only checks; safe against any deployment.
    { name: "smoke", testDir: "./tests/smoke", fullyParallel: true, use: browser },
    // Read-only; nothing depends on it, so an accessibility finding never blocks the journeys.
    { name: "accessibility", testDir: "./tests/accessibility", use: browser },
    // Journeys create, change and delete real objects. The setup project sweeps
    // leftovers from aborted runs first; each spec file owns its sample table and
    // runs its tests in order, while different files run in parallel.
    {
      name: "journeys-setup",
      testDir: "./tests/journeys",
      testMatch: /.*\.setup\.ts/,
      dependencies: ["smoke"],
      use: browser,
    },
    {
      name: "journeys",
      testDir: "./tests/journeys",
      testIgnore: /isolated\//,
      fullyParallel: false,
      dependencies: ["journeys-setup"],
      use: browser,
    },
    // Journeys that touch shared state — app-wide settings, or rules Studio names
    // itself (marketplace, data contracts) that can't be told apart by prefix —
    // run one at a time after every other journey has finished.
    {
      name: "isolated",
      testDir: "./tests/journeys/isolated",
      fullyParallel: false,
      workers: 1,
      dependencies: ["journeys"],
      use: browser,
    },
  ],
});
