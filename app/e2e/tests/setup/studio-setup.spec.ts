import { readFileSync } from "node:fs";
import { join } from "node:path";

import type { Page } from "@playwright/test";

import { expect, test } from "../../fixtures";
import type { SetupStatus, SetupStep, StudioApi } from "../../support/studio-api";
import { openStudio, sidebarLinks } from "../../support/ui";

// Runs before every other project: takes a fresh deployment through the setup
// wizard as an administrator, the way a customer would after installing Studio,
// and leaves it ready for smoke, accessibility and the journeys. On a deployment
// whose setup already finished it only checks that Studio is ready.
//
// The wizard is driven only through what it offers: running a step's GRANT
// statements and verifying again. It never overrides a step, because an
// override would hide exactly the problem setup exists to catch; a step that
// needs one (or any other manual fix) fails the test and names the step.
// Along the way it checks what the wizard shows against the backend's report.
// A second test checks the setup page of a ready deployment, which a fresh
// browser sees once when setup finished with warnings.

const SETUP_COPY = JSON.parse(
  readFileSync(
    join(__dirname, "..", "..", "..", "src", "databricks_labs_dqx_app", "ui", "lib", "i18n", "locales", "en.json"),
    "utf8",
  ),
) as {
  setup: {
    title: string;
    steps: Record<string, string>;
    states: Record<string, string>;
    actions: Record<string, string>;
    warningsReview: { title: string; acknowledge: string };
    configuration: { submit: string };
    override: { button: Record<string, string>; help: Record<string, string> };
  };
  sqlCodeBlock: { run: string };
};

/** States that no longer block readiness. */
const SETTLED = new Set(["passed", "overridden", "warning"]);
/** Each round runs the grants Studio still lists, then verifies again. */
const MAX_ROUNDS = 4;
/** A fresh deployment creates schemas, runs migrations and refreshes metadata before it can report. */
const CHECK_TIMEOUT_MS = 10 * 60_000;

function blockingSteps(status: SetupStatus): SetupStep[] {
  return status.report.steps.filter((s) => !SETTLED.has(s.state));
}

function describeSteps(steps: SetupStep[]): string {
  return steps
    .map(
      (s) =>
        `  ${s.id} [${s.state}${s.code ? `, ${s.code}` : ""}] ${s.summary ?? ""}` +
        (s.actions?.length ? ` (offers: ${s.actions.join(", ")})` : ""),
    )
    .join("\n");
}

/**
 * Waits for Studio's own check to finish (the wizard polls it while it runs).
 * A just-deployed app answers 502 from the Apps proxy until its server is up,
 * so failed requests count as "still starting" rather than failing the test.
 */
async function settledStatus(studio: StudioApi): Promise<SetupStatus> {
  let status: SetupStatus | undefined;
  await expect
    .poll(
      async () => {
        try {
          status = await studio.setupStatus();
        } catch (err) {
          return `unreachable (${(err as Error).message.split("\n")[0].slice(0, 80)})`;
        }
        return status.report.state;
      },
      {
        message: "Studio's setup check did not finish",
        intervals: [5_000],
        timeout: CHECK_TIMEOUT_MS,
      },
    )
    .toMatch(/^(setup_required|ready)$/);
  return status!;
}

function stepCard(page: Page, stepId: string) {
  const title = SETUP_COPY.setup.steps[stepId] ?? stepId;
  return page.locator("ol > li").filter({ has: page.getByText(title, { exact: true }) });
}

/** The step's card shows the backend's state badge and, when it has them, its GRANT statements. */
async function expectStepCard(page: Page, step: SetupStep): Promise<void> {
  const card = stepCard(page, step.id);
  await expect(card, `wizard shows the ${step.id} step`).toBeVisible();
  await expect(card.getByText(SETUP_COPY.setup.states[step.state], { exact: true }), `${step.id} badge`).toBeVisible();
  for (const statement of step.grant_statements ?? []) await expect(card.locator("pre")).toContainText(statement);
}

/** Runs one step's GRANT statements from the wizard and fails with Studio's explanation if any didn't run. */
async function runStepGrants(page: Page, step: SetupStep): Promise<void> {
  const card = stepCard(page, step.id);
  await expect(card, `wizard shows the ${step.id} step`).toBeVisible();
  const run = card.getByRole("button", { name: SETUP_COPY.sqlCodeBlock.run });
  const applied = page.waitForResponse(
    (res) => res.request().method() === "POST" && res.url().endsWith(`/api/v1/setup/steps/${step.id}/apply-grants`),
    { timeout: 180_000 },
  );
  await run.click();
  const res = await applied;
  expect(res.status(), `running the ${step.id} GRANT statements`).toBe(200);
  const body = (await res.json()) as {
    applied: number;
    total: number;
    failed: { statement: string; error_code: string }[];
  };
  if (body.failed.length) {
    const notice = (
      await card
        .getByRole("alert")
        .textContent()
        .catch(() => null)
    )?.trim();
    throw new Error(
      `Ran ${body.applied} of ${body.total} GRANT statements for ${step.id}:\n` +
        body.failed.map((f) => `  ${f.error_code}: ${f.statement}`).join("\n") +
        (notice ? `\nStudio says: ${notice}` : ""),
    );
  }
}

/** Clicks the wizard's own "Run setup" / "Verify again" button, then waits for the check to finish. */
async function verifyAgain(page: Page, studio: StudioApi): Promise<SetupStatus> {
  const { reconcile, verify_again: verify } = SETUP_COPY.setup.actions;
  const button = page.getByRole("button", { name: new RegExp(`^(${reconcile}|${verify})$`) }).first();
  await expect(button).toBeEnabled();
  const reconciled = page.waitForResponse(
    (res) => res.request().method() === "POST" && res.url().endsWith("/api/v1/setup/reconcile"),
    { timeout: CHECK_TIMEOUT_MS },
  );
  await button.click();
  expect((await reconciled).status(), "setup reconcile request").toBeLessThan(500);
  return settledStatus(studio);
}

// Fresh deployments spend minutes on their first check; grants and re-checks add more.
test.setTimeout(30 * 60_000);

test.describe.configure({ mode: "serial" });

test("administrator completes the setup wizard", async ({ page, api, studio, problems }) => {
  let status = await settledStatus(studio);
  expect(status.can_manage, "run the suite as a Studio administrator (workspace admins or DQX_ADMIN_GROUP)").toBe(true);

  for (let round = 1; status.report.state !== "ready"; round++) {
    const blocking = blockingSteps(status);
    await test.step(`round ${round}: ${blocking.map((s) => s.id).join(", ")}`, async () => {
      if (round > MAX_ROUNDS) {
        throw new Error(`Setup is still blocked after ${MAX_ROUNDS} rounds:\n${describeSteps(blocking)}`);
      }
      const withGrants = blocking.filter((s) => s.grant_statements?.length);
      const verifiable = blocking.some((s) => s.actions?.some((a) => a === "reconcile" || a === "verify_again"));
      if (!withGrants.length && !verifiable) {
        throw new Error(`Setup needs a manual fix the wizard can't apply here:\n${describeSteps(blocking)}`);
      }

      await page.goto("/");
      await expect(page.getByRole("heading", { name: SETUP_COPY.setup.title })).toBeVisible({ timeout: 60_000 });
      console.log(`Setup round ${round}, blocked on:\n${describeSteps(blocking)}`);

      // Until setup is ready, routes outside the setup allowlist are refused.
      const gated = await api.get("/api/v1/config/branding");
      expect(gated.status(), "routes outside setup answer 503 before setup is ready").toBe(503);
      expect(await gated.json()).toEqual({ detail: "DQX Studio setup is required." });

      for (const step of status.report.steps) await expectStepCard(page, step);
      const runner = stepCard(page, "task_runner");
      if (status.task_runner_job_id && !status.report.steps.some((s) => s.actions?.includes("choose_task_runner"))) {
        // The deployment chose the job's runner, so the step links straight to the job.
        await expect(runner.getByRole("link", { name: SETUP_COPY.setup.actions.openTaskRunnerJob })).toHaveAttribute(
          "href",
          new RegExp(`/jobs/${status.task_runner_job_id}$`),
        );
      }

      for (const step of withGrants) await runStepGrants(page, step);
      // Running grants re-verifies their step; only check again if something still blocks.
      status = await settledStatus(studio);
      if (status.report.state !== "ready") status = await verifyAgain(page, studio);
    });
  }

  await test.step("Studio opens once setup is ready", async () => {
    const warnings = status.report.steps.filter((s) => s.state === "warning");
    // Until setup is ready the backend gate answers 503 to most routes, including
    // ones the wizard page prefetches (branding); only a ready Studio must be clean.
    problems.drain();
    await page.goto("/");
    if (warnings.length) {
      // A fresh browser shows ready-with-warnings setups to administrators once.
      await expect(
        page.getByRole("heading", {
          name: SETUP_COPY.setup.warningsReview.title,
        }),
      ).toBeVisible();
      for (const step of warnings) await expect(stepCard(page, step.id)).toBeVisible();
      console.log(`Setup finished with warnings:\n${describeSteps(warnings)}`);
    }
    await openStudio(page, "/home");
    await expect(sidebarLinks(page).first()).toBeVisible();
    await problems.settle();
    expect(problems.drain()).toEqual([]);
  });

  expect(blockingSteps(await studio.setupStatus()), "every setup step is settled").toEqual([]);
});

test("setup page of a ready deployment matches the backend's report", async ({ page, studio }) => {
  const status = await settledStatus(studio);
  expect(status.report.state, "the previous test leaves setup ready").toBe("ready");
  const warnings = status.report.steps.filter((s) => s.state === "warning");
  test.skip(!warnings.length, "setup finished without warnings, so a ready Studio never shows the setup page");

  // Each test gets a new browser, so the review shows again even though the previous test acknowledged it.
  await page.goto("/");
  await expect(page.getByRole("heading", { name: SETUP_COPY.setup.warningsReview.title })).toBeVisible();

  await test.step("every step shows its state", async () => {
    for (const step of status.report.steps) await expectStepCard(page, step);
  });

  const configuration = status.configuration;
  if (configuration?.source === "deployment") {
    await test.step("configuration shows the deployed values and can't be edited", async () => {
      const card = stepCard(page, "configuration");
      for (const value of [configuration.catalog, configuration.prefix, configuration.audience_group]) {
        if (value) await expect(card.getByText(value, { exact: true })).toBeVisible();
      }
      await expect(card.getByRole("button", { name: SETUP_COPY.setup.configuration.submit })).toHaveCount(0);
    });
  }

  await test.step("warnings that only an administrator can confirm offer it, with an explanation", async () => {
    for (const step of warnings.filter((s) => s.actions?.includes("override"))) {
      const key = step.id in SETUP_COPY.setup.override.button ? step.id : "default";
      const card = stepCard(page, step.id);
      await expect(card.getByRole("button", { name: SETUP_COPY.setup.override.button[key] })).toBeEnabled();
      await expect(card.getByText(SETUP_COPY.setup.override.help[key])).toBeVisible();
    }
  });

  await test.step("acknowledging opens Studio, and the browser remembers it", async () => {
    await page.getByRole("button", { name: SETUP_COPY.setup.warningsReview.acknowledge }).click();
    await expect(sidebarLinks(page).first()).toBeVisible();
    await page.reload();
    await expect(sidebarLinks(page).first()).toBeVisible();
    await expect(page.getByRole("heading", { name: SETUP_COPY.setup.warningsReview.title })).toHaveCount(0);
  });
});
