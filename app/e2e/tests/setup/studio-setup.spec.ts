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

const SETUP_COPY = JSON.parse(
  readFileSync(
    join(__dirname, "..", "..", "..", "src", "databricks_labs_dqx_app", "ui", "lib", "i18n", "locales", "en.json"),
    "utf8",
  ),
) as {
  setup: {
    title: string;
    steps: Record<string, string>;
    actions: Record<string, string>;
    warningsReview: { title: string };
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

test("administrator completes the setup wizard", async ({ page, studio, problems }) => {
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
