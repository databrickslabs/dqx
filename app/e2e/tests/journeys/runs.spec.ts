import type { Page } from "@playwright/test";

import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import type { StudioApi } from "../../support/studio-api";
import { dialogWithHeading, expectToast, openStudio } from "../../support/ui";

// Journey: an approved table is run on demand, the run shows up in Runs
// History, and a reviewer triages it (review status + comments). The run is a
// real Databricks job over samples.tpch.supplier (10k rows, ~9% negative
// account balances), so one rule passes and one deliberately fails.

const TABLE = SAMPLE_TABLES.runs;

async function approvedSupplierTable(studio: StudioApi, workerIndex: number, cleanup: { rule(id: string): void; table(id: string): void }) {
  const present = await studio.createPublishedRule({
    name: e2eName("supplier-name-present", workerIndex),
    fn: "is_not_null",
    dimension: "Completeness",
  });
  cleanup.rule(present.rule_id);
  const nonNegative = await studio.createPublishedRule({
    name: e2eName("supplier-balance-not-negative", workerIndex),
    fn: "is_not_less_than",
    args: { limit: 0 },
    dimension: "Validity",
  });
  cleanup.rule(nonNegative.rule_id);

  const table = await studio.monitorTable(TABLE);
  cleanup.table(table.binding_id);
  await studio.applyRule(table.binding_id, present.rule_id, "s_name");
  await studio.applyRule(table.binding_id, nonNegative.rule_id, "s_acctbal");
  await studio.submitTable(table.binding_id);
  await studio.approveTable(table.binding_id);
  return { bindingId: table.binding_id, present, nonNegative };
}

/** The Runs History row for this table's most recent run. */
function latestRunRow(page: Page) {
  return page.getByRole("row").filter({ hasText: TABLE.split(".").pop()! }).first();
}

test.describe("Runs", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("run an approved table and triage the run", async ({ page, studio, cleanup }, testInfo) => {
    test.slow();
    test.setTimeout(15 * 60_000);
    const { bindingId, present, nonNegative } = await approvedSupplierTable(studio, testInfo.workerIndex, cleanup);

    await test.step("start a run from the table page", async () => {
      await openStudio(page, `/monitored-tables/${bindingId}`);
      // "Run now (v1)" runs the approved version; "Run draft" would be hidden from published results.
      await page.getByRole("group", { name: "Run this table's checks" }).getByRole("button", { name: /^Run now/ }).click();
      await expectToast(page, "Run started.");
    });

    await test.step("the run completes in Runs History", async () => {
      await openStudio(page, "/runs-history");
      await expect(latestRunRow(page)).toBeVisible();
      // The page polls every 5s while a run is RUNNING; the job takes about a minute.
      await expect(latestRunRow(page)).toContainText("Success", { timeout: 10 * 60_000 });
    });

    const results = page.getByRole("tabpanel", { name: "Results" });

    await test.step("the table's results show the failing and passing rule", async () => {
      await openStudio(page, `/monitored-tables/${bindingId}?tab=results`);
      // By-rule rows are clickable (role=button) but don't always expose a name; match the rule cell.
      const byRule = (name: string) =>
        results.getByRole("button").filter({ has: page.getByRole("cell", { name, exact: true }) });
      // The By rule table shows a capped number of rows; filter to one rule first.
      const searchRule = async (name: string) => {
        await results.getByPlaceholder("Search rule…").fill(name);
        return byRule(name);
      };
      const nonNegativeName = nonNegative.user_metadata.name as string;
      const presentName = present.user_metadata.name as string;
      // Scores are computed asynchronously after the run reports success; until
      // then the tab can still show an earlier run of the same table. Reload
      // until this run's own rules appear.
      await expect
        .poll(
          async () => {
            await page.reload();
            await expect(page.getByText("A run is in progress.")).toBeHidden({ timeout: 60_000 });
            await expect(results).toContainText("Overall score", { timeout: 30_000 });
            const row = await searchRule(nonNegativeName);
            return row
              .waitFor({ state: "visible", timeout: 10_000 })
              .then(() => true)
              .catch(() => false);
          },
          { message: "this run's rules never appeared in the table's results", intervals: [5_000], timeout: 5 * 60_000 },
        )
        .toBe(true);
      // Cells: rule, # checks, failed tests, total tests, score.
      const failedTests = (row: Awaited<ReturnType<typeof searchRule>>) => row.getByRole("cell").nth(2);
      // ~9% of suppliers have a negative balance; every supplier has a name.
      await expect(failedTests(await searchRule(nonNegativeName))).toHaveText(/^[1-9][\d,]*$/);
      const presentRow = await searchRule(presentName);
      await expect(failedTests(presentRow)).toHaveText("0");
      await expect(presentRow).toContainText("100.0%");
      await results.getByPlaceholder("Search rule…").fill("");
    });

    await test.step("a reviewer marks the run as confirmed", async () => {
      await results.getByRole("button", { name: "Pending review" }).click();
      // Options are buttons labelled "<status> <description>".
      await page.getByRole("button", { name: /^Confirmed Known issue/ }).click();
      await expectToast(page, 'Marked as "Confirmed"');
      await expect(results.getByRole("button", { name: "Confirmed" })).toBeVisible();
    });

    await test.step("a reviewer leaves a comment on the run, then deletes it", async () => {
      const note = `Negative balances are expected for suppliers in credit (${testInfo.title}).`;
      await results.getByRole("button", { name: "Show comments" }).click();
      const comments = dialogWithHeading(page, "Comments");
      await expect(comments).toContainText("No comments yet.");
      await comments.getByPlaceholder("Add a comment...").fill(note);
      await comments.getByPlaceholder("Add a comment...").press("ControlOrMeta+Enter");
      await expect(comments).toContainText(note);

      await comments.getByRole("button", { name: "Delete comment" }).click();
      const confirm = page.getByRole("alertdialog");
      await expect(confirm).toContainText("Delete this comment?");
      await confirm.getByRole("button", { name: "Delete" }).click();
      await expect(comments).toContainText("No comments yet.");
    });
  });
});
