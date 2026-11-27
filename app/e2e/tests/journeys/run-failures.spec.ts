import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import type { StudioApi } from "../../support/studio-api";
import { knownBug } from "../../support/known-bugs";
import { openStudio } from "../../support/ui";

// Journeys for runs that go wrong: a rule whose SQL can't be evaluated, and a
// run the owner wants to stop. Tests in this file share their table and run in
// order.

const TABLE = SAMPLE_TABLES.runFailures;

/** A published SQL rule whose predicate references a column income_band doesn't have, applied and approved. */
async function brokenRuleTable(studio: StudioApi, label: string, workerIndex: number, cleanup: { rule(id: string): void; table(id: string): void }) {
  const rule = await studio.createPublishedSqlRule({
    name: e2eName(label, workerIndex),
    predicate: "{{lower}} <= no_such_column",
    slots: [{ name: "lower", family: "numeric" }],
  });
  cleanup.rule(rule.rule_id);
  const table = await studio.monitorTable(TABLE);
  cleanup.table(table.binding_id);
  await studio.applyRule(table.binding_id, rule.rule_id, "ib_lower_bound", "lower");
  await studio.submitTable(table.binding_id);
  await studio.approveTable(table.binding_id);
  return { rule, bindingId: table.binding_id };
}

test.describe("Run failures", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("a rule whose SQL can't be evaluated explains itself in the failed records", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    const { rule, bindingId } = await brokenRuleTable(studio, "broken-sql-explained", testInfo.workerIndex, cleanup);
    const runId = await studio.runTable(bindingId);
    cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
    await studio.waitForRunStatus(runId);

    await openStudio(page, `/monitored-tables/${bindingId}?tab=results`);
    const results = page.getByRole("tabpanel", { name: "Results" });
    await expect(page.getByText("A run is in progress.")).toBeHidden({ timeout: 60_000 });
    // DQX doesn't stop on an invalid expression: it flags every row instead.
    await expect(results.getByText(/^Showing 1–20 of 20 failed records\./)).toBeVisible({ timeout: 60_000 });
    const headers = await results.locator("table").last().locator("thead th").allTextContents();
    const lower = headers.findIndex((h) => h.trim() === "ib_lower_bound");
    await results.locator("table").last().locator("tbody tr").first().locator("td").nth(lower).hover();
    const tooltip = page.getByRole("tooltip");
    await expect(tooltip).toContainText(rule.user_metadata.name as string);
    await expect(tooltip).toContainText("Check evaluation skipped due to invalid sql expression");
  });

  test("a run whose checks can't be evaluated isn't reported as a success", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    const { bindingId } = await brokenRuleTable(studio, "broken-sql-status", testInfo.workerIndex, cleanup);
    const before = await studio.runIdsFor(TABLE);
    await openStudio(page, `/monitored-tables/${bindingId}`);
    await page.getByRole("group", { name: "Run this table's checks" }).getByRole("button", { name: /^Run now/ }).click();
    await expect.poll(async () => (await studio.listRuns()).find((r) => r.source_table_fqn === TABLE && !before.has(r.run_id))?.run_id).toBeTruthy();
    const runId = (await studio.listRuns()).find((r) => r.source_table_fqn === TABLE && !before.has(r.run_id))!.run_id;
    cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
    await studio.waitForRunStatus(runId);

    await openStudio(page, "/runs-history");
    await expect(page.getByText("Loading...")).toBeHidden({ timeout: 60_000 });
    const row = page.getByRole("row").filter({ hasText: TABLE }).first();
    await expect(row).toBeVisible();
    // Known Studio behaviour to decide on: a check DQX can't evaluate (invalid
    // SQL, or a mapped column the table doesn't have) is accepted at create,
    // apply and approve, and the run then reports Success while flagging every
    // row. Nothing marks the run or the rule as broken.
    await knownBug("a run whose checks can't be evaluated is reported as Success", async () => {
      await expect(row).not.toContainText("Success", { timeout: 5_000 });
    });
  });

  test("a running table run can be cancelled", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(5 * 60_000);
    const rule = await studio.createPublishedRule({ name: e2eName("band-key-present", testInfo.workerIndex), fn: "is_not_null" });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "ib_income_band_sk" }]);
    cleanup.table(table.binding_id);
    const runId = await studio.runTable(table.binding_id);
    cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));

    await openStudio(page, "/runs-history");
    const row = page.getByRole("row").filter({ hasText: TABLE }).first();
    await expect(row).toContainText("Running");
    // Known gap: POST /api/v1/dryrun/runs/{id}/cancel exists and the strings are
    // translated (runsHistory.cancelDryRun), but no Runs History or table-page
    // control calls it, so a user can't stop a validation run.
    await knownBug("a running validation run has no cancel control", async () => {
      await row.getByRole("button", { name: /Cancel|Stop/ }).click({ timeout: 10_000 });
      await expect(row).toContainText("Canceled");
    });
  });
});
