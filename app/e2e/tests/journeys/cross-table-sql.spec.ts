import { expect, test } from "../../fixtures";
import { knownBug } from "../../support/known-bugs";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import type { StudioApi } from "../../support/studio-api";
import { openStudio, saveAndSubmitNewRule, startNewRule } from "../../support/ui";

// Journeys for cross-table SQL checks: rules with no column variables whose
// query spans other tables. Studio stores them under the synthetic table name
// "__sql_check__/<rule name>" and runs them alongside the table they're applied
// to. A dataset-level query returns one boolean "condition" row — true means
// the data fails. Here: every nation's region must be one of regions 0–2,
// which is false for the 10 nations in regions 3 and 4, so the check fails.

const HOST = SAMPLE_TABLES.crossTableHost;
const QUERY =
  "SELECT COUNT(*) > 0 AS condition FROM samples.tpch.nation n " +
  "WHERE n.n_regionkey NOT IN (SELECT r_regionkey FROM samples.tpch.region WHERE r_regionkey < 3)";

/** Monitors the host table with one ordinary check plus the cross-table rule (left in draft). */
async function hostWithCrossTableRule(studio: StudioApi, crossTableRuleId: string, workerIndex: number, cleanup: { rule(id: string): void; table(id: string): void }) {
  const keyRule = await studio.createPublishedRule({ name: e2eName("item-key-present", workerIndex), fn: "is_not_null" });
  cleanup.rule(keyRule.rule_id);
  const table = await studio.monitorTable(HOST);
  cleanup.table(table.binding_id);
  await studio.applyRule(table.binding_id, keyRule.rule_id, "i_item_sk");
  await studio.applyRuleWithMapping(table.binding_id, crossTableRuleId, {});
  return table.binding_id;
}

/** Runs the table from its page with the given run button and returns the new run's id. */
async function runFromTablePage(page: import("@playwright/test").Page, studio: StudioApi, bindingId: string, button: RegExp) {
  const before = await studio.runIdsFor(HOST);
  await openStudio(page, `/monitored-tables/${bindingId}`);
  await page.getByRole("group", { name: "Run this table's checks" }).getByRole("button", { name: button }).click();
  await expect.poll(async () => (await studio.listRuns()).find((r) => r.source_table_fqn === HOST && !before.has(r.run_id))?.run_id).toBeTruthy();
  return (await studio.listRuns()).find((r) => r.source_table_fqn === HOST && !before.has(r.run_id))!.run_id;
}

test.describe("Cross-table SQL checks", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("write a cross-table SQL check and see it fail on a draft run", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    const name = e2eName("nations-in-known-regions", testInfo.workerIndex);

    const ruleId = await test.step("write the query as a dataset-level SQL rule", async () => {
      const panel = await startNewRule(page, {
        name,
        description: "Every nation belongs to one of the first three regions.",
        dimension: "Consistency",
        severity: "High",
        check: "__sql_condition__",
      });
      // No column variables: the query names its own tables.
      await panel.getByRole("button", { name: "Remove" }).first().click();
      await panel.locator(".cm-content").first().click();
      await page.keyboard.type(QUERY);
      await panel.getByRole("radio", { name: "Dataset-level" }).click();
      const id = await saveAndSubmitNewRule(page);
      cleanup.rule(id);
      const rule = await studio.getRule(id);
      expect(rule.mode).toBe("sql");
      expect(rule.definition.slots).toEqual([]);
      expect(rule.definition.body.sql_query).toBe(QUERY);
      return id;
    });

    await test.step("publish it and run it with a table", async () => {
      await studio.approveRule(ruleId);
      const bindingId = await hostWithCrossTableRule(studio, ruleId, testInfo.workerIndex, cleanup);
      const runId = await runFromTablePage(page, studio, bindingId, /^Run draft$/);
      cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
      expect(await studio.waitForRunStatus(runId)).toBe("SUCCESS");
      // A failing dataset-level check flags every row of the table it runs with.
      const results = await studio.runResults(runId);
      expect(results.error_summary.map((e) => e.error)).toContain(name);
    });
  });

  test("an approved table runs its cross-table checks", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    const name = e2eName("nations-in-known-regions-approved", testInfo.workerIndex);
    const rule = await studio.createPublishedSqlRule({ name, sqlQuery: QUERY });
    cleanup.rule(rule.rule_id);
    const bindingId = await hostWithCrossTableRule(studio, rule.rule_id, testInfo.workerIndex, cleanup);
    await studio.submitTable(bindingId);
    await studio.approveTable(bindingId);

    const runId = await runFromTablePage(page, studio, bindingId, /^Run now/);
    cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
    expect(await studio.waitForRunStatus(runId)).toBe("SUCCESS");
    const results = await studio.runResults(runId);

    // Known Studio bug: cross-table checks applied to a table are run by "Run
    // draft" but dropped from the approved version, so "Run now" (and scheduled
    // runs) silently skip them — and when they're a table's only checks, "Run
    // now" fails with "No checks resolved for binding … (source=approved)".
    await knownBug("approved runs skip the table's cross-table SQL checks", async () => {
      expect(results.error_summary.map((e) => e.error)).toContain(name);
    });
  });
});
