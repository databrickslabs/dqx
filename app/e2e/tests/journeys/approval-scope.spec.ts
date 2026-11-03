import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { expectToast, openStudio } from "../../support/ui";

// Journey: approving one table leaves every other approved table's checks in
// place. Regression for #1639 (fixed in Studio 0.1.2), where approving table B
// deleted table A's materialized checks: A still showed "approved", but its runs
// had nothing to check.

const [TABLE_A, TABLE_B] = SAMPLE_TABLES.approvalScope;

test.describe("Approval scope", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("approving a second table keeps the first table's checks", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    // n_nationkey 0-4 and r_regionkey 0-4 are below the limit: 5 failing rows in each table.
    const rule = await studio.createPublishedRule({
      name: e2eName("key-at-least-5", testInfo.workerIndex),
      fn: "is_not_less_than",
      args: { limit: 5 },
      dimension: "Validity",
    });
    cleanup.rule(rule.rule_id);

    const tableA = await studio.approvedTable(TABLE_A, [{ ruleId: rule.rule_id, column: "n_nationkey" }]);
    cleanup.table(tableA.binding_id);

    await test.step("submit a second table and approve it in Review & Approve", async () => {
      const tableB = await studio.monitorTable(TABLE_B);
      cleanup.table(tableB.binding_id);
      await studio.applyRule(tableB.binding_id, rule.rule_id, "r_regionkey");
      await studio.submitTable(tableB.binding_id);

      await openStudio(page, "/rules/drafts");
      const row = page.getByRole("row").filter({ hasText: TABLE_B });
      await expect(row).toBeVisible();
      await row.getByRole("button", { name: "Approve" }).click();
      await expectToast(page, /^Table approved/);
      await expect.poll(async () => (await studio.getMonitoredTable(tableB.binding_id)).status).toBe("approved");
    });

    await test.step("the first table still runs its approved checks", async () => {
      expect((await studio.getMonitoredTable(tableA.binding_id)).status).toBe("approved");
      const runId = await studio.runTable(tableA.binding_id);
      cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
      expect(await studio.waitForRunStatus(runId)).toBe("SUCCESS");
      const results = await studio.runResults(runId);
      expect(results.total_rows).toBe(25);
      expect(results.invalid_rows).toBe(5);
    });
  });
});
