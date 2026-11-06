import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { chooseSelect, openStudio, saveAndSubmitNewRule, startNewRule } from "../../support/ui";

// Journey: an author writes rules that aren't built-in checks — one with the
// visual condition builder, one in SQL — publishes them, applies them to a
// table and runs it. samples.tpcds_sf1.store has 12 stores with employee
// counts and floor space.

const TABLE = SAMPLE_TABLES.customRules;

test.describe("Custom rules", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("write a visual-builder rule and a SQL rule, then run both", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    const lowcodeName = e2eName("store-has-staff", testInfo.workerIndex);
    const sqlName = e2eName("store-floor-space-non-negative", testInfo.workerIndex);

    const lowcodeId = await test.step("build '{{employees}} > 0' with the visual builder", async () => {
      const panel = await startNewRule(page, {
        name: lowcodeName,
        description: "Every store has staff.",
        dimension: "Validity",
        severity: "High",
        check: "__custom_condition__",
      });
      await panel.getByRole("textbox", { name: "Variable name" }).fill("employees");
      await chooseSelect(panel, "Any", "Numeric");
      // The first condition row starts as "{{employees}} is null".
      await panel.getByRole("button", { name: "is null" }).first().click();
      await page.keyboard.type(">");
      await page.locator("[cmdk-item]").filter({ hasText: /^\s*>\s*$/ }).first().click();
      await panel.getByRole("group").first().getByRole("spinbutton").fill("0");
      const id = await saveAndSubmitNewRule(page);
      cleanup.rule(id);
      return id;
    });

    const sqlId = await test.step("write '{{floor_space}} >= 0' in SQL", async () => {
      const panel = await startNewRule(page, {
        name: sqlName,
        description: "Floor space is never negative.",
        dimension: "Validity",
        severity: "High",
        check: "__sql_condition__",
      });
      await panel.getByRole("textbox", { name: "Variable name" }).fill("floor_space");
      await chooseSelect(panel, "Any", "Numeric");
      await panel.locator(".cm-content").first().click();
      await page.keyboard.type("{{floor_space}} >= 0");
      await expect(panel.getByRole("radio", { name: "PASSES" })).toHaveAttribute("aria-checked", "true");
      const id = await saveAndSubmitNewRule(page);
      cleanup.rule(id);
      return id;
    });

    await test.step("both are stored as written", async () => {
      const lowcode = await studio.getRule(lowcodeId);
      expect(lowcode.mode).toBe("lowcode");
      expect(JSON.stringify(lowcode.definition.body)).toContain("{{employees}}");
      const sql = await studio.getRule(sqlId);
      expect(sql.mode).toBe("sql");
      expect(sql.definition.body.predicate).toBe("{{floor_space}} >= 0");
    });

    await test.step("publish, apply and run them on a table", async () => {
      for (const id of [lowcodeId, sqlId]) await studio.approveRule(id);
      const table = await studio.monitorTable(TABLE);
      cleanup.table(table.binding_id);
      await studio.applyRule(table.binding_id, lowcodeId, "s_number_employees", "employees");
      await studio.applyRule(table.binding_id, sqlId, "s_floor_space", "floor_space");
      await studio.submitTable(table.binding_id);
      await studio.approveTable(table.binding_id);
      const runId = await studio.runTable(table.binding_id);
      cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
      expect(await studio.waitForRunStatus(runId)).toBe("SUCCESS");

      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=results`);
      const results = page.getByRole("tabpanel", { name: "Results" });
      await expect(page.getByText("A run is in progress.")).toBeHidden({ timeout: 60_000 });
      for (const name of [lowcodeName, sqlName]) {
        await results.getByPlaceholder("Search rule…").fill(name);
        await expect(results.getByRole("cell", { name, exact: true }), name).toBeVisible({ timeout: 60_000 });
      }
    });
  });
});
