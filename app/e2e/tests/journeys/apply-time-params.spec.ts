import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { openStudio } from "../../support/ui";

// Journey (#1663): Variable Parameters — empty params stay fillable at apply
// time; filled params stay locked (hidden on the apply page) unless the author
// unlocked them.

const TABLE = SAMPLE_TABLES.applyTimeParams;

function mappingKey(slot: string, column: string): string {
  return JSON.stringify({ [slot]: column });
}

test.describe("Apply-time Variable Parameters", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("fill an empty parameter at apply time; hide a locked filled default", async ({
    page,
    studio,
    cleanup,
  }, testInfo) => {
    test.setTimeout(180_000);
    const ruleName = e2eName("demo-sk-max-length", testInfo.workerIndex);

    // Empty {{max_length}} → always overridable. Filled {{note}} without an
    // overridable_parameters entry → locked and hidden on the apply page.
    const rule = await studio.createPublishedSqlRule({
      name: ruleName,
      predicate: "length(cast({{demo_sk}} as string)) <= {{max_length}}",
      slots: [{ name: "demo_sk", family: "numeric" }],
      parameters: [
        { name: "max_length", type: "integer", value: null },
        { name: "note", type: "string", value: "registry-default" },
      ],
    });
    cleanup.rule(rule.rule_id);

    const table = await studio.monitorTable(TABLE);
    cleanup.table(table.binding_id);

    await test.step("apply the rule and map the column", async () => {
      await studio.applyRuleWithMapping(table.binding_id, rule.rule_id, { demo_sk: "cd_demo_sk" });
    });

    await test.step("the apply page shows the empty parameter and hides the locked one", async () => {
      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=apply-rules`);
      await expect(page.getByText(ruleName)).toBeVisible({ timeout: 60_000 });
      // Empty → shown (no padlock on this page). Exact match avoids the SQL preview.
      await expect(page.getByText("{{max_length}}", { exact: true })).toBeVisible();
      await expect(page.getByLabel("max_length")).toBeVisible();
      // Filled + locked → hidden.
      await expect(page.getByText("{{note}}", { exact: true })).toHaveCount(0);
      await expect(page.getByLabel("note")).toHaveCount(0);
    });

    await test.step("persist an apply-time value via saveAppliedRules", async () => {
      const key = mappingKey("demo_sk", "cd_demo_sk");
      await studio.saveAppliedRules(table.binding_id, [
        {
          rule_id: rule.rule_id,
          column_mapping: [{ demo_sk: "cd_demo_sk" }],
          parameter_values: { [key]: { max_length: 12 } },
        },
      ]);

      const detail = await studio.getMonitoredTableDetail(table.binding_id);
      const applied = detail.applied_rules.find((r) => r.rule_id === rule.rule_id);
      expect(applied?.parameter_values?.[key]?.max_length).toBe(12);
    });

    await test.step("an unlocked filled default can be overridden", async () => {
      const unlockedName = e2eName("note-overridable", testInfo.workerIndex);
      const unlocked = await studio.createPublishedSqlRule({
        name: unlockedName,
        predicate: "{{demo_sk}} IS NOT NULL",
        slots: [{ name: "demo_sk", family: "numeric" }],
        parameters: [{ name: "note", type: "string", value: "registry-default" }],
        overridableParameters: ["note"],
      });
      cleanup.rule(unlocked.rule_id);

      const key = mappingKey("demo_sk", "cd_demo_sk");
      // saveAppliedRules replaces the full set — re-include the first rule.
      await studio.saveAppliedRules(table.binding_id, [
        {
          rule_id: rule.rule_id,
          column_mapping: [{ demo_sk: "cd_demo_sk" }],
          parameter_values: { [key]: { max_length: 12 } },
        },
        {
          rule_id: unlocked.rule_id,
          column_mapping: [{ demo_sk: "cd_demo_sk" }],
          parameter_values: { [key]: { note: "apply-override" } },
        },
      ]);

      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=apply-rules`);
      await expect(page.getByText(unlockedName)).toBeVisible({ timeout: 60_000 });
      await expect(page.getByText("{{note}}", { exact: true })).toBeVisible();
      await expect(page.getByLabel("note")).toHaveValue("apply-override");
    });
  });
});
