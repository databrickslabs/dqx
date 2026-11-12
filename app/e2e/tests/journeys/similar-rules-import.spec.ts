import { expect, test } from "../../fixtures";
import { e2eName, E2E_TAG, runTag } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { expectToast, openStudio } from "../../support/ui";

// Journey (#1662): YAML import reuses a similar existing rule (same check,
// different settings), strips custom tags from the registry rule, and keeps
// those tags / a row_filter override on the applied rule instead.

const TABLE = SAMPLE_TABLES.similarRulesImport;

test.describe("Similar-rule import and applied overrides", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("reuse a similar rule on import and keep custom tags on the applied rule", async ({
    page,
    studio,
    cleanup,
  }, testInfo) => {
    test.setTimeout(180_000);
    const existingName = e2eName("status-allowed-free-pro", testInfo.workerIndex);
    const importName = e2eName("status-allowed-free-trial", testInfo.workerIndex);
    const tagKey = `team_${runTag().replace(/[^a-z0-9]/gi, "")}`;
    const tagValue = "e2e-import";

    const existing = await studio.createPublishedRule({
      name: existingName,
      fn: "is_in_list",
      parameters: [{ name: "allowed", type: "list", value: [`${runTag()}-free`, `${runTag()}-pro`] }],
      filter: "1 = 1",
      dimension: "Validity",
    });
    cleanup.rule(existing.rule_id);

    await test.step("import YAML that matches the same check with different allowed values", async () => {
      const yaml = `- criticality: error
  user_metadata:
    name: ${importName}
    dimension: Validity
    severity: High
    ${tagKey}: ${tagValue}
    ${E2E_TAG}: "${runTag()}"
  check:
    function: is_in_list
    arguments:
      column: "{{column}}"
      allowed:
        - "${runTag()}-free"
        - "${runTag()}-trial"`;

      await openStudio(page, "/registry-rules/import");
      await expect(page.getByText("Import from YAML")).toBeVisible();
      await page.locator("textarea").first().fill(yaml);
      await expect(page.getByText("Loaded 1 checks from YAML").or(page.getByText("Loaded 1 check from YAML"))).toBeVisible();

      // Similar-rule lookup is debounced after parse. Different allowed values
      // are a settings conflict, so reuse is offered unchecked — tick it.
      await expect(page.getByText(/imported rule matches an existing rule/)).toBeVisible({ timeout: 15_000 });
      const reuse = page.getByRole("checkbox", { name: new RegExp(`Reuse ${existingName}`) });
      await expect(reuse).toBeVisible();
      if (!(await reuse.isChecked())) await reuse.check();

      await page.getByRole("button", { name: "Save as drafts" }).click();
      await expectToast(page, /Reused \d+ existing rule/);
    });

    await test.step("the import reused the existing rule and did not create a duplicate", async () => {
      expect(await studio.findRuleByName(importName)).toBeUndefined();
      const reused = await studio.getRule(existing.rule_id);
      expect(reused.user_metadata.name).toBe(existingName);
      // Custom tags stay off the reusable registry rule.
      expect(reused.user_metadata[tagKey]).toBeUndefined();
    });

    await test.step("custom tags and a row_filter override live on the applied rule", async () => {
      const table = await studio.monitorTable(TABLE);
      cleanup.table(table.binding_id);
      await studio.applyRule(table.binding_id, existing.rule_id, "ps_availqty", "column", {
        tags: { [tagKey]: tagValue },
        row_filter: "ps_availqty > 0",
      });

      const detail = await studio.getMonitoredTableDetail(table.binding_id);
      const applied = detail.applied_rules.find((r) => r.rule_id === existing.rule_id);
      expect(applied, "applied rule").toBeDefined();
      expect(applied!.user_metadata?.[tagKey]).toBe(tagValue);
      expect(applied!.row_filter ?? applied!.user_metadata?.row_filter).toBeTruthy();

      const registry = await studio.getRule(existing.rule_id);
      expect(registry.user_metadata[tagKey]).toBeUndefined();
      expect(registry.definition.filter).toBe("1 = 1");
    });
  });
});
