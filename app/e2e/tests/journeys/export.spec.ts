import { readFileSync } from "node:fs";

import type { Download, Page } from "@playwright/test";

import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { dialogWithHeading, expectToast, openStudio } from "../../support/ui";

// Journey: a data engineer exports approved checks to take them into a
// pipeline — a table's checks as DQX YAML and as an ODCS data contract, and a
// single rule from the registry — then checks the DQX file imports back.

const TABLE = SAMPLE_TABLES.export;
// Export filenames slugify the table name ("date_dim" -> "date-dim").
const FILE_STEM = TABLE.split(".").pop()!.replace(/_/g, "-");

/** Opens the page's Actions menu, picks "Export…", optionally a format, and returns the download. */
async function exportFrom(page: Page, format?: "DQX YAML" | "Data contract (ODCS)"): Promise<{ file: Download; text: string }> {
  await page.getByRole("button", { name: "Actions" }).click();
  const downloaded = page.waitForEvent("download");
  await page.getByRole("menuitem", { name: /^Export/ }).click();
  // Pages with a single format (the registry) download straight away.
  if (format) await dialogWithHeading(page, "Export").getByRole("button", { name: format }).click();
  const file = await downloaded;
  await expectToast(page, `Exported ${file.suggestedFilename()}`);
  return { file, text: readFileSync(await file.path(), "utf8") };
}

test.describe("Export", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("export a table's checks and a rule, and import the export back", async ({ page, studio, cleanup }, testInfo) => {
    const ruleName = e2eName("date-key-present", testInfo.workerIndex);
    const rule = await studio.createPublishedRule({ name: ruleName, fn: "is_not_null", dimension: "Completeness" });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "d_date_sk" }]);
    cleanup.table(table.binding_id);
    let dqxYaml = "";

    await test.step("export the table's checks as DQX YAML", async () => {
      await openStudio(page, `/monitored-tables/${table.binding_id}`);
      const { file, text } = await exportFrom(page, "DQX YAML");
      expect(file.suggestedFilename()).toBe(`${FILE_STEM}.dqx.yaml`);
      expect(text).toContain("is_not_null");
      expect(text).toContain("d_date_sk");
      expect(text).toContain(ruleName);
      dqxYaml = text;
    });

    await test.step("export the table as an ODCS data contract", async () => {
      const { file, text } = await exportFrom(page, "Data contract (ODCS)");
      expect(file.suggestedFilename()).toBe(`${FILE_STEM}.odcs.yaml`);
      expect(text).toMatch(/^kind: DataContract$/m);
      expect(text).toContain(TABLE);
      expect(text).toContain("d_date_sk");
    });

    await test.step("export the rule from the registry", async () => {
      await openStudio(page, `/registry-rules/${rule.rule_id}`);
      const { file, text } = await exportFrom(page);
      expect(file.suggestedFilename()).toMatch(/^rule_.+\.dqx\.yaml$/);
      expect(text).toContain("is_not_null");
      expect(text).toContain(ruleName);
    });

    await test.step("the table's DQX export validates on Import", async () => {
      await openStudio(page, "/registry-rules/import");
      await page.locator("textarea").first().fill(dqxYaml);
      await expect(page.getByText(/^Loaded 1 checks? from YAML$/)).toBeVisible();
      await page.getByRole("button", { name: "Validate" }).click();
      await expectToast(page, "All checks are valid");
    });
  });
});
