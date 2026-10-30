import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { dialogWithHeading, expectToast, openStudio, pickFromCombobox } from "../../support/ui";

// Journey: a data owner starts monitoring a table, applies published registry
// rules to its columns, submits the table for review, an approver approves it,
// and the owner later stops monitoring it.

const TABLE = SAMPLE_TABLES.monitoredTablesUi;
const [CATALOG, SCHEMA, TABLE_NAME] = TABLE.split(".");

test.describe("Monitored tables", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("monitor a table, apply a rule, get it approved, stop monitoring", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(180_000);
    const ruleName = e2eName("nation-name-present", testInfo.workerIndex);
    const rule = await studio.createPublishedRule({ name: ruleName, fn: "is_not_null", dimension: "Completeness" });
    cleanup.rule(rule.rule_id);
    let bindingId = "";

    await test.step("register the table from the catalog browser", async () => {
      await openStudio(page, "/monitored-tables");
      await page.getByRole("button", { name: "Monitor a table" }).first().click();
      const dialog = dialogWithHeading(page, "Monitor tables");
      await expect(dialog).toBeVisible();
      await pickFromCombobox(page, "Select catalogs…", CATALOG);
      await pickFromCombobox(page, "Select schemas…", SCHEMA);
      await pickFromCombobox(page, "Select tables…", TABLE_NAME);
      await dialog.getByRole("button", { name: "Add 1 table" }).click();
      await expectToast(page, "Table registered.");
      await page.waitForURL(/\/monitored-tables\/[^/?]+\?.*tab=apply-rules/);
      bindingId = new URL(page.url()).pathname.split("/").pop()!;
      cleanup.table(bindingId);
      expect((await studio.getMonitoredTable(bindingId)).table_fqn).toBe(TABLE);
    });

    await test.step("apply the published rule", async () => {
      await page.getByRole("button", { name: "Apply rules" }).first().click();
      const dialog = dialogWithHeading(page, "Add a rule");
      await dialog.getByPlaceholder("Search published rules…").fill(ruleName);
      await dialog.getByText(ruleName).first().click();
      await dialog.getByRole("button", { name: "Apply rule" }).click();
      await expectToast(page, "Rule applied.");
    });

    await test.step("map the rule's column slot to n_name", async () => {
      const tab = page.getByRole("tabpanel", { name: "Apply Rules" });
      await expect(tab).toContainText("0 checks via 1 rule");
      const columnPicker = tab.getByRole("button", { name: "Select a column…" });
      if ((await columnPicker.getAttribute("aria-expanded")) !== "true") await columnPicker.click();
      await page.getByRole("option", { name: /^n_name\b/ }).click();
      await expect(tab).toContainText("1 check via 1 rule");
    });

    await test.step("submit the table for review", async () => {
      await page.getByRole("button", { name: "Submit for review" }).click();
      await expectToast(page, "Table submitted for review.");
      await expect(page.getByText("Pending approval").first()).toBeVisible();
      await expect.poll(async () => (await studio.getMonitoredTable(bindingId)).status).toBe("pending_approval");
    });

    await test.step("an approver approves it in Review & Approve", async () => {
      await openStudio(page, "/rules/drafts");
      const row = page.getByRole("row").filter({ hasText: TABLE });
      await expect(row).toBeVisible();
      await row.getByRole("button", { name: "Approve" }).click();
      await expectToast(page, /^Table approved/);
      await expect.poll(async () => (await studio.getMonitoredTable(bindingId)).status).toBe("approved");
    });

    await test.step("the approved table shows its check", async () => {
      await openStudio(page, `/monitored-tables/${bindingId}?tab=apply-rules`);
      await expect(page.getByRole("tabpanel", { name: "Apply Rules" })).toContainText(ruleName);
      const detail = await studio.getMonitoredTableDetail(bindingId);
      expect(detail.applied_rules.map((a) => a.rule_id)).toEqual([rule.rule_id]);
    });

    await test.step("stop monitoring the table", async () => {
      await page.getByRole("button", { name: "Actions" }).click();
      await page.getByRole("menuitem", { name: "Delete" }).click();
      const confirm = page.getByRole("alertdialog");
      await expect(confirm).toContainText("Stop monitoring this table?");
      await confirm.getByRole("button", { name: "Delete" }).click();
      await expectToast(page, "Table deleted.");
      expect(await studio.findMonitoredTable(TABLE)).toBeUndefined();
    });
  });
});
