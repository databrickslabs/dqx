import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { expectToast, openStudio } from "../../support/ui";

// Journey: an admin schedules an approved table's checks, pauses and resumes
// the schedule, then removes it. The schedule is set far enough out that it
// never fires during the test.

const TABLE = SAMPLE_TABLES.schedules;

test.describe("Schedules", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("schedule a table, pause and resume it, remove it", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(180_000);
    const rule = await studio.createPublishedRule({
      name: e2eName("customer-key-present", testInfo.workerIndex),
      fn: "is_not_null",
      dimension: "Completeness",
    });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "c_custkey" }]);
    cleanup.table(table.binding_id);

    cleanup.defer(`clear schedule on ${TABLE}`, () => studio.clearTableSchedule(table.binding_id));
    const tableName = TABLE.split(".").pop()!;

    await test.step("create a daily schedule for the table", async () => {
      await openStudio(page, "/schedules");
      await page.getByRole("button", { name: "New schedule" }).or(page.getByRole("link", { name: "New schedule" })).first().click();
      await expect(page.getByRole("heading", { name: "New schedule" })).toBeVisible();
      await page.getByRole("button", { name: "Add table" }).click();
      const picker = page.getByRole("dialog", { name: "Add table" });
      await picker.getByRole("textbox", { name: "Search tables" }).fill(tableName);
      await picker.getByRole("row").filter({ hasText: TABLE }).click();
      await expect(picker).toBeHidden();
      // Defaults: data quality only, full table, every day at 06:00 UTC.
      await expect(page.getByRole("combobox", { name: "What this schedule runs" })).toHaveText("Data quality only");
      await page.getByRole("button", { name: "Save schedule" }).click();
      await expectToast(page, "Schedule saved.");
      await expect.poll(async () => (await studio.tableSchedule(table.binding_id))?.cron ?? null).not.toBeNull();
    });

    const row = page.getByRole("row").filter({ hasText: tableName });

    await test.step("the schedule is listed as active", async () => {
      await openStudio(page, "/schedules");
      await expect(row).toContainText("Active");
    });

    await test.step("pause and resume it", async () => {
      await row.getByRole("button", { name: "Pause" }).click();
      await expectToast(page, "Schedule paused");
      await expect(row).toContainText("Paused");
      expect((await studio.tableSchedule(table.binding_id))?.paused).toBe(true);

      await row.getByRole("button", { name: "Unpause" }).click();
      await expectToast(page, "Schedule unpaused");
      await expect(row).toContainText("Active");
      expect((await studio.tableSchedule(table.binding_id))?.paused).toBe(false);
    });

    await test.step("remove the schedule", async () => {
      await row.getByRole("button", { name: /Delete|Remove/ }).first().click();
      const confirm = page.getByRole("alertdialog");
      await expect(confirm).toContainText(tableName);
      await confirm.getByRole("button", { name: "Delete schedule" }).click();
      await expectToast(page, "Schedule removed");
      await expect(row).toHaveCount(0);
      await expect.poll(async () => (await studio.tableSchedule(table.binding_id))?.cron ?? null).toBeNull();
    });
  });
});
