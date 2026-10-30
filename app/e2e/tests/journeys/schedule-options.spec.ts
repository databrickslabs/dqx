import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { expectToast, openStudio } from "../../support/ui";

// Journey: an owner schedules profiling as well as data quality, on a random
// sample rather than the full table, and the choices stick. The default daily
// 06:00 UTC slot is never reached during the test.

const TABLE = SAMPLE_TABLES.scheduleOptions;

test.describe("Schedule options", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("schedule profiling and data quality on a 500-row sample", async ({ page, studio, cleanup }, testInfo) => {
    const rule = await studio.createPublishedRule({ name: e2eName("page-key-present", testInfo.workerIndex), fn: "is_not_null" });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "cp_catalog_page_sk" }]);
    cleanup.table(table.binding_id);
    cleanup.defer(`clear schedule on ${TABLE}`, () => studio.clearTableSchedule(table.binding_id));
    const tableName = TABLE.split(".").pop()!;

    await openStudio(page, "/schedules/new");
    await page.getByRole("button", { name: "Add table" }).click();
    const picker = page.getByRole("dialog", { name: "Add table" });
    await picker.getByRole("textbox", { name: "Search tables" }).fill(tableName);
    await picker.getByRole("row").filter({ hasText: TABLE }).click();

    await page.getByRole("combobox", { name: "What this schedule runs" }).click();
    await page.getByRole("option", { name: "Profiling + data quality" }).click();
    await page.getByRole("combobox").filter({ hasText: "Full table" }).click();
    await page.getByRole("option", { name: "Random sample" }).click();
    await page.getByRole("spinbutton").first().fill("500");
    await page.getByRole("button", { name: "Save schedule" }).click();
    await expectToast(page, "Schedule saved.");

    await expect.poll(async () => (await studio.tableSchedule(table.binding_id))?.schedule_kind).toBe("profiling_and_dq");
    expect((await studio.tableSchedule(table.binding_id))?.sample_size).toBe(500);

    await openStudio(page, "/schedules");
    const row = page.getByRole("row").filter({ hasText: tableName });
    await row.getByRole("button", { name: /Delete|Remove/ }).first().click();
    await page.getByRole("alertdialog").getByRole("button", { name: "Delete schedule" }).click();
    await expectToast(page, "Schedule removed");
  });
});
