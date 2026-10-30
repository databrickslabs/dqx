import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { expectNewSuccessfulRun, expectToast, openStudio } from "../../support/ui";

// Journey: an owner schedules a table every 5 minutes with a custom cron, the
// in-app scheduler fires it (as the app's service principal, not the user),
// the scheduled run succeeds in Runs History, and the owner turns the schedule
// off. Waits for a real scheduler tick, so it takes up to ~8 minutes.

const TABLE = SAMPLE_TABLES.scheduledRun;
const EVERY_5_MINUTES = "*/5 * * * *";

test.describe("Scheduled run", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("a 5-minute schedule fires a scheduled run, then is turned off", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(20 * 60_000);
    const rule = await studio.createPublishedRule({
      name: e2eName("ship-mode-key-present", testInfo.workerIndex),
      fn: "is_not_null",
      dimension: "Completeness",
    });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "sm_ship_mode_sk" }]);
    cleanup.table(table.binding_id);
    cleanup.defer(`clear schedule on ${TABLE}`, () => studio.clearTableSchedule(table.binding_id));
    const tableName = TABLE.split(".").pop()!;

    const scheduledBefore = await studio.runIdsFor(TABLE);

    await test.step("schedule the table every 5 minutes with a cron expression", async () => {
      await openStudio(page, "/schedules/new");
      await page.getByRole("button", { name: "Add table" }).click();
      const picker = page.getByRole("dialog", { name: "Add table" });
      await picker.getByRole("textbox", { name: "Search tables" }).fill(tableName);
      await picker.getByRole("row").filter({ hasText: TABLE }).click();
      await expect(picker).toBeHidden();
      await page.getByRole("checkbox", { name: "Show cron syntax" }).check();
      const cron = page.getByRole("textbox", { name: "Cron expression" });
      await cron.fill(EVERY_5_MINUTES);
      // Expressions without a friendly description are echoed back verbatim.
      await expect(page.getByText(`Runs on: ${EVERY_5_MINUTES} (UTC)`)).toBeVisible();
      await page.getByRole("button", { name: "Save schedule" }).click();
      await expectToast(page, "Schedule saved.");
      await expect.poll(async () => (await studio.tableSchedule(table.binding_id))?.cron).toBe(EVERY_5_MINUTES);
    });

    await test.step("the scheduler fires a scheduled run that succeeds", async () => {
      // Due at the next 5-minute boundary; the scheduler ticks every minute while schedules exist.
      await expectNewSuccessfulRun(page, studio, TABLE, scheduledBefore, { type: "scheduled", timeout: 9 * 60_000 });
    });

    await test.step("turn the schedule off", async () => {
      await openStudio(page, "/schedules");
      const row = page.getByRole("row").filter({ hasText: tableName });
      await row.getByRole("button", { name: /Delete|Remove/ }).first().click();
      await page.getByRole("alertdialog").getByRole("button", { name: "Delete schedule" }).click();
      await expectToast(page, "Schedule removed");
      await expect(row).toHaveCount(0);
      await expect.poll(async () => (await studio.tableSchedule(table.binding_id))?.cron ?? null).toBeNull();
    });
  });
});
