import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { expectToast, openStudio } from "../../support/ui";

// Journey: a data product owner schedules a whole collection, pauses and
// resumes it, jumps to its runs, and removes the schedule. The default daily
// 06:00 UTC slot is never reached during the test.

const TABLE = SAMPLE_TABLES.collectionSchedule;

test.describe("Collection schedule", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("schedule a collection, pause and resume it, remove it", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(240_000);
    const rule = await studio.createPublishedRule({
      name: e2eName("reason-key-present", testInfo.workerIndex),
      fn: "is_not_null",
      dimension: "Completeness",
    });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "r_reason_sk" }]);
    cleanup.table(table.binding_id);
    const name = e2eName("returns-insights", testInfo.workerIndex);
    const collection = await studio.createCollection(name);
    cleanup.defer(`delete collection ${name}`, async () => {
      await studio.clearCollectionSchedule(collection.product_id);
      await studio.deleteCollection(collection.product_id);
    });
    await studio.addCollectionMember(collection.product_id, table.binding_id);
    await studio.submitCollection(collection.product_id);
    await studio.approveCollection(collection.product_id);

    await test.step("schedule the collection", async () => {
      await openStudio(page, "/schedules/new");
      await page.getByRole("button", { name: "Add collection" }).click();
      const picker = page.getByRole("dialog", { name: "Add collection" });
      await picker.getByRole("textbox").first().fill(name);
      await picker.getByRole("row").filter({ hasText: name }).click();
      await expect(picker).toBeHidden();
      await page.getByRole("button", { name: "Save schedule" }).click();
      await expectToast(page, "Schedule saved.");
      await expect.poll(async () => (await studio.collectionSchedule(collection.product_id))?.cron ?? null).not.toBeNull();
    });

    const row = page.getByRole("row").filter({ hasText: name });

    await test.step("pause and resume it", async () => {
      await openStudio(page, "/schedules");
      await expect(row).toContainText("Active");
      await row.getByRole("button", { name: "Pause" }).click();
      await expectToast(page, "Schedule paused");
      await expect(row).toContainText("Paused");
      expect((await studio.collectionSchedule(collection.product_id))?.paused).toBe(true);
      await row.getByRole("button", { name: "Unpause" }).click();
      await expectToast(page, "Schedule unpaused");
      await expect(row).toContainText("Active");
    });

    await test.step("jump to the collection's runs", async () => {
      await row.getByRole("link", { name: "View runs" }).click();
      await page.waitForURL(/\/runs-history/);
      await page.goBack();
      await expect(row).toBeVisible();
    });

    await test.step("remove the schedule", async () => {
      await row.getByRole("button", { name: /Delete|Remove/ }).first().click();
      await page.getByRole("alertdialog").getByRole("button", { name: "Delete schedule" }).click();
      await expectToast(page, "Schedule removed");
      await expect(row).toHaveCount(0);
      await expect.poll(async () => (await studio.collectionSchedule(collection.product_id))?.cron ?? null).toBeNull();
    });
  });
});
