import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { dialogWithHeading, expectToast, openStudio } from "../../support/ui";

// Journey: a data product owner groups approved tables into a collection,
// saves it, and later deletes it.

const TABLE = SAMPLE_TABLES.collections;

test.describe("Collections", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("create a collection, add an approved table, delete it", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(180_000);
    const rule = await studio.createPublishedRule({
      name: e2eName("part-name-present", testInfo.workerIndex),
      fn: "is_not_null",
      dimension: "Completeness",
    });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "p_name" }]);
    cleanup.table(table.binding_id);
    const name = e2eName("parts-catalogue", testInfo.workerIndex);
    let productId = "";

    await test.step("create the collection", async () => {
      await openStudio(page, "/collections");
      await page.getByRole("button", { name: "New Collection" }).click();
      await page.locator("#dp-name").fill(name);
      await page.locator("#dp-desc").fill("Parts data used by procurement.");
      await page.getByRole("button", { name: "Create collection" }).click();
      await expectToast(page, "Collection created.");
      await page.waitForURL(/\/collections\/(?!new)[^/?]+/);
      productId = new URL(page.url()).pathname.split("/").pop()!;
      cleanup.defer(`delete collection ${productId}`, () => studio.deleteCollection(productId));
      expect((await studio.getCollection(productId))?.name).toBe(name);
    });

    await test.step("add the approved table and save", async () => {
      await page.getByRole("tab", { name: "Tables" }).click();
      await page.getByRole("button", { name: "Add tables" }).first().click();
      const dialog = dialogWithHeading(page, "Add tables");
      await dialog.getByRole("checkbox", { name: `Select ${TABLE}` }).check();
      await dialog.getByRole("button", { name: "Add 1 table" }).click();
      await page.getByRole("button", { name: "Save as draft" }).click();
      await expectToast(page, "Saved as draft.");
      await expect
        .poll(async () => (await studio.getCollection(productId))?.members.map((m) => m.binding_id))
        .toEqual([table.binding_id]);
    });

    await test.step("the collection appears on the Collections list", async () => {
      await openStudio(page, "/collections");
      await expect(page.getByText(name)).toBeVisible();
    });

    await test.step("delete the collection", async () => {
      await openStudio(page, `/collections/${productId}`);
      await page.getByRole("button", { name: "Collection actions" }).click();
      await page.getByRole("menuitem", { name: "Delete" }).click();
      const confirm = page.getByRole("alertdialog");
      await expect(confirm).toContainText("Delete this collection?");
      await confirm.getByRole("button", { name: "Delete" }).click();
      await expectToast(page, "Collection deleted.");
      expect(await studio.getCollection(productId)).toBeUndefined();
    });
  });
});
