import type { Page } from "@playwright/test";

import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { dialogWithHeading, expectNewSuccessfulRun, expectToast, openStudio } from "../../support/ui";

// Journeys: a data product owner builds a collection from two approved tables,
// gets it approved, runs it as one unit and reviews the rolled-up results; and
// later changes its membership and publishes a second version. Tests in this
// file share their tables and run in order.

const [WAREHOUSE, CALL_CENTER] = SAMPLE_TABLES.collectionLifecycle;

/** Creates a collection on /collections/new and adds *tables* from the Tables tab; returns its id. */
async function createCollectionInUi(page: Page, name: string, tables: readonly string[]): Promise<string> {
  await openStudio(page, "/collections/new");
  await page.locator("#dp-name").fill(name);
  await page.getByRole("button", { name: "Create collection" }).click();
  await expectToast(page, "Collection created.");
  await page.waitForURL(/\/collections\/(?!new)[^/?]+/);
  const productId = new URL(page.url()).pathname.split("/").pop()!;

  await page.getByRole("tab", { name: "Tables" }).click();
  await page.getByRole("button", { name: "Add tables" }).first().click();
  const dialog = dialogWithHeading(page, "Add tables");
  for (const fqn of tables) await dialog.getByRole("checkbox", { name: `Select ${fqn}` }).check();
  await dialog.getByRole("button", { name: `Add ${tables.length} tables` }).click();
  await page.getByRole("button", { name: "Save as draft" }).click();
  await expectToast(page, "Saved as draft.");
  return productId;
}

test.describe("Collection lifecycle", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("build, approve and run a collection of two tables", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(20 * 60_000);
    const rule = await studio.createPublishedRule({
      name: e2eName("surrogate-key-present", testInfo.workerIndex),
      fn: "is_not_null",
      dimension: "Completeness",
    });
    cleanup.rule(rule.rule_id);
    const warehouse = await studio.approvedTable(WAREHOUSE, [{ ruleId: rule.rule_id, column: "w_warehouse_sk" }]);
    cleanup.table(warehouse.binding_id);
    const callCenter = await studio.approvedTable(CALL_CENTER, [{ ruleId: rule.rule_id, column: "cc_call_center_sk" }]);
    cleanup.table(callCenter.binding_id);
    const name = e2eName("store-ops", testInfo.workerIndex);
    let productId = "";
    const collection = () => studio.getCollection(productId);

    await test.step("create the collection with both tables", async () => {
      productId = await createCollectionInUi(page, name, [WAREHOUSE, CALL_CENTER]);
      cleanup.defer(`delete collection ${productId}`, () => studio.deleteCollection(productId));
      await expect
        .poll(async () => (await collection())?.members.map((m) => m.binding_id).sort())
        .toEqual([warehouse.binding_id, callCenter.binding_id].sort());
    });

    await test.step("submit it and approve it in Review & Approve", async () => {
      await page.getByRole("button", { name: "Submit for review" }).click();
      await expectToast(page, "Submitted for review.");
      await expect.poll(async () => (await collection())?.status).toBe("pending_approval");

      await openStudio(page, "/rules/drafts");
      const row = page.getByRole("row").filter({ hasText: name });
      // Watch for the toast from the start: it can come and go while we check
      // for the optional rationale dialog below.
      const approved = expectToast(page, "Collection approved.", 30_000);
      await row.getByRole("button", { name: "Approve" }).click();
      const rationale = page.getByRole("dialog").or(page.getByRole("alertdialog")).filter({ hasText: /rationale|approve/i });
      await rationale
        .getByRole("button", { name: /^Approve/ })
        .click({ timeout: 5_000 })
        .catch(() => undefined);
      await approved;
      await expect.poll(async () => (await collection())?.status).toBe("approved");
    });

    await test.step("run the collection and see a run per table", async () => {
      const before = { [WAREHOUSE]: await studio.runIdsFor(WAREHOUSE), [CALL_CENTER]: await studio.runIdsFor(CALL_CENTER) };
      await openStudio(page, `/collections/${productId}`);
      await page.getByRole("button", { name: "Run now", exact: true }).click();
      await expect(page.getByRole("button", { name: "Running…" })).toBeVisible();
      // A collection run fans out into one run per member table.
      for (const fqn of [WAREHOUSE, CALL_CENTER]) await expectNewSuccessfulRun(page, studio, fqn, before[fqn]);
      await openStudio(page, `/collections/${productId}`);
      await expect(page.getByRole("button", { name: "Run now", exact: true })).toBeVisible({ timeout: 60_000 });
    });

    await test.step("the collection's results cover both tables", async () => {
      await page.getByRole("tab", { name: "Results" }).click();
      const results = page.getByRole("tabpanel", { name: "Results" });
      // Collections roll their tables up into an average score.
      await expect(results).toContainText("Average score", { timeout: 60_000 });
      for (const fqn of [WAREHOUSE, CALL_CENTER]) await expect(results).toContainText(fqn.split(".").pop()!);
    });

  });

  test("revise an approved collection: remove a table and publish version 2", async ({ page, studio, cleanup }, testInfo) => {
    // Known Studio bug (reproduced 3/3 on 0.1.2): for a collection whose members
    // were added in the UI, removing one, saving the draft and then submitting
    // fails with "Data product member not found: <id>" — submit replays the
    // removal the draft save already persisted. Members added via the API don't
    // trigger it. Remove test.fail() once that's fixed.
    test.fail();
    test.setTimeout(240_000);
    const rule = await studio.createPublishedRule({
      name: e2eName("surrogate-key-present-v2", testInfo.workerIndex),
      fn: "is_not_null",
      dimension: "Completeness",
    });
    cleanup.rule(rule.rule_id);
    const warehouse = await studio.approvedTable(WAREHOUSE, [{ ruleId: rule.rule_id, column: "w_warehouse_sk" }]);
    cleanup.table(warehouse.binding_id);
    const callCenter = await studio.approvedTable(CALL_CENTER, [{ ruleId: rule.rule_id, column: "cc_call_center_sk" }]);
    cleanup.table(callCenter.binding_id);

    const productId = await createCollectionInUi(page, e2eName("store-ops-v2", testInfo.workerIndex), [WAREHOUSE, CALL_CENTER]);
    cleanup.defer(`delete collection ${productId}`, () => studio.deleteCollection(productId));
    await studio.submitCollection(productId);
    await studio.approveCollection(productId);

    await openStudio(page, `/collections/${productId}?tab=tables`);
    await page.getByRole("button", { name: `Remove ${CALL_CENTER}` }).click();
    await page.getByRole("alertdialog").getByRole("button", { name: "Remove" }).click();
    await page.getByRole("button", { name: "Save as draft" }).click();
    await expectToast(page, "Saved as draft.");
    await page.getByRole("button", { name: "Submit for review" }).click();
    await expectToast(page, "Submitted for review.");
    await studio.approveCollection(productId);
    await expect.poll(async () => (await studio.getCollection(productId))?.members.map((m) => m.binding_id)).toEqual([
      warehouse.binding_id,
    ]);
    await openStudio(page, `/collections/${productId}?tab=history`);
    await expect(page.getByRole("tabpanel", { name: "History" })).toContainText(/v2|Version 2/);
  });
});
