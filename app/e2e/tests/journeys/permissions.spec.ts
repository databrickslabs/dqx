import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { dialogWithHeading, expectToast, openStudio } from "../../support/ui";

// Journey: a table owner shares their monitored table with the workspace
// "users" group from its Permissions tab, then revokes it. Studio permissions
// only (not Unity Catalog grants), and the table is a read-only sample.

const TABLE = SAMPLE_TABLES.permissions;
const GROUP = "users";

test.describe("Permissions", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("grant a group SELECT on a table, then revoke it", async ({ page, studio, cleanup }, testInfo) => {
    const rule = await studio.createPublishedRule({ name: e2eName("demo-key-present", testInfo.workerIndex), fn: "is_not_null" });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "hd_demo_sk" }]);
    cleanup.table(table.binding_id);
    // The built-in workspace group's principal id is "users"; its display name may differ.
    const groupGrant = async () =>
      (await studio.grants("monitored_table", table.binding_id)).find(
        (g) => g.principal_id === GROUP || g.principal_name === GROUP,
      );
    test.skip(Boolean(await groupGrant()), `new tables are already shared with "${GROUP}" on this deployment`);
    cleanup.defer(`revoke ${GROUP}`, async () => {
      const grant = await groupGrant();
      if (grant) await studio.revokeGrant("monitored_table", table.binding_id, grant.principal_id);
    });
    const tab = page.getByRole("tabpanel", { name: "Permissions" });

    await test.step(`grant ${GROUP} SELECT`, async () => {
      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=permissions`);
      await tab.getByRole("button", { name: "Grant permission" }).click();
      const dialog = dialogWithHeading(page, "Grant permission");
      await dialog.getByRole("combobox").filter({ hasText: "Select a principal…" }).click();
      await page.getByPlaceholder("Search users or groups…").fill(GROUP);
      // Exact match: prefix search also returns look-alikes such as "users-clone-…".
      await page.locator("[cmdk-item]").filter({ hasText: new RegExp(`^\\s*${GROUP}\\s*Group\\s*$`) }).first().click();
      await dialog.getByRole("checkbox", { name: /SELECT/ }).check();
      await dialog.getByRole("button", { name: "Save" }).click();
      await expectToast(page, "Permission saved.");
      await expect(tab.getByRole("row").filter({ hasText: GROUP })).toContainText("SELECT");
      expect((await groupGrant())?.privileges).toContain("SELECT");
    });

    await test.step("revoke it", async () => {
      await tab.getByRole("row").filter({ hasText: GROUP }).getByRole("button", { name: "Revoke permission" }).click();
      await expectToast(page, "Permission removed.");
      await expect(tab.getByRole("row").filter({ hasText: GROUP })).toHaveCount(0);
      expect(await groupGrant()).toBeUndefined();
    });
  });
});
