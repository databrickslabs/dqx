import { expect, test } from "../../../fixtures";
import { SAMPLE_TABLES } from "../../../support/sample-tables";
import { expectToast, openStudio } from "../../../support/ui";

// Journey: an engineer profiles a monitored table, reviews the column
// statistics and adds the profiler's suggested checks. Profiling is a real
// Databricks job (about a minute for this 5-row table). Suggestions become
// Studio-named rules, so this runs isolated and cleans up by rule-id diff.

const TABLE = SAMPLE_TABLES.profiler;

test.describe("Profiler", () => {
  test("profile a table and add the suggested checks", async ({ page, studio, cleanup }) => {
    test.setTimeout(15 * 60_000);
    const before = new Set((await studio.listRules()).map((r) => r.rule_id));
    const table = await studio.monitorTable(TABLE);
    // One step so the table (and its applied rules) goes before the rules themselves.
    cleanup.defer("delete the table, then rules created from suggestions", async () => {
      await studio.deleteMonitoredTable(table.binding_id);
      for (const rule of await studio.listRules()) if (!before.has(rule.rule_id)) await studio.deleteRule(rule.rule_id);
    });
    const profileTab = page.getByRole("tabpanel", { name: "Profile" });

    await test.step("run the profiler on the whole table", async () => {
      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=profile`);
      // Profiles are kept per table, so an earlier profile of the same table may
      // already show ("Refresh profile"); a never-profiled table shows "Run profile".
      await profileTab.getByRole("button", { name: /^(Run|Refresh) profile$/ }).click();
      const confirm = page.getByRole("alertdialog", { name: "Run profile" });
      // region has 5 rows; profile all of them rather than the default 10% sample.
      await confirm.getByRole("spinbutton").fill("100");
      await confirm.getByRole("button", { name: "Run profile" }).click();
      await expect(confirm).toBeHidden();
      await expect(profileTab.getByText("Profiling…").first()).toBeVisible();
    });

    await test.step("column statistics appear", async () => {
      await expect(profileTab.getByRole("button", { name: "Refresh profile" })).toBeEnabled({ timeout: 12 * 60_000 });
      await expect(profileTab).toContainText(/Rows profiled\s*5/);
      await expect(profileTab).toContainText(/Columns\s*3/);
      const regionKey = profileTab.getByRole("button", { name: /^r_regionkey LONG 100% complete/ });
      await expect(regionKey).toBeVisible();
      await expect(profileTab).toContainText(/Min\s*0\s*Max\s*4/);
    });

    await test.step("add every suggested check", async () => {
      // The suggestions panel starts collapsed.
      await profileTab.getByRole("button", { name: /suggested checks?$/ }).click();
      await profileTab.getByRole("button", { name: "Select all" }).click();
      await profileTab.getByRole("button", { name: /^Add [1-9]\d* checks?$/ }).click();
      // Suggestions are staged into the table's unsaved selection, not saved yet.
      await expectToast(page, /^Added \d+ checks? to your selection — Save to apply\./);
      // Each suggestion becomes a published registry rule named after its check and column.
      await expect
        .poll(async () => (await studio.listRules()).filter((r) => !before.has(r.rule_id)).map((r) => r.user_metadata?.name))
        .toContain("Is not null: r_regionkey");
    });

    await test.step("the suggested rules are staged on the table and saved as a draft", async () => {
      await page.getByRole("tab", { name: "Apply Rules" }).click();
      const applyTab = page.getByRole("tabpanel", { name: "Apply Rules" });
      await expect(applyTab).toContainText("Is not null: r_regionkey");
      await expect(applyTab).toContainText("Is in range: r_regionkey");
      await page.getByRole("button", { name: "Save as draft" }).click();
      await expect
        .poll(async () => (await studio.getMonitoredTableDetail(table.binding_id)).applied_rules.length)
        .toBeGreaterThanOrEqual(3);
    });
  });
});
