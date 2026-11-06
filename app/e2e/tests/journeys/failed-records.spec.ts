import { readFileSync } from "node:fs";

import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { openStudio } from "../../support/ui";

// Journey: after a run, a steward digs into the rows that failed on the
// table's Results tab — pages through them, reads why a cell failed, and
// downloads them as CSV and Excel. samples.tpch_sf1.supplier has ~9% of its
// 10k suppliers with a negative balance, so the balance rule fails ~900 rows.

const TABLE = SAMPLE_TABLES.failedRecords;

test.describe("Failed records", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("browse, explain and download a run's failed records", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    const ruleName = e2eName("supplier-balance-not-negative", testInfo.workerIndex);
    const rule = await studio.createPublishedRule({
      name: ruleName,
      fn: "is_not_less_than",
      args: { limit: 0 },
      dimension: "Validity",
    });
    cleanup.rule(rule.rule_id);
    const table = await studio.approvedTable(TABLE, [{ ruleId: rule.rule_id, column: "s_acctbal" }]);
    cleanup.table(table.binding_id);
    const runId = await studio.runTable(table.binding_id);
    cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
    expect(await studio.waitForRunStatus(runId)).toBe("SUCCESS");

    const results = page.getByRole("tabpanel", { name: "Results" });
    const caption = results.getByText(/^Showing .* failed records\./);

    await test.step("the Results tab lists the failed rows, 20 per page", async () => {
      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=results`);
      await expect(page.getByText("A run is in progress.")).toBeHidden({ timeout: 60_000 });
      const failedRecords = results.getByRole("button", { name: "Failed Records" });
      if ((await failedRecords.getAttribute("aria-expanded")) !== "true") await failedRecords.click();
      await expect(caption).toHaveText(/^Showing 1–20 of (first )?[\d,]+ /, { timeout: 60_000 });
      await expect(results.getByText(/^Page 1 of \d+$/)).toBeVisible();
      await results.getByRole("button", { name: "Next" }).click();
      await expect(caption).toHaveText(/^Showing 21–40 of /);
      await results.getByRole("button", { name: "Prev" }).click();
      await expect(caption).toHaveText(/^Showing 1–20 of /);
    });

    await test.step("hovering a failed cell explains which rule failed", async () => {
      // Failed cells are tinted; the balance column is the one that fails.
      const row = results.locator("table").last().locator("tbody tr").first();
      const headers = await results.locator("table").last().locator("thead th").allTextContents();
      const balance = headers.findIndex((h) => h.trim() === "s_acctbal");
      expect(balance, `s_acctbal column in ${headers.join(",")}`).toBeGreaterThanOrEqual(0);
      await row.locator("td").nth(balance).hover();
      await expect(page.getByRole("tooltip")).toContainText(ruleName);
    });

    await test.step("download them as CSV and Excel", async () => {
      const total = Number(((await caption.textContent()) ?? "").match(/([\d,]+) total failed records|of ([\d,]+) failed/)?.slice(1).find(Boolean)?.replace(/,/g, ""));
      for (const [label, extension] of [
        ["CSV", "csv"],
        ["Excel", "xlsx"],
      ] as const) {
        await results.getByRole("button", { name: "Download" }).click();
        const downloaded = page.waitForEvent("download");
        await page.getByRole("menuitem", { name: label }).click();
        const file = await downloaded;
        expect(file.suggestedFilename()).toMatch(new RegExp(`^supplier_\\d{4}-\\d{2}-\\d{2}_invalid_records\\.${extension}$`));
        if (extension === "csv") {
          const lines = readFileSync(await file.path(), "utf8").trim().split(/\r?\n/);
          expect(lines[0]).toContain("s_acctbal");
          expect(lines[0]).toContain("Failed Rules");
          expect(lines.length - 1, "one CSV line per failed record").toBe(total);
        }
      }
    });
  });
});
