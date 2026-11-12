import { expect, test } from "../../../fixtures";
import { knownBug } from "../../../support/known-bugs";
import { e2eName } from "../../../support/names";
import { SAMPLE_TABLES } from "../../../support/sample-tables";
import { expectToast, openStudio } from "../../../support/ui";

// Journeys for the approval knobs in Admin Settings → Governance, which apply
// app-wide (so they run isolated and always restore the original values):
// requiring a draft run before submit, and skipping approval when the
// submitter could approve it themselves.

const TABLE = SAMPLE_TABLES.approvalSettings;

test.describe("Approval settings", () => {
  test("require a draft run before a table can be submitted", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(15 * 60_000);
    const original = await studio.requireDraftRun();
    cleanup.defer(`restore draft-run setting (${original})`, () => studio.setRequireDraftRun(original));
    const rule = await studio.createPublishedRule({ name: e2eName("promo-key-present", testInfo.workerIndex), fn: "is_not_null" });
    cleanup.rule(rule.rule_id);
    const table = await studio.monitorTable(TABLE);
    cleanup.table(table.binding_id);
    await studio.applyRule(table.binding_id, rule.rule_id, "p_promo_sk");

    await test.step("turn the requirement on", async () => {
      await openStudio(page, "/settings");
      await page.getByRole("tab", { name: "Governance" }).click();
      const toggle = page.getByRole("switch", { name: "Require a draft run before submit" });
      if ((await toggle.getAttribute("aria-checked")) !== "true") {
        await toggle.click();
        await expectToast(page, "Draft-run requirement saved.");
      }
      await expect.poll(() => studio.requireDraftRun()).toBe(true);
    });

    const submit = page.getByRole("button", { name: "Submit for review" });

    await test.step("submit is blocked until the draft has been run", async () => {
      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=apply-rules`);
      await expect(submit).toBeDisabled();
    });

    await test.step("run the draft", async () => {
      const before = await studio.runIdsFor(TABLE);
      await page.getByRole("group", { name: "Run this table's checks" }).getByRole("button", { name: "Run draft" }).click();
      await expectToast(page, "Run started.");
      await expect.poll(async () => (await studio.listRuns()).find((r) => r.source_table_fqn === TABLE && !before.has(r.run_id))?.run_id).toBeTruthy();
      const runId = (await studio.listRuns()).find((r) => r.source_table_fqn === TABLE && !before.has(r.run_id))!.run_id;
      cleanup.defer(`cancel run ${runId}`, () => studio.cancelRun(runId));
      expect(await studio.waitForRunStatus(runId)).toBe("SUCCESS");
    });

    await test.step("submit unlocks once the run has finished", async () => {
      // Known Studio bug (suspected from code): the run-activity hook only
      // invalidates results/score queries when a run settles, not the table
      // detail that carries last_run_at, so Submit stays disabled until the
      // page is reloaded or the 5-minute cache expires.
      await knownBug("Submit stays disabled after a draft run finishes until the page reloads", async () => {
        await expect(submit).toBeEnabled({ timeout: 30_000 });
      });
      await page.reload();
      await expect(submit).toBeEnabled();
      await submit.click();
      await expectToast(page, "Table submitted for review.");
      await expect.poll(async () => (await studio.getMonitoredTable(table.binding_id)).status).toBe("pending_approval");
    });
  });

  test("skip approval when the submitter can approve it themselves", async ({ page, studio, cleanup }, testInfo) => {
    const original = await studio.approvalsMode();
    cleanup.defer(`restore approvals mode ${original}`, () => studio.setApprovalsMode(original));
    const rule = await studio.createPublishedRule({ name: e2eName("promo-name-present", testInfo.workerIndex), fn: "is_not_null" });
    cleanup.rule(rule.rule_id);
    const table = await studio.monitorTable(TABLE);
    cleanup.table(table.binding_id);
    await studio.applyRule(table.binding_id, rule.rule_id, "p_promo_name");

    await test.step("switch approvals to Skip when self-approvable", async () => {
      await openStudio(page, "/settings");
      await page.getByRole("tab", { name: "Governance" }).click();
      await page.getByRole("combobox", { name: "Approvals mode" }).click();
      await page.getByRole("option", { name: "Skip when self-approvable" }).click();
      await expectToast(page, "Approvals mode saved.");
      await expect.poll(() => studio.approvalsMode()).toBe("auto_bypass");
    });

    await test.step("an admin's submit publishes straight away", async () => {
      await openStudio(page, `/monitored-tables/${table.binding_id}?tab=apply-rules`);
      await page.getByRole("button", { name: "Save & publish" }).click();
      await expect.poll(async () => (await studio.getMonitoredTable(table.binding_id)).status).toBe("approved");
      await expect(page.getByText("Pending approval")).toHaveCount(0);
    });
  });
});
