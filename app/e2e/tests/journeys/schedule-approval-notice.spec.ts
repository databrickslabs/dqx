import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { openStudio } from "../../support/ui";

// Journey (#1665): a schedule on a table that is still waiting for approval
// shows the "Won't run until approved" notice, so authors can save the cron
// without thinking it will fire early.

const TABLE = SAMPLE_TABLES.scheduleApprovalNotice;

test.describe("Schedule approval notice", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("shows the waiting-for-approval notice on an unapproved table's schedule", async ({
    page,
    studio,
    cleanup,
  }, testInfo) => {
    test.setTimeout(180_000);
    const rule = await studio.createPublishedRule({
      name: e2eName("time-sk-present", testInfo.workerIndex),
      fn: "is_not_null",
      dimension: "Completeness",
    });
    cleanup.rule(rule.rule_id);

    const table = await studio.monitorTable(TABLE);
    cleanup.table(table.binding_id);
    await studio.applyRule(table.binding_id, rule.rule_id, "t_time_sk");
    // Leave the table in draft / pending — do not approve.
    expect((await studio.getMonitoredTable(table.binding_id)).status).not.toBe("approved");

    await openStudio(page, `/monitored-tables/${table.binding_id}?tab=scheduling`);
    await expect(page.getByRole("heading", { name: "Won't run until approved" })).toBeVisible({
      timeout: 60_000,
    });
    await expect(
      page.getByText(/Scheduled runs use the latest approved rules, and this table has none yet/),
    ).toBeVisible();
  });
});
