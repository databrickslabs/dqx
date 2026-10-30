import { expect, test } from "../../fixtures";
import { e2eName } from "../../support/names";
import { SAMPLE_TABLES } from "../../support/sample-tables";
import { expectToast, openStudio } from "../../support/ui";

// Journey: a rule is used by two tables — one follows its latest version, one
// is pinned to v1. The rule's owner revises it and an approver publishes v2:
// the following table picks v2 up and comes back for approval, the pinned one
// keeps running v1.

const [FOLLOWING, PINNED] = SAMPLE_TABLES.ruleVersions;

test.describe("Rule versions", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("publish v2 of a rule used by a following and a pinned table", async ({ page, studio, cleanup }, testInfo) => {
    test.setTimeout(240_000);
    const name = e2eName("site-key-present", testInfo.workerIndex);
    const rule = await studio.createPublishedRule({ name, fn: "is_not_null", dimension: "Completeness", description: "v1: keys are present." });
    cleanup.rule(rule.rule_id);
    const following = await studio.approvedTable(FOLLOWING, [{ ruleId: rule.rule_id, column: "web_site_sk" }]);
    cleanup.table(following.binding_id);
    const pinned = await studio.approvedTable(PINNED, [{ ruleId: rule.rule_id, column: "wp_web_page_sk" }]);
    cleanup.table(pinned.binding_id);
    const applyRules = page.getByRole("tabpanel", { name: "Apply Rules" });

    await test.step("pin the second table to v1", async () => {
      await openStudio(page, `/monitored-tables/${pinned.binding_id}?tab=apply-rules`);
      await applyRules.getByRole("button", { name: "Latest (v1)" }).click();
      await page.getByRole("menuitem", { name: "v1", exact: true }).click();
      await expect(applyRules.getByRole("button", { name: "Pinned (v1)" })).toBeVisible();
      await page.getByRole("button", { name: "Save as draft" }).click();
      await expectToast(page, "Applied rules saved as draft.");
      await studio.submitTable(pinned.binding_id);
      await studio.approveTable(pinned.binding_id);
    });

    await test.step("revise the rule and publish v2", async () => {
      await openStudio(page, `/registry-rules/${rule.rule_id}`);
      await page.getByPlaceholder("What this rule checks and why.").fill("v2: keys are present and checked on every load.");
      await page.getByRole("button", { name: "Save & submit for review" }).click();
      await expect.poll(async () => (await studio.getRule(rule.rule_id)).status).toBe("pending_approval");

      await openStudio(page, "/registry-rules");
      await page.getByPlaceholder("Search rules…").fill(name);
      await page.getByRole("row").filter({ hasText: name }).getByRole("button", { name: "Approve" }).click();
      await expectToast(page, "Rule approved and published.");
      await expect.poll(async () => (await studio.getRule(rule.rule_id)).version).toBe(2);
    });

    await test.step("the following table picks up v2 and comes back for approval", async () => {
      await expect.poll(async () => (await studio.getMonitoredTable(following.binding_id)).status).toBe("pending_approval");
      await openStudio(page, `/monitored-tables/${following.binding_id}?tab=apply-rules`);
      await expect(applyRules.getByRole("button", { name: "Latest (v2)" })).toBeVisible();

      await openStudio(page, "/rules/drafts");
      const row = page.getByRole("row").filter({ hasText: FOLLOWING });
      await row.getByRole("button", { name: "Approve" }).click();
      await expectToast(page, /^Table approved/);
      await expect.poll(async () => (await studio.getMonitoredTable(following.binding_id)).status).toBe("approved");
    });

    await test.step("the pinned table stays on v1", async () => {
      expect((await studio.getMonitoredTable(pinned.binding_id)).status).toBe("approved");
      await openStudio(page, `/monitored-tables/${pinned.binding_id}?tab=apply-rules`);
      await expect(applyRules.getByRole("button", { name: "Pinned (v1)" })).toBeVisible();
    });
  });
});
