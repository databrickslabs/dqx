import type { Page } from "@playwright/test";

import { expect, test } from "../../fixtures";
import { e2eName, E2E_TAG, runTag } from "../../support/names";
import { expectToast, openStudio } from "../../support/ui";

// Journey: a rule author builds a reusable rule in the Rules Registry, an
// approver reviews it, and it is retired. Also covers bulk import from YAML.

/** Picks an option from a Radix select identified by its placeholder/current text. */
async function chooseSelect(page: Page, current: string, option: string): Promise<void> {
  await page.getByRole("combobox").filter({ hasText: current }).click();
  // Options carry a description after the label, e.g. "Completeness Whether all …".
  await page.getByRole("option", { name: new RegExp(`^${option}\\b`) }).click();
}

/** Finds a registry rule's row on the Rules list by searching for its name. */
async function ruleRow(page: Page, name: string) {
  await openStudio(page, "/registry-rules");
  await page.getByPlaceholder("Search rules…").fill(name);
  const row = page.getByRole("row").filter({ hasText: name });
  await expect(row).toHaveCount(1);
  return row;
}

test.describe("Rules Registry", () => {
  test.beforeAll(async ({ studio }) => {
    test.skip((await studio.approvalsMode()) !== "enabled", "these journeys assume approvals mode 'Require approval'");
  });

  test("author a rule in the editor, approve it, then delete it", async ({ page, studio, cleanup }, testInfo) => {
    const name = e2eName("ui-not-null", testInfo.workerIndex);
    let ruleId = "";

    await test.step("author the rule", async () => {
      await openStudio(page, "/registry-rules");
      await page.getByRole("button", { name: "New rule" }).click();
      await expect(page.getByRole("heading", { name: "New registry rule" })).toBeVisible();

      await page.getByPlaceholder("e.g. Order amount is positive").fill(name);
      await page.getByPlaceholder("What this rule checks and why.").fill("Order identifiers must be present.");
      await chooseSelect(page, "Select a dimension…", "Completeness");
      await chooseSelect(page, "Select a severity…", "High");

      await page.getByRole("tab", { name: "Implementation" }).click();
      // The check picker is the last select-style trigger in the Condition row.
      await page.getByRole("tabpanel").locator("button[data-slot=select-trigger]").last().click();
      await page.keyboard.type("is_not_null");
      await page.locator('[cmdk-item][data-value="is_not_null"]').click();

      await page.getByRole("button", { name: "Save & submit" }).click();
      // Other journeys publish identical is_not_null rules concurrently; Studio
      // then asks before creating a duplicate, as it would for any author.
      const duplicate = page.getByRole("button", { name: "Yes, create duplicate" });
      const saved = page.waitForURL(/\/registry-rules\/(?!new)[^/?]+/);
      const confirmDuplicate = duplicate
        .waitFor()
        .then(() => duplicate.click())
        .catch(() => undefined); // no prompt: the save navigated first
      await Promise.race([saved, confirmDuplicate]);
      await saved;
      ruleId = new URL(page.url()).pathname.split("/").pop()!;
      cleanup.rule(ruleId);
    });

    await test.step("the submitted rule waits for approval", async () => {
      const rule = await studio.getRule(ruleId);
      expect(rule.user_metadata.name).toBe(name);
      expect(rule.status).toBe("pending_approval");
    });

    await test.step("an approver approves it from the Rules list", async () => {
      const row = await ruleRow(page, name);
      await row.getByRole("button", { name: "Approve" }).click();
      await expectToast(page, "Rule approved and published.");
      await expect.poll(async () => (await studio.getRule(ruleId)).status).toBe("approved");
    });

    await test.step("the rule is deleted from its detail page", async () => {
      await openStudio(page, `/registry-rules/${ruleId}`);
      await page.getByRole("button", { name: "Actions" }).click();
      await page.getByRole("menuitem", { name: "Delete" }).click();
      const confirm = page.getByRole("alertdialog");
      await expect(confirm).toContainText("Delete this registry rule?");
      await confirm.getByRole("button", { name: "Delete" }).click();
      await expectToast(page, "Rule deleted.");
      expect(await studio.findRuleByName(name)).toBeUndefined();
    });
  });

  test("an approver rejects a submitted rule", async ({ page, studio, cleanup }, testInfo) => {
    const name = e2eName("reject-me", testInfo.workerIndex);
    const rule = await studio.createNativeRule({ name, fn: "is_not_null" });
    cleanup.rule(rule.rule_id);
    await studio.submitRule(rule.rule_id);

    const row = await ruleRow(page, name);
    // The list-row action rejects immediately (no confirm step).
    await row.getByRole("button", { name: "Reject" }).click();
    await expectToast(page, "Rule rejected.");
    await expect(row).toContainText("Rejected");
    await expect.poll(async () => (await studio.getRule(rule.rule_id)).status).toBe("rejected");
  });

  test("import rules from YAML as drafts", async ({ page, studio, cleanup }, testInfo) => {
    const names = [e2eName("yaml-status-allowed", testInfo.workerIndex), e2eName("yaml-code-allowed", testInfo.workerIndex)];
    // Import reuses any existing rule with the same definition (fingerprint), and
    // other journeys publish plain checks like is_not_null concurrently. Allowed
    // values containing the run tag keep these two definitions unique to this run.
    const yaml = names
      .map(
        (name, i) => `- criticality: error
  user_metadata:
    name: ${name}
    dimension: Validity
    severity: High
    ${E2E_TAG}: "${runTag()}"
  check:
    function: is_in_list
    arguments:
      column: "{{column}}"
      allowed:
        - "${runTag()}-${i}-a"
        - "${runTag()}-${i}-b"`,
      )
      .join("\n");

    await openStudio(page, "/registry-rules/import");
    await expect(page.getByText("Import from YAML")).toBeVisible();
    await page.locator("textarea").first().fill(yaml);
    await expect(page.getByText("Loaded 2 checks from YAML")).toBeVisible();

    await page.getByRole("button", { name: "Validate" }).click();
    await expectToast(page, "All checks are valid");

    await page.getByRole("button", { name: "Save as drafts" }).click();
    await expectToast(page, /Saved 2 checks? as drafts/);

    for (const name of names) {
      const rule = await studio.findRuleByName(name);
      expect(rule, `imported rule ${name}`).toBeDefined();
      cleanup.rule(rule!.rule_id);
      expect(rule!.status).toBe("draft");
    }
  });
});
