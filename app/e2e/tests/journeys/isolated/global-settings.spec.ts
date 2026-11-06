import { expect, test } from "../../../fixtures";
import { E2E_LABEL_KEY_PREFIX, E2E_REVIEW_STATUS_PREFIX, runTag } from "../../../support/names";
import { expectToast, openStudio } from "../../../support/ui";

// Journeys that change app-wide settings. They run in their own project after
// every other journey (see playwright.config.ts) and always restore what they
// changed, even when they fail.

test.describe("Admin settings", () => {
  test("switch approvals off and on again", async ({ page, studio, cleanup }) => {
    const original = await studio.approvalsMode();
    cleanup.defer(`restore approvals mode ${original}`, () => studio.setApprovalsMode(original));
    const approvalsMode = page.getByRole("combobox", { name: "Approvals mode" });

    await test.step("turn approvals off in Governance", async () => {
      await openStudio(page, "/settings");
      await page.getByRole("tab", { name: "Governance" }).click();
      await approvalsMode.click();
      await page.getByRole("option", { name: "No approval needed" }).click();
      await expectToast(page, "Approvals mode saved.");
      await expect(page.getByText("Every submit publishes immediately with no review step.")).toBeVisible();
      await expect.poll(() => studio.approvalsMode()).toBe("disabled");
    });

    await test.step("authors now publish directly", async () => {
      await openStudio(page, "/registry-rules/new");
      await expect(page.getByRole("button", { name: "Save & publish" })).toBeVisible();
      await expect(page.getByRole("button", { name: "Save & submit" })).toHaveCount(0);
    });

    await test.step("review & approve disappears from the sidebar", async () => {
      await expect(page.locator('a[data-sidebar="menu-button"][href="/rules/drafts"]')).toHaveCount(0);
    });

    await test.step("turn approvals back on", async () => {
      await openStudio(page, "/settings");
      await page.getByRole("tab", { name: "Governance" }).click();
      await approvalsMode.click();
      await page.getByRole("option", { name: "Require approval" }).click();
      await expectToast(page, "Approvals mode saved.");
      await expect.poll(() => studio.approvalsMode()).toBe("enabled");
      await openStudio(page, "/registry-rules/new");
      await expect(page.getByRole("button", { name: "Save & submit" })).toBeVisible();
    });
  });

  test("add a rule label key, then remove it", async ({ page, studio, cleanup }) => {
    const original = await studio.labelDefinitions();
    cleanup.defer("restore label definitions", () => studio.setLabelDefinitions(original));
    // Keys must match ^[A-Za-z][A-Za-z0-9_]*$.
    const key = `${E2E_LABEL_KEY_PREFIX}${runTag()}`;
    const tab = page.getByRole("tabpanel", { name: "Tags" });

    await test.step("add the key", async () => {
      await openStudio(page, "/settings");
      await page.getByRole("tab", { name: "Tags" }).click();
      await tab.getByRole("button", { name: "Add new key" }).click();
      const keyInput = tab.getByPlaceholder("e.g. team").last();
      await keyInput.fill(key);
      await keyInput.blur();
      await expectToast(page, /^Saved \d+ rule labels?\./);
      await expect.poll(async () => (await studio.labelDefinitions()).map((d) => d.key)).toContain(key);
    });

    await test.step("remove it", async () => {
      await page.reload();
      await page.getByRole("tab", { name: "Tags" }).click();
      const keyInput = tab.getByPlaceholder("e.g. team").and(page.locator(`[value="${key}"]`));
      await expect(keyInput).toBeVisible();
      // The key's own "Remove definition" button is the next one after its input.
      const removeButtons = tab.getByRole("button", { name: "Remove definition" });
      const index = await tab.getByPlaceholder("e.g. team").evaluateAll(
        (inputs, k) => inputs.findIndex((i) => (i as HTMLInputElement).value === k),
        key,
      );
      await removeButtons.nth(index).click();
      await expectToast(page, /^Saved \d+ rule labels?\./);
      await expect.poll(async () => (await studio.labelDefinitions()).map((d) => d.key)).not.toContain(key);
    });
  });

  test("add a run review status, then remove it", async ({ page, studio, cleanup }) => {
    const original = await studio.reviewStatuses();
    cleanup.defer("restore review statuses", () => studio.setReviewStatuses(original));
    // Values allow letters, digits, spaces and _-/. — no leading punctuation.
    const value = `${E2E_REVIEW_STATUS_PREFIX}${runTag()}`;
    const values = async () => (await studio.reviewStatuses()).map((s) => s.value);
    const tab = page.getByRole("tabpanel", { name: "Governance" });

    await test.step("add the status", async () => {
      await openStudio(page, "/settings");
      await page.getByRole("tab", { name: "Governance" }).click();
      await tab.getByRole("button", { name: "Add status" }).click();
      await tab.getByPlaceholder("e.g. Confirmed").last().fill(value);
      await tab.getByPlaceholder("Shown as a tooltip on the dropdown").last().fill("Added by the Studio Playwright suite.");
      await expectToast(page, "Review statuses saved.");
      await expect.poll(values).toContain(value);
    });

    await test.step("remove it", async () => {
      await page.reload();
      await page.getByRole("tab", { name: "Governance" }).click();
      await tab.getByRole("button", { name: `Remove ${value}` }).click();
      await expectToast(page, "Review statuses saved.");
      await expect.poll(values).not.toContain(value);
      await expect.poll(async () => (await studio.reviewStatuses()).length).toBe(original.length);
    });
  });
});
