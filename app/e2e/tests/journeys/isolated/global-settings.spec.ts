import { expect, test } from "../../../fixtures";
import { runTag } from "../../../support/names";
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
    const key = `e2e_${runTag()}`;
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
});
