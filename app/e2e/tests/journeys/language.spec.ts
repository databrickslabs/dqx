import { expect, test } from "../../fixtures";
import { openStudio } from "../../support/ui";

// Journey: a user switches the UI language from the header user menu and back.
// The choice is stored in the browser only, so this changes nothing server-side.

test.describe("Language", () => {
  test("switch the UI to French and back to English", async ({ page }) => {
    await openStudio(page, "/home");
    const nav = page.locator('a[data-sidebar="menu-button"][href="/registry-rules"]');
    await expect(nav).toHaveText("Rules");

    const switchTo = async (language: string) => {
      // The avatar button shows the user's initials and display name; it has no aria-label.
      await page.getByRole("banner").getByRole("button").filter({ hasNotText: /theme/i }).last().click();
      await page.getByRole("menuitem", { name: /Language|Langue/ }).hover();
      await page.getByRole("menuitemradio", { name: language }).click();
    };

    await switchTo("Français");
    await expect(nav).not.toHaveText("Rules");
    await expect(page.locator("html")).toHaveAttribute("lang", /^fr/);

    await switchTo("English");
    await expect(nav).toHaveText("Rules");
  });
});
