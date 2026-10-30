import { expect, type Locator, type Page } from "@playwright/test";

// UI helpers shared by journeys. Selectors use roles and the English UI copy
// (ui/lib/i18n/locales/en.json); the browser runs with an en-US locale.

const SIDEBAR_LINK = 'a[data-sidebar="menu-button"]';

/**
 * Opens a Studio page and gets past the "setup finished with warnings" review,
 * which a fresh browser shows once (the acknowledgement lives in localStorage).
 */
export async function openStudio(page: Page, path = "/home"): Promise<void> {
  await page.goto(path);
  const nav = page.locator(SIDEBAR_LINK).first();
  const acknowledge = page.getByRole("button", { name: "I understand, continue to Studio" });
  await expect(nav.or(acknowledge)).toBeVisible();
  if (await acknowledge.isVisible()) {
    await acknowledge.click();
    await expect(nav).toBeVisible();
    // The SPA stays on the setup route after acknowledging; go where we were asked.
    if (new URL(page.url()).pathname !== path) await page.goto(path);
  }
  await expect(nav).toBeVisible();
}

/** The sidebar navigation links (hrefs vary with role and approvals mode). */
export function sidebarLinks(page: Page): Locator {
  return page.locator(SIDEBAR_LINK);
}

/** Waits for a Sonner toast containing *text*. */
export async function expectToast(page: Page, text: string | RegExp): Promise<void> {
  await expect(page.locator("[data-sonner-toast]").filter({ hasText: text }).first()).toBeVisible();
}

/** A dialog identified by its heading (Studio dialogs don't all expose an accessible name). */
export function dialogWithHeading(page: Page, heading: string): Locator {
  return page.getByRole("dialog").filter({ has: page.getByRole("heading", { name: heading }) });
}

/**
 * Operates a cmdk-backed multi-select (MultiSelectPopover): opens the combobox
 * showing *placeholder* (its label isn't associated, so it has no accessible
 * name), types *option* into its search box and picks the exact match.
 */
export async function pickFromCombobox(page: Page, placeholder: string, option: string): Promise<void> {
  await page.getByRole("combobox").filter({ hasText: placeholder }).click();
  const search = page.locator("[cmdk-input]").last();
  await search.fill(option);
  await page.locator("[cmdk-item]").filter({ hasText: new RegExp(`^\\s*${escapeRegExp(option)}\\s*$`) }).first().click();
  await page.keyboard.press("Escape");
}

export function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
