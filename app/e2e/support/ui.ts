import { expect, type Locator, type Page } from "@playwright/test";

import type { StudioApi } from "./studio-api";

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
export async function expectToast(page: Page, text: string | RegExp, timeout = 15_000): Promise<void> {
  const toasts = page.locator("[data-sonner-toast]");
  // Record every toast shown while waiting, so a mismatch reports what Studio actually said.
  const seen = new Set<string>();
  const found = await expect
    .poll(
      async () => {
        for (const t of await toasts.allTextContents()) seen.add(t.trim());
        return [...seen].some((t) => (typeof text === "string" ? t.includes(text) : text.test(t)));
      },
      { intervals: [200], timeout },
    )
    .toBe(true)
    .then(() => true)
    .catch(() => false);
  if (!found) {
    throw new Error(`Expected a toast matching ${String(text)}; saw: ${seen.size ? [...seen].join(" | ") : "no toasts"}`);
  }
}

/**
 * Waits for a validation run of *tableFqn* that isn't in *before* (run ids taken
 * with *StudioApi.runIdsFor* before triggering it) to succeed, then checks Runs
 * History shows it. Run history outlives monitored tables and the list is
 * capped, so earlier runs of the suite can't be told apart by row count.
 */
export async function expectNewSuccessfulRun(
  page: Page,
  studio: StudioApi,
  tableFqn: string,
  before: Set<string>,
  { type, timeout = 10 * 60_000 }: { type?: "scheduled" | "manual"; timeout?: number } = {},
): Promise<void> {
  let status = "";
  await expect
    .poll(
      async () => {
        const run = (await studio.listRuns()).find(
          (r) => r.source_table_fqn === tableFqn && !before.has(r.run_id) && (!type || r.run_type === type),
        );
        status = run?.status ?? "";
        return status;
      },
      { message: `no new ${type ?? ""} run of ${tableFqn} finished`, intervals: [10_000], timeout },
    )
    .toMatch(/^(SUCCESS|FAILED|CANCELED)$/);
  expect(status, `new run of ${tableFqn}`).toBe("SUCCESS");

  // Newest first: the table's top row in Runs History is this run.
  await openStudio(page, "/runs-history");
  await expect(page.getByText("Loading...")).toBeHidden({ timeout: 60_000 });
  const row = page.getByRole("row").filter({ hasText: tableFqn }).first();
  await expect(row, tableFqn).toContainText("Success");
  if (type === "scheduled") await expect(row).toContainText("Scheduled");
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
  await page.locator("[cmdk-input]").last().fill(option);
  // Clicking a row's label replaces the selection with it and closes the popover.
  await comboboxOption(page, option).click();
  await expect(page.locator("[cmdk-input]")).toHaveCount(0);
}

/**
 * Like *pickFromCombobox*, but selects several options: ticking a row's
 * checkbox (rather than its label) adds it and keeps the popover open.
 */
export async function pickManyFromCombobox(page: Page, placeholder: string, options: readonly string[]): Promise<void> {
  await page.getByRole("combobox").filter({ hasText: placeholder }).click();
  const search = page.locator("[cmdk-input]").last();
  for (const option of options) {
    await search.fill(option);
    await comboboxOption(page, option).getByRole("checkbox").click();
  }
  await page.keyboard.press("Escape");
}

function comboboxOption(page: Page, option: string): Locator {
  return page.locator("[cmdk-item]").filter({ hasText: new RegExp(`^\\s*${escapeRegExp(option)}\\s*$`) }).first();
}

export function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
