import AxeBuilder from "@axe-core/playwright";
import type { Page } from "@playwright/test";

import { expect, test } from "../../fixtures";
import { knownBug } from "../../support/known-bugs";
import { openStudio, sidebarLinks } from "../../support/ui";

// Read-only WCAG 2 A/AA scan (axe-core) of every sidebar page. Only serious
// and critical violations count. Runs as its own project so an accessibility
// regression never blocks the journeys.

/**
 * Violations known today (Studio 0.1.2), by page. Several only show up when a
 * page has data (e.g. a table wide enough to scroll), so these are allowed, not
 * required: the first test fails only on violations not listed here. Prune
 * entries as they're fixed.
 *
 * - list: the sidebar <ul data-sidebar="menu"> has non-<li> children (every page).
 * - button-name: comboboxes/buttons with no accessible name — the Rules list
 *   filter selects, the Runs History filters and refresh button, the Settings
 *   principal picker.
 * - scrollable-region-focusable: horizontally scrollable table containers
 *   (data-slot="table-container") can't be reached by keyboard.
 * - aria-valid-attr-value: Results collapsible triggers point aria-controls at
 *   a panel id that isn't in the DOM.
 * - color-contrast: the Results "Organization-wide score across N tables"
 *   caption (opacity-85 on the score card).
 * - nested-interactive: Results breakdown rows are <tr role="button"> and
 *   contain links (only once there are results).
 * - aria-prohibited-attr: the amber "Pass rate below threshold (70%)" warning
 *   icon is a <span aria-label> with no role (only once a table breaches its
 *   pass threshold).
 */
const KNOWN: Record<string, readonly string[]> = {
  "/home": ["list"],
  "/registry-rules": ["button-name", "list", "scrollable-region-focusable"],
  "/monitored-tables": ["list", "scrollable-region-focusable"],
  "/collections": ["list", "scrollable-region-focusable"],
  "/rules/drafts": ["list", "scrollable-region-focusable"],
  "/results": [
    "aria-prohibited-attr",
    "aria-valid-attr-value",
    "color-contrast",
    "list",
    "nested-interactive",
    "scrollable-region-focusable",
  ],
  "/schedules": ["list", "scrollable-region-focusable"],
  "/runs-history": ["button-name", "list", "scrollable-region-focusable"],
  "/marketplace": ["list", "scrollable-region-focusable"],
  "/settings": ["button-name", "list"],
  "/profile": ["list"],
};

async function sidebarPages(page: Page): Promise<string[]> {
  await openStudio(page, "/home");
  const hrefs = await sidebarLinks(page).evaluateAll((links) => links.map((a) => a.getAttribute("href") ?? ""));
  return [...new Set(["/home", ...hrefs.filter((h) => h.startsWith("/")), "/settings", "/profile"])];
}

/** Serious/critical violation rule ids on *path*, after the page has settled. */
async function violations(page: Page, path: string, settle: () => Promise<void>): Promise<string[]> {
  await page.goto(path);
  await expect(page.locator("main")).toBeVisible();
  await settle();
  const result = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa"]).analyze();
  return result.violations
    .filter((v) => v.impact === "serious" || v.impact === "critical")
    .map((v) => `${v.id}`)
    .sort();
}

test.describe("Accessibility", () => {
  test.setTimeout(180_000);

  test("no new serious accessibility violations on any page", async ({ page, problems }) => {
    for (const path of await sidebarPages(page)) {
      await test.step(path, async () => {
        const found = await violations(page, path, () => problems.settle());
        const known = KNOWN[path] ?? [];
        expect.soft(found.filter((id) => !known.includes(id)), `new violations on ${path}`).toEqual([]);
      });
    }
  });

  test("pages have no serious accessibility violations", async ({ page, problems }) => {
    const found: Record<string, string[]> = {};
    for (const path of await sidebarPages(page)) {
      const ids = await violations(page, path, () => problems.settle());
      if (ids.length) found[path] = ids;
    }
    await knownBug("serious accessibility violations on sidebar pages (see KNOWN)", async () => {
      expect(found).toEqual({});
    });
  });
});
