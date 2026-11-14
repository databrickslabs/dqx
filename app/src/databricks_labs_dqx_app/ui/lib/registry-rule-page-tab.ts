// Top-level page tabs of the rule editor. Persisted to the URL (`?tab=`) by
// the routed detail / new pages so browser back/forward moves between them.
export const PAGE_TAB_KEYS = ["about", "permissions", "implementation", "test", "history", "results"] as const;

export type PageTab = (typeof PAGE_TAB_KEYS)[number];

export const DEFAULT_PAGE_TAB: PageTab = "about";

/** Resolve the active tab from the raw `?tab=` search value. An absent or
 *  unknown value falls back to the About tab without rewriting the URL, so
 *  opening a rule adds exactly one history entry (mirrors the Tables and
 *  Collections detail pages). */
export function resolvePageTab(tab: string | undefined): PageTab {
  return tab !== undefined && (PAGE_TAB_KEYS as readonly string[]).includes(tab) ? (tab as PageTab) : DEFAULT_PAGE_TAB;
}
