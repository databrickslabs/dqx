import { useLayoutEffect } from "react";

/**
 * Reset the app's main scroll container to the top whenever *key* changes.
 *
 * The scrollable element is the `[data-scroll-root]` div in
 * `components/layout/SidebarLayout.tsx` — NOT the window — so
 * `window.scrollTo(0, 0)` is a no-op here. TanStack Router's
 * `scrollRestoration` only fires on PATH changes; the Results view on both the
 * monitored-table and collection detail pages is a `?tab=` search-param change
 * within the same route, so the router never resets scroll and the container
 * keeps the previous tab's position when Results opens partway down.
 *
 * Pass a *key* that changes when the visible view changes (e.g. the active tab
 * key). The reset runs on mount and on every subsequent *key* change.
 * `useLayoutEffect` runs the jump before paint so there is no scroll flash.
 * Guarded for non-DOM (SSR/test) environments.
 *
 * @param key value whose change re-triggers the scroll reset (default: a
 *   constant, so the reset runs once on mount).
 */
export function useScrollToTop(key: string | number = "mount"): void {
  useLayoutEffect(() => {
    if (typeof document === "undefined") return;
    const root = document.querySelector<HTMLElement>("[data-scroll-root]");
    root?.scrollTo({ top: 0, left: 0 });
  }, [key]);
}
