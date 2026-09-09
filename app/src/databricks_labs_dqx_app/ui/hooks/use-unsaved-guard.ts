import { useEffect, type MutableRefObject } from "react";
import { useBlocker } from "@tanstack/react-router";

interface UseUnsavedGuardOptions {
  hasUnsavedChanges: boolean;
  isRunning?: boolean;
  /** When set to `true`, the guard is bypassed (e.g. right after a successful save). */
  bypassRef?: MutableRefObject<boolean>;
}

/**
 * Blocks in-app navigation and browser close when the page has unsaved edits
 * or a background job (e.g. dry run) is active.
 *
 * Returns a TanStack Router blocker whose `.status` / `.proceed()` / `.reset()`
 * should be wired to an AlertDialog in the consuming component.
 */
export function useUnsavedGuard({ hasUnsavedChanges, isRunning = false, bypassRef }: UseUnsavedGuardOptions) {
  const shouldBlock = hasUnsavedChanges || isRunning;

  useEffect(() => {
    if (!shouldBlock) return;
    const handler = (e: BeforeUnloadEvent) => {
      if (bypassRef?.current) return;
      e.preventDefault();
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [shouldBlock, bypassRef]);

  const blocker = useBlocker({
    shouldBlockFn: ({ current, next }) => {
      if (bypassRef?.current) return false;
      // A same-route navigation (e.g. switching the routed detail page's
      // horizontal tabs via `?tab=`) isn't "leaving" the editor — only
      // block when the destination is a different route.
      if (current?.routeId === next?.routeId) return false;
      return shouldBlock;
    },
    // TanStack Router's `history.block()` also wires its own native
    // `beforeunload` listener (see `@tanstack/history`'s `onBeforeUnload`),
    // gated ONLY by this static `enableBeforeUnload` flag — it does NOT
    // re-evaluate `shouldBlockFn` (which is async) for that native event, and
    // defaults to `true` whenever this hook's `useBlocker` is registered at
    // all. Left at its default, the browser's native "leave site?" prompt
    // fires unconditionally for every visit to a page using this hook — even
    // with zero unsaved changes — because the blocker itself is always
    // registered while the component is mounted (B10). We already run our
    // own correctly-gated `window.addEventListener("beforeunload", ...)`
    // above (checks `shouldBlock`/`bypassRef` live), so disable the
    // router's redundant, unconditional one and rely solely on ours.
    enableBeforeUnload: false,
    withResolver: true,
  });

  return { blocker, shouldBlock, isRunning, hasUnsavedChanges };
}
