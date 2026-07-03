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
    withResolver: true,
  });

  return { blocker, shouldBlock, isRunning, hasUnsavedChanges };
}
