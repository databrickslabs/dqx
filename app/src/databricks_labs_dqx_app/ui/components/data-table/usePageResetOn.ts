import { useState } from "react";

/**
 * Pagination state that jumps back to page 1 whenever *dependency* changes
 * identity — pass a saved view's working *values* so any filter edit, or a
 * switch to another view, starts the list from the top. Adjusted during
 * render (not in an effect) so an out-of-range page never paints.
 */
export function usePageResetOn(dependency: unknown): [number, (page: number) => void] {
  const [page, setPage] = useState(1);
  const [seen, setSeen] = useState(dependency);
  if (seen !== dependency) {
    setSeen(dependency);
    setPage(1);
  }
  return [page, setPage];
}
