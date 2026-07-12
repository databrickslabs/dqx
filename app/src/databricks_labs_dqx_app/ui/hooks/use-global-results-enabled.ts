import { useGetGlobalResultsSettings } from "@/lib/api";

/**
 * Read the app-wide "global Results tab" gate (issue B2-20). The global,
 * all-tables Results surface is OFF by default; an admin opts in via the
 * Configuration page. Consumers gate the global Results sidebar nav item and
 * the homepage overall-score "?" explainer on this.
 *
 * Defaults to ``false`` (disabled) while loading or on error, so the gated
 * surfaces stay hidden until we positively know the admin enabled them —
 * never a flash of a screen that shouldn't be there.
 */
export function useGlobalResultsEnabled(): boolean {
  // B2-22: this gate is session-stable app config consumed on nearly every
  // page (the sidebar Layout calls it) — pin to staleTime: Infinity so it's
  // fetched once and served from cache instead of refetching per route/tab.
  const { data } = useGetGlobalResultsSettings({
    query: { select: (d) => d.data, staleTime: Infinity },
  });
  return data?.global_results_enabled ?? false;
}
