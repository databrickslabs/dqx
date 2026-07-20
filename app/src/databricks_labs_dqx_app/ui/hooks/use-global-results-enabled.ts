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

/**
 * Read the app-wide "show Results tab on rules" gate (item 35). Distinct from
 * {@link useGlobalResultsEnabled}: that gates the app-wide, all-tables Results
 * SURFACE + its sidebar entry; this gates only the per-rule Results tab inside
 * the Rules Registry rule dialog.
 *
 * Defaults to ``false`` (disabled) while loading or on error, so the rule
 * Results tab stays hidden until we positively know the admin enabled it.
 * Served from the same session-stable settings query (staleTime: Infinity).
 */
export function useRulesResultsTabEnabled(): boolean {
  const { data } = useGetGlobalResultsSettings({
    query: { select: (d) => d.data, staleTime: Infinity },
  });
  return data?.rules_results_tab_enabled ?? false;
}
