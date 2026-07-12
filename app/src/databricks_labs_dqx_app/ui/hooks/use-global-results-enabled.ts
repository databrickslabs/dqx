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
  const { data } = useGetGlobalResultsSettings({ query: { select: (d) => d.data } });
  return data?.global_results_enabled ?? false;
}
