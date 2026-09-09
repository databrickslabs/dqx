import { useGetRulesRegistrySettings } from "@/lib/api";

/**
 * Read the app-wide "pass-threshold quality gates" enable flag. When ON,
 * checks warn when fewer than a set % of rows pass; when OFF, all threshold
 * UI is hidden and no breaches are computed server-side.
 *
 * Defaults to {@code true} (fail-open) while loading or on error, preserving
 * current behavior for all consumers until the settings response arrives.
 *
 * Pinned to staleTime: Infinity — this is session-stable app config consumed
 * on nearly every page; fetched once and served from cache rather than
 * refetching per route/tab.
 */
export function usePassThresholdEnabled(): boolean {
  const { data } = useGetRulesRegistrySettings({
    query: { select: (d) => d.data, staleTime: Infinity },
  });
  return data?.pass_threshold_enabled ?? true;
}
