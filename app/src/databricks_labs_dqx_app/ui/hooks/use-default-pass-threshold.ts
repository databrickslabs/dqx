import { useGetRulesRegistrySettings } from "@/lib/api";

/**
 * Read the workspace-wide default pass threshold (integer percent [0, 100]).
 * Checks warn when fewer than this % of rows pass; overridable per rule and
 * per column.
 *
 * Defaults to {@code 70} while loading or on error. This hook's consumers
 * (pills, rule-form placeholders) show the concrete number rather than a
 * blank, so a sensible fallback prevents a flash of an empty field.
 *
 * Pinned to staleTime: Infinity — session-stable app config; fetched once
 * and served from cache across all mounts rather than refetching per route.
 */
export function useDefaultPassThreshold(): number {
  const { data } = useGetRulesRegistrySettings({
    query: { select: (d) => d.data, staleTime: Infinity },
  });
  return data?.default_pass_threshold ?? 70;
}
