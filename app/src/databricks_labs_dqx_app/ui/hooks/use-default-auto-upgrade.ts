import { useGetRulesRegistrySettings } from "@/lib/api";

/**
 * Read the app-wide "Follow latest version by default" governance setting
 * (`default_auto_upgrade`, P21-G).
 *
 * Governs the version pin chosen at ATTACH time for a brand-new rule
 * application / data-product member when the caller does not request an
 * explicit pin:
 *   * `true`  (default) → the new attachment follows the latest published
 *     version (`pinned_version: null`);
 *   * `false`           → the new attachment is pinned to the rule's CURRENT
 *     version at attach time.
 *
 * The Apply Rules staging path (`newStagedRow`) reads this so a freshly staged
 * row's pin reflects the admin default in the UI, matching the server-side
 * `resolve_pinned_version_for_new_attachment` resolution applied on save
 * (B2-116).
 *
 * Defaults to `true` (follow latest) while loading or on error, mirroring the
 * backend default so the staged row is never wrongly pinned before we
 * positively know the admin turned auto-upgrade off.
 */
export function useDefaultAutoUpgrade(): boolean {
  const { data } = useGetRulesRegistrySettings({ query: { select: (d) => d.data } });
  return data?.default_auto_upgrade ?? true;
}
