/**
 * Pure derivation of the version-pin dropdown's menu model from a rule's
 * full published-version history — shared by `VersionPinDropdown`
 * (apply-rules/RuleConfigCard.tsx) so the menu-building logic has exactly
 * one implementation and can be unit-tested without mounting the dropdown.
 *
 * Mirrors `MemberVersionPin`'s "Use latest (vN)" + vN..v1 shape (visual
 * parity, per the P24 fix), but is generic over the caller's *versions*
 * list rather than assuming a dense 1..N range — the applied-rule pin
 * dropdown fetches the rule's REAL published version history (P20's
 * `listRegistryRuleVersions`), which is exactly that dense range in
 * practice (every publish adds the next integer), but deriving from the
 * fetched list rather than counting from `latestVersion` keeps this
 * correct even if versions are ever deleted/sparse.
 */

export interface VersionPinMenuEntry {
  /** A published version number, descending order in the returned list. */
  version: number;
  /** True when this entry is the currently-pinned version. */
  checked: boolean;
}

export interface VersionPinMenuModel {
  /** Whether "Follow latest" is the active choice (no explicit pin). */
  followLatestChecked: boolean;
  /** The rule's current published version — what "Follow latest" resolves to. */
  latestVersion: number;
  /** Every published version, newest first, with the pinned one flagged. */
  entries: VersionPinMenuEntry[];
  /** True when the pin points at an older version than latest (amber indicator). */
  stale: boolean;
}

/**
 * Builds the version-pin dropdown's menu model.
 *
 * @param versions - The rule's full published version numbers, any order
 *   (deduplicated and sorted descending internally). Typically the
 *   `.version` field of every `RegistryRuleVersionOut` returned by
 *   `listRegistryRuleVersions`.
 * @param pinnedVersion - `null`/`undefined` means "follow latest"; a
 *   number pins to that version.
 * @param latestVersion - The rule's current published version (from the
 *   registry rule's `version` field), used both as the "Follow latest"
 *   label and the staleness comparison — kept as an explicit input
 *   (rather than `Math.max(...versions)`) so a pin can be recognized as
 *   stale even before the versions list has loaded.
 */
export function buildVersionPinMenuModel(
  versions: number[],
  pinnedVersion: number | null | undefined,
  latestVersion: number,
): VersionPinMenuModel {
  const pinned = pinnedVersion ?? null;
  const sorted = Array.from(new Set(versions)).sort((a, b) => b - a);
  return {
    followLatestChecked: pinned === null,
    latestVersion,
    entries: sorted.map((version) => ({ version, checked: pinned === version })),
    stale: pinned !== null && pinned < latestVersion,
  };
}
