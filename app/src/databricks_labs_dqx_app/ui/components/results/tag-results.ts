import type { GroupRowOut, TrendPointOut } from "@/lib/api";
import type { FacetFilterOption } from "@/components/results/ResultsFacetFilters";

// Rule tags on the multi-table Results surfaces. The API returns by_tag /
// trend_by_tag rows for EVERY tag key in scope, each carrying its tag_key;
// the UI shows one key at a time. The tag facet value is `key=value` — the
// format of the API's repeatable `tag` param.

/** Empty value selects checks missing this key (rule tags never have empty values). */
export function tagFacetValue(key: string, value: string | null): string {
  return `${key}=${value ?? ""}`;
}

/** Split a tag facet value on its FIRST "=" (mirrors the backend); null when malformed. */
export function parseTagFacetValue(raw: string): { key: string; value: string | null } | null {
  const i = raw.indexOf("=");
  if (i <= 0) return null;
  return { key: raw.slice(0, i), value: raw.slice(i + 1) || null };
}

/** Display form of a tag facet value: "domain = finance". */
export function tagFacetLabel(raw: string, missingLabel = "Untagged"): string {
  const parsed = parseTagFacetValue(raw);
  return parsed ? `${parsed.key} = ${parsed.value ?? missingLabel}` : raw;
}

/** Include keys that only occur in older runs, beyond the latest-run groups. */
export function tagKeysFromHistory(
  options: Record<string, Array<string | null>> | undefined,
  ...groupLists: Array<GroupRowOut[] | undefined>
): string[] {
  return [...new Set([...Object.keys(options ?? {}), ...tagKeysOf(...groupLists)])].sort((a, b) => a.localeCompare(b));
}

/** Distinct tag keys across the given by_tag lists, sorted. */
export function tagKeysOf(...groupLists: Array<GroupRowOut[] | undefined>): string[] {
  const keys = new Set<string>();
  for (const groups of groupLists) {
    for (const g of groups ?? []) if (g.tag_key) keys.add(g.tag_key);
  }
  return [...keys].sort((a, b) => a.localeCompare(b));
}

/** The tag key to show: the chosen key while it is still in scope, else the first key (null when none). */
export function effectiveTagKey(keys: readonly string[], chosen: string | null): string | null {
  if (chosen !== null && keys.includes(chosen)) return chosen;
  return keys[0] ?? null;
}

/** The by_tag groups of one tag key. */
export function groupsForTagKey(groups: GroupRowOut[] | undefined, key: string | null): GroupRowOut[] {
  return key === null ? [] : (groups ?? []).filter((g) => g.tag_key === key);
}

/** The trend_by_tag points of one tag key. */
export function trendForTagKey(points: TrendPointOut[] | undefined, key: string | null): TrendPointOut[] {
  return key === null ? [] : (points ?? []).filter((p) => p.tag_key === key);
}

/** Tag filter options from the scoped run history, including missing values. */
export function tagFilterOptions(
  options: Record<string, Array<string | null>> | undefined,
  missingLabel = "Untagged",
): FacetFilterOption[] {
  const byValue = new Map<string, FacetFilterOption>();
  for (const [key, values] of Object.entries(options ?? {})) {
    for (const tagValue of values) {
      const value = tagFacetValue(key, tagValue);
      if (!byValue.has(value)) byValue.set(value, { value, label: tagFacetLabel(value, missingLabel) });
    }
  }
  return [...byValue.values()].sort((a, b) => a.label.localeCompare(b.label));
}
