/** Filter the available class.* tags: drop ones already selected, then
 *  substring-match the query (case-insensitive). Preserves input order. */
export function filterTags(all: string[], selected: string[], query: string): string[] {
  const selectedSet = new Set(selected);
  const q = query.trim().toLowerCase();
  return all.filter((t) => !selectedSet.has(t) && (q === "" || t.toLowerCase().includes(q)));
}

/** One top-level governed-tag KEY plus its `=value` variants, ready for the
 *  two-level drilldown picker. */
export interface TagGroup {
  /** The part before the first `=` (or the whole tag when there is no `=`). */
  key: string;
  /** True when the bare `KEY` itself is a selectable governed tag — i.e. it
   *  exists in the list without a `=value` suffix AND is not already applied. */
  hasBareTag: boolean;
  /** Remaining selectable `value` portions for this key (full `KEY=value`
   *  strings that are already applied are dropped). Order of first appearance. */
  values: string[];
}

/** Collapse the flat governed-tag strings into one group per top-level KEY for
 *  the drilldown picker. Each tag is either a bare `KEY` or `KEY=value`; all
 *  variants of a KEY fold into a single group.
 *
 *  `selected` (already-applied full tag strings) is respected at both levels:
 *  a bare KEY that is applied is no longer selectable (`hasBareTag = false`) but
 *  the group is still returned so its values remain reachable; an applied
 *  `KEY=value` is removed from `values`. A group with no selectable bare tag and
 *  no remaining values is omitted entirely.
 *
 *  Key order and value order both follow first appearance in `all`. */
export function groupTags(all: string[], selected: string[]): TagGroup[] {
  const selectedSet = new Set(selected);
  const order: string[] = [];
  const bareExists = new Map<string, boolean>();
  const valuesByKey = new Map<string, string[]>();

  for (const tag of all) {
    const eq = tag.indexOf("=");
    const key = eq === -1 ? tag : tag.slice(0, eq);
    if (!valuesByKey.has(key)) {
      valuesByKey.set(key, []);
      bareExists.set(key, false);
      order.push(key);
    }
    if (eq === -1) {
      bareExists.set(key, true);
    } else {
      const value = tag.slice(eq + 1);
      const list = valuesByKey.get(key)!;
      if (!list.includes(value)) list.push(value);
    }
  }

  const groups: TagGroup[] = [];
  for (const key of order) {
    const hasBareTag = bareExists.get(key)! && !selectedSet.has(key);
    const values = valuesByKey.get(key)!.filter((v) => !selectedSet.has(`${key}=${v}`));
    if (!hasBareTag && values.length === 0) continue;
    groups.push({ key, hasBareTag, values });
  }
  return groups;
}

/** Root-view search: keep a group when the query is empty, matches its KEY, or
 *  matches any of its (remaining) values. Case-insensitive substring, so a
 *  search for a value still surfaces the parent KEY row. Preserves order. */
export function filterGroups(groups: TagGroup[], query: string): TagGroup[] {
  const q = query.trim().toLowerCase();
  if (q === "") return groups;
  return groups.filter(
    (g) => g.key.toLowerCase().includes(q) || g.values.some((v) => v.toLowerCase().includes(q)),
  );
}
