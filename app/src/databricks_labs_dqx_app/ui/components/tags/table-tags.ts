import { useListMonitoredTableUcTags } from "@/lib/api";
import { itemTags, tagId, type ItemTags } from "./tag-filter";

/** Unity Catalog tags by table FQN (tag id -> value), as listMonitoredTableUcTags returns them. */
export type UcTagsByTable = Record<string, Record<string, string>>;

/**
 * The tags a monitored table carries in the tag filter: its Unity Catalog
 * catalog, schema and table tags, plus the tags of the rules applied to it
 * (*ruleTags*, the list API's custom_tags).
 */
export function monitoredTableTags(
  ucTags: Record<string, string> | undefined,
  ruleTags: ReadonlyArray<{ key: string; value: string }> | undefined,
): ItemTags {
  return itemTags([
    ...Object.entries(ucTags ?? {}),
    ...(ruleTags ?? []).map(({ key, value }) => [tagId("rule", key), value] as const),
  ]);
}

const EMPTY: UcTagsByTable = {};

/**
 * Unity Catalog tags of every monitored table. Null while loading, so saved
 * tag picks are not dropped before their options are known; empty when the
 * read fails.
 */
export function useMonitoredTableUcTags(): UcTagsByTable | null {
  const { data, isError } = useListMonitoredTableUcTags({ query: { staleTime: 5 * 60_000 } });
  if (data) return data.data.tags_by_table ?? EMPTY;
  return isError ? EMPTY : null;
}
