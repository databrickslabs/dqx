import { DQ_SCORE_BUCKETS, DQ_SCORE_FILTER_ALL, dqScoreBucketOf } from "@/components/data-table/filter-bar";

/** How the Tables overview groups its rows. */
export type TableGroupBy = "none" | "dqScore" | "catalog" | "schema";

/** One group header: a stable *key* and its display *label*. */
export interface TableGroup {
  key: string;
  label: string;
}

/** The fields grouping reads from a monitored-table row. */
export interface GroupableTable {
  tableFqn: string;
  score: number | null | undefined;
}

const UNKNOWN_GROUP_KEY = "__unknown";

/** DQ-score bands, best first, then "No score" — the "Any DQ score" filter's order. */
const DQ_SCORE_GROUP_ORDER: readonly string[] = DQ_SCORE_BUCKETS.filter((b) => b.value !== DQ_SCORE_FILTER_ALL).map(
  (b) => b.value,
);
const DQ_SCORE_LABEL_KEYS: ReadonlyMap<string, string> = new Map(
  DQ_SCORE_BUCKETS.map((b) => [b.value, b.labelKey]),
);

/**
 * The group a table falls in for *groupBy*. Schema groups are keyed
 * `catalog.schema`, so equally named schemas in different catalogs stay
 * apart; DQ-score groups use the score filter's bands plus "No score".
 * *translate* resolves i18n keys (pass `t`).
 */
export function tableGroupOf(
  row: GroupableTable,
  groupBy: TableGroupBy,
  translate: (key: string) => string,
): TableGroup {
  const [catalog = "", schema = ""] = row.tableFqn.split(".");
  switch (groupBy) {
    case "dqScore": {
      const bucket = dqScoreBucketOf(row.score);
      return { key: bucket, label: translate(DQ_SCORE_LABEL_KEYS.get(bucket) ?? "common.dqScoreFilter.none") };
    }
    case "catalog":
      return catalog
        ? { key: catalog, label: catalog }
        : { key: UNKNOWN_GROUP_KEY, label: translate("monitoredTables.groupUnknown") };
    case "schema":
      return catalog && schema
        ? { key: `${catalog}.${schema}`, label: `${catalog}.${schema}` }
        : { key: UNKNOWN_GROUP_KEY, label: translate("monitoredTables.groupUnknown") };
    case "none":
      return { key: "all", label: "" };
  }
}

/** Orders two group keys: score bands best-first, names alphabetically, "Unknown" last. */
function compareGroupKeys(a: string, b: string, groupBy: TableGroupBy): number {
  if (groupBy === "dqScore") return DQ_SCORE_GROUP_ORDER.indexOf(a) - DQ_SCORE_GROUP_ORDER.indexOf(b);
  if (a === b) return 0;
  if (a === UNKNOWN_GROUP_KEY) return 1;
  if (b === UNKNOWN_GROUP_KEY) return -1;
  return a.localeCompare(b);
}

/**
 * *rows* reordered so each group's rows are contiguous under one header, in
 * group order. The sort is stable, so within a group rows keep the order
 * they arrived in (the active column sort). `"none"` returns *rows* as is.
 */
export function sortTablesByGroup<T>(
  rows: readonly T[],
  groupBy: TableGroupBy,
  toGroupable: (row: T) => GroupableTable,
): T[] {
  if (groupBy === "none") return [...rows];
  const keyOf = (row: T) => tableGroupOf(toGroupable(row), groupBy, (k) => k).key;
  return rows
    .map((row) => ({ row, key: keyOf(row) }))
    .sort((a, b) => compareGroupKeys(a.key, b.key, groupBy))
    .map(({ row }) => row);
}
