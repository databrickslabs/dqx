import type { ColumnLayoutDef } from "@/components/data-table/column-layout";
import { DQ_SCORE_BUCKETS, DQ_SCORE_FILTER_ALL } from "@/components/data-table/filter-bar";
import type { FilterLayoutDef } from "@/components/data-table/filter-layout";
import { optionsSanitizer, type ViewDefs, type ViewValue } from "@/components/data-table/saved-views";
import type { TableGroupBy } from "./table-grouping";

/** "No filter" value of the catalog, schema and owner pills. */
export const TABLES_FILTER_ALL = "all";

export type TablesFilterKey = "groupBy" | "search" | "catalog" | "schema" | "owner" | "dqScore";

// Group by is pinned: always first, divided from the other pills, not reorderable.
export const TABLES_FILTER_ORDER: readonly TablesFilterKey[] = [
  "groupBy",
  "search",
  "catalog",
  "schema",
  "owner",
  "dqScore",
];

export const TABLES_FILTERS: Record<TablesFilterKey, FilterLayoutDef> = {
  groupBy: { labelKey: "common.groupBy", defaultVisible: false, pinned: true },
  search: { labelKey: "monitoredTables.filterSearch", defaultVisible: true },
  catalog: { labelKey: "monitoredTables.colCatalog", defaultVisible: true },
  schema: { labelKey: "monitoredTables.colSchema", defaultVisible: true },
  owner: { labelKey: "monitoredTables.colOwner", defaultVisible: true },
  dqScore: { labelKey: "monitoredTables.colDqScore", defaultVisible: true },
};

export const TABLE_GROUP_BY_VALUES: readonly TableGroupBy[] = ["none", "dqScore", "catalog", "schema"];

export const TABLES_EMPTY_VALUES: Record<TablesFilterKey, ViewValue> = {
  groupBy: "none",
  search: "",
  catalog: TABLES_FILTER_ALL,
  schema: TABLES_FILTER_ALL,
  owner: TABLES_FILTER_ALL,
  dqScore: DQ_SCORE_FILTER_ALL,
};

/** Browser-only layouts from before saved views; removed on first load. */
export const TABLES_LEGACY_STORAGE_KEYS = ["dqx.monitoredTables.filters", "dqx.monitoredTables.layout.v4"];

/** Facet options of the Tables filter bar; null while the tables are still loading. */
export interface TablesFilterOptions {
  catalogs: readonly string[] | null;
  /** Every schema across all catalogs, so changing the catalog never clears the schema. */
  schemas: readonly string[] | null;
  owners: readonly string[] | null;
}

/** Saved-view definitions of the Tables overview. Memoise on *options*. */
export function buildMonitoredTablesViewDefs<C extends string>(
  columns: { defaultOrder: readonly C[]; columns: Record<C, ColumnLayoutDef> },
  options: TablesFilterOptions,
): ViewDefs<C, TablesFilterKey> {
  return {
    columns,
    filters: { defaultOrder: TABLES_FILTER_ORDER, filters: TABLES_FILTERS },
    emptyValues: TABLES_EMPTY_VALUES,
    sanitizeValue: optionsSanitizer<TablesFilterKey>(
      {
        groupBy: TABLE_GROUP_BY_VALUES,
        catalog: options.catalogs,
        schema: options.schemas,
        owner: options.owners,
        dqScore: DQ_SCORE_BUCKETS.map((b) => b.value),
      },
      TABLES_EMPTY_VALUES,
    ),
    legacyStorageKeys: TABLES_LEGACY_STORAGE_KEYS,
  };
}
