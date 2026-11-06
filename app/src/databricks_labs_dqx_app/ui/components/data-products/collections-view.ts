import type { ColumnLayoutDef } from "@/components/data-table/column-layout";
import { DQ_SCORE_BUCKETS, DQ_SCORE_FILTER_ALL } from "@/components/data-table/filter-bar";
import type { FilterLayoutDef } from "@/components/data-table/filter-layout";
import { optionsSanitizer, type ViewDefs, type ViewValue } from "@/components/data-table/saved-views";

/** "No filter" value of the owner pill. */
export const COLLECTIONS_FILTER_ALL = "all";

export type CollectionsFilterKey = "search" | "owner" | "dqScore";

export const COLLECTIONS_FILTER_ORDER: readonly CollectionsFilterKey[] = ["search", "owner", "dqScore"];

export const COLLECTIONS_FILTERS: Record<CollectionsFilterKey, FilterLayoutDef> = {
  search: { labelKey: "dataProducts.filterSearch", defaultVisible: true },
  owner: { labelKey: "dataProducts.colOwner", defaultVisible: true },
  dqScore: { labelKey: "dataProducts.colDqScore", defaultVisible: true },
};

export const COLLECTIONS_EMPTY_VALUES: Record<CollectionsFilterKey, ViewValue> = {
  search: "",
  owner: COLLECTIONS_FILTER_ALL,
  dqScore: DQ_SCORE_FILTER_ALL,
};

/** Browser-only layouts from before saved views; removed on first load. */
export const COLLECTIONS_LEGACY_STORAGE_KEYS = ["dqx.collections.filters", "dqx.products.layout.v3"];

/** Owner options of the Collections filter bar; null while the collections are still loading. */
export interface CollectionsFilterOptions {
  owners: readonly string[] | null;
}

/** Saved-view definitions of the Collections overview. Memoise on *options*. */
export function buildCollectionsViewDefs<C extends string>(
  columns: { defaultOrder: readonly C[]; columns: Record<C, ColumnLayoutDef> },
  options: CollectionsFilterOptions,
): ViewDefs<C, CollectionsFilterKey> {
  return {
    columns,
    filters: { defaultOrder: COLLECTIONS_FILTER_ORDER, filters: COLLECTIONS_FILTERS },
    emptyValues: COLLECTIONS_EMPTY_VALUES,
    sanitizeValue: optionsSanitizer<CollectionsFilterKey>(
      { owner: options.owners, dqScore: DQ_SCORE_BUCKETS.map((b) => b.value) },
      COLLECTIONS_EMPTY_VALUES,
    ),
    legacyStorageKeys: COLLECTIONS_LEGACY_STORAGE_KEYS,
  };
}
