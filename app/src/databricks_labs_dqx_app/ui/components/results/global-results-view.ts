import { DQ_SCORE_BUCKETS, DQ_SCORE_FILTER_ALL } from "@/components/data-table/filter-bar";
import type { FilterLayoutDef } from "@/components/data-table/filter-layout";
import {
  optionsSanitizer,
  stringArrayValue,
  stringValue,
  type ViewDefs,
  type ViewValue,
  type ViewValues,
} from "@/components/data-table/saved-views";
import type { GetGlobalResultsParams } from "@/lib/api";
import type { MultiFilters } from "@/components/monitored-tables/BindingResultsTab";

/** The Global Results filter bubbles, in their shipped order (spec §6.2). */
export type GlobalResultsFilterKey =
  | "catalog"
  | "schema"
  | "owner"
  | "collection"
  | "dqScore"
  | "table"
  | "rule"
  | "outcome"
  | "dimension"
  | "severity";

export const GLOBAL_RESULTS_FILTER_ORDER: readonly GlobalResultsFilterKey[] = [
  "catalog",
  "schema",
  "owner",
  "collection",
  "table",
  "rule",
  "dqScore",
  "outcome",
  "dimension",
  "severity",
];

export const GLOBAL_RESULTS_FILTERS: Record<GlobalResultsFilterKey, FilterLayoutDef> = {
  catalog: { labelKey: "resultsUi.filterLabelCatalog", defaultVisible: true },
  schema: { labelKey: "resultsUi.filterLabelSchema", defaultVisible: true },
  owner: { labelKey: "resultsUi.filterLabelOwner", defaultVisible: false },
  collection: { labelKey: "resultsUi.filterLabelCollection", defaultVisible: false },
  dqScore: { labelKey: "resultsUi.filterLabelDqScore", defaultVisible: false },
  table: { labelKey: "resultsUi.filterLabelTable", defaultVisible: true },
  rule: { labelKey: "resultsUi.filterLabelRule", defaultVisible: true },
  outcome: { labelKey: "resultsUi.filterLabelAlerts", defaultVisible: true },
  dimension: { labelKey: "resultsUi.filterLabelDimension", defaultVisible: false },
  severity: { labelKey: "resultsUi.filterLabelSeverity", defaultVisible: false },
};

export const GLOBAL_RESULTS_EMPTY_VALUES: Record<GlobalResultsFilterKey, ViewValue> = {
  catalog: [],
  schema: [],
  owner: [],
  collection: [],
  dqScore: DQ_SCORE_FILTER_ALL,
  table: [],
  rule: [],
  outcome: [],
  dimension: [],
  severity: [],
};

/** Set by breakdown-row clicks too, so hiding their bubble keeps the value (shown as a chip). */
export const GLOBAL_RESULTS_KEEP_VALUE_ON_HIDE: readonly GlobalResultsFilterKey[] = [
  "table",
  "rule",
  "dimension",
  "severity",
];

export const GLOBAL_RESULTS_OUTCOME_VALUES: readonly string[] = ["failed", "passed"];

/** Option values of each Global Results filter; null while that list is still loading. */
export type GlobalResultsFilterOptionValues = Record<
  Exclude<GlobalResultsFilterKey, "dqScore" | "outcome">,
  readonly string[] | null
>;

/**
 * Saved-view definitions of Global Results (Filters only, no columns). Memoise on
 * *options* and *reconcileValues* (which keeps cross-filter picks consistent).
 */
export function buildGlobalResultsViewDefs(
  options: GlobalResultsFilterOptionValues,
  reconcileValues?: (values: ViewValues) => ViewValues,
): ViewDefs<never, GlobalResultsFilterKey> {
  return {
    filters: { defaultOrder: GLOBAL_RESULTS_FILTER_ORDER, filters: GLOBAL_RESULTS_FILTERS },
    emptyValues: GLOBAL_RESULTS_EMPTY_VALUES,
    keepValueOnHide: GLOBAL_RESULTS_KEEP_VALUE_ON_HIDE,
    sanitizeValue: optionsSanitizer<GlobalResultsFilterKey>(
      {
        ...options,
        dqScore: DQ_SCORE_BUCKETS.map((b) => b.value),
        outcome: GLOBAL_RESULTS_OUTCOME_VALUES,
      },
      GLOBAL_RESULTS_EMPTY_VALUES,
    ),
    reconcileValues,
  };
}

/** The API's DQ score bucket param values. */
export type GlobalResultsScoreBucket = NonNullable<GetGlobalResultsParams["score_bucket"]>;

const SCORE_BUCKETS: readonly GlobalResultsScoreBucket[] = ["75-100", "50-75", "25-50", "0-25", "none"];

/** The facet filters a saved Global Results view describes. *column* is not part of a view. */
export function filtersFromViewValues(values: ViewValues, column: string[] = []): MultiFilters {
  return {
    catalog: stringArrayValue(values.catalog),
    schema: stringArrayValue(values.schema),
    owner: stringArrayValue(values.owner),
    collection: stringArrayValue(values.collection),
    scoreBucket: stringValue(values.dqScore, DQ_SCORE_FILTER_ALL),
    table: stringArrayValue(values.table),
    rule: stringArrayValue(values.rule),
    outcome: stringArrayValue(values.outcome),
    dimension: stringArrayValue(values.dimension),
    severity: stringArrayValue(values.severity),
    column,
  };
}

function sameStrings(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

/** The view values that change when the filters go from *prev* to *next* (column excluded). */
export function viewValueChanges(
  prev: MultiFilters,
  next: MultiFilters,
): Array<[GlobalResultsFilterKey, ViewValue]> {
  const changes: Array<[GlobalResultsFilterKey, ViewValue]> = [];
  for (const key of GLOBAL_RESULTS_FILTER_ORDER) {
    if (key === "dqScore") {
      if (prev.scoreBucket !== next.scoreBucket) changes.push([key, next.scoreBucket]);
    } else if (!sameStrings(prev[key], next[key])) {
      changes.push([key, [...next[key]]]);
    }
  }
  return changes;
}

/**
 * The table-universe params of the Global Results endpoint (Collection, Owner, DQ score).
 * Empty filters are omitted, as is the "any score" bucket.
 */
export function topFilterParams(filters: MultiFilters): {
  collection?: string[];
  owner?: string[];
  score_bucket?: GlobalResultsScoreBucket;
} {
  const bucket = SCORE_BUCKETS.find((b) => b === filters.scoreBucket);
  return {
    ...(filters.collection.length ? { collection: filters.collection } : {}),
    ...(filters.owner.length ? { owner: filters.owner } : {}),
    ...(bucket ? { score_bucket: bucket } : {}),
  };
}

/**
 * Table FQNs that belong to any of the *selected* collections, or null when no
 * collection is selected (no narrowing). Unknown collection ids contribute nothing.
 */
export function collectionTableFqns(
  membersByCollection: ReadonlyMap<string, readonly string[]>,
  selected: readonly string[],
): Set<string> | null {
  if (selected.length === 0) return null;
  const out = new Set<string>();
  for (const id of selected) for (const fqn of membersByCollection.get(id) ?? []) out.add(fqn);
  return out;
}
