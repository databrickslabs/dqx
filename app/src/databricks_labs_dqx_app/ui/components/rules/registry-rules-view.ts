import type { ColumnLayoutDef } from "@/components/data-table/column-layout";
import type { FilterLayoutDef } from "@/components/data-table/filter-layout";
import { optionsSanitizer, type ViewDefs, type ViewValue } from "@/components/data-table/saved-views";
import type { LabelSelection } from "@/components/Labels";
import { labelToken, tokenToLabel } from "@/lib/format-utils";

/** "No filter" value of the dimension, severity and owner pills. */
export const RULES_FILTER_ALL = "all";

export type RuleGroupBy = "none" | "columnType" | "dimension" | "severity" | "mode" | "owner";
export const RULE_GROUP_BY_VALUES: readonly RuleGroupBy[] = [
  "none",
  "columnType",
  "dimension",
  "severity",
  "mode",
  "owner",
];

export type RulesFilterKey = "search" | "dimension" | "severity" | "owner" | "labels" | "groupBy";

// Group by is pinned: always first, divided from the other pills, not reorderable.
export const RULES_FILTER_ORDER: readonly RulesFilterKey[] = [
  "groupBy",
  "search",
  "dimension",
  "severity",
  "owner",
  "labels",
];

export const RULES_FILTERS: Record<RulesFilterKey, FilterLayoutDef> = {
  search: { labelKey: "rulesRegistry.filterSearch", defaultVisible: true },
  dimension: { labelKey: "rulesRegistry.filterDimensions", defaultVisible: true },
  severity: { labelKey: "rulesRegistry.filterSeverities", defaultVisible: true },
  owner: { labelKey: "rulesRegistry.filterOwners", defaultVisible: true },
  labels: { labelKey: "rulesRegistry.filterLabels", defaultVisible: true },
  groupBy: { labelKey: "common.groupBy", defaultVisible: false, pinned: true },
};

export const RULES_EMPTY_VALUES: Record<RulesFilterKey, ViewValue> = {
  groupBy: "none",
  search: "",
  dimension: RULES_FILTER_ALL,
  severity: RULES_FILTER_ALL,
  owner: RULES_FILTER_ALL,
  labels: [],
};

/** Browser-only layouts from before saved views; removed on first load. */
export const RULES_LEGACY_STORAGE_KEYS = ["dqx.rulesRegistry.filters", "dqx.rulesRegistry.layout"];

/**
 * Stores a label selection as sorted tokens: *labelToken(key, value)* per chosen
 * value, or the bare key when any value of that key matches. Sorted so the same
 * selection always stores the same way.
 */
export function labelSelectionToValue(selection: LabelSelection): string[] {
  const out: string[] = [];
  for (const [key, values] of selection) {
    if (values.size === 0) out.push(key);
    else for (const value of values) out.push(labelToken(key, value));
  }
  return out.sort();
}

/** Inverse of {@link labelSelectionToValue}. */
export function labelSelectionFromValue(value: readonly string[]): LabelSelection {
  const selection: LabelSelection = new Map();
  for (const token of value) {
    if (!token.includes("\u0001")) {
      if (!selection.has(token)) selection.set(token, new Set());
      continue;
    }
    const { key, value: labelValue } = tokenToLabel(token);
    const values = selection.get(key) ?? new Set<string>();
    values.add(labelValue);
    selection.set(key, values);
  }
  return selection;
}

/** Label definition values of the Rules Registry filters; null while they are still loading. */
export interface RulesFilterOptions {
  dimensions: readonly string[] | null;
  severities: readonly string[] | null;
}

/**
 * Saved-view definitions of the Rules Registry. Memoise on *options*.
 *
 * Owner and label values are not checked against options: those lists are
 * derived from the dimension/severity-filtered rules, so checking them would
 * clear an owner or label whenever the dimension changes.
 */
export function buildRegistryRulesViewDefs<C extends string>(
  columns: { defaultOrder: readonly C[]; columns: Record<C, ColumnLayoutDef> },
  options: RulesFilterOptions,
): ViewDefs<C, RulesFilterKey> {
  return {
    columns,
    filters: { defaultOrder: RULES_FILTER_ORDER, filters: RULES_FILTERS },
    emptyValues: RULES_EMPTY_VALUES,
    sanitizeValue: optionsSanitizer<RulesFilterKey>(
      { groupBy: RULE_GROUP_BY_VALUES, dimension: options.dimensions, severity: options.severities },
      RULES_EMPTY_VALUES,
    ),
    legacyStorageKeys: RULES_LEGACY_STORAGE_KEYS,
  };
}
