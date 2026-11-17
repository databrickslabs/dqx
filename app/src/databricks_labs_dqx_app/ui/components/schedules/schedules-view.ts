import type { ColumnLayoutDef } from "@/components/data-table/column-layout";
import {
  oneOf,
  optionsSanitizer,
  stringValue,
  type ViewDefs,
  type ViewValue,
  type ViewValues,
} from "@/components/data-table/saved-views";
import {
  EMPTY_SCHEDULE_FILTERS,
  SCHEDULE_FILTER_ALL,
  SCHEDULE_FILTER_DEFS,
  SCHEDULE_FILTER_ORDER,
  type ScheduleFilterKey,
  type ScheduleFilters,
} from "./schedule-list";

const TYPE_VALUES: readonly ScheduleFilters["type"][] = [SCHEDULE_FILTER_ALL, "table", "collection", "scope"];
const STATUS_VALUES: readonly ScheduleFilters["status"][] = [SCHEDULE_FILTER_ALL, "active", "paused", "failing"];

export const SCHEDULES_EMPTY_VALUES: Record<ScheduleFilterKey, ViewValue> = { ...EMPTY_SCHEDULE_FILTERS };

/** Browser-only layouts from before saved views; removed on first load. */
export const SCHEDULES_LEGACY_STORAGE_KEYS = ["dqx.schedules.filters.v1", "dqx.schedules.layout.v1"];

/** A saved view's values as the Schedules filters (unknown values fall back to "no filter"). */
export function scheduleFiltersFromValues(values: ViewValues): ScheduleFilters {
  return {
    search: stringValue(values.search, EMPTY_SCHEDULE_FILTERS.search),
    type: oneOf(values.type, TYPE_VALUES, SCHEDULE_FILTER_ALL),
    status: oneOf(values.status, STATUS_VALUES, SCHEDULE_FILTER_ALL),
    owner: stringValue(values.owner, EMPTY_SCHEDULE_FILTERS.owner),
  };
}

/** Owner options of the Schedules filter bar; null while the schedules are still loading. */
export interface SchedulesFilterOptions {
  owners: readonly string[] | null;
}

/** Saved-view definitions of the Schedules overview. Memoise on *options*. */
export function buildSchedulesViewDefs<C extends string>(
  columns: { defaultOrder: readonly C[]; columns: Record<C, ColumnLayoutDef> },
  options: SchedulesFilterOptions,
): ViewDefs<C, ScheduleFilterKey> {
  return {
    columns,
    filters: { defaultOrder: SCHEDULE_FILTER_ORDER, filters: SCHEDULE_FILTER_DEFS },
    emptyValues: SCHEDULES_EMPTY_VALUES,
    sanitizeValue: optionsSanitizer<ScheduleFilterKey>(
      { type: TYPE_VALUES, status: STATUS_VALUES, owner: options.owners },
      SCHEDULES_EMPTY_VALUES,
    ),
    legacyStorageKeys: SCHEDULES_LEGACY_STORAGE_KEYS,
  };
}
