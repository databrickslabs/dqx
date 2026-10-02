/**
 * Pure list logic for the Schedules overview (`/schedules`): row identity,
 * health classification, filter matching, client-sort values, and the
 * "next run" relative-time breakdown. Kept free of React so it can be unit
 * tested with `bun test` (see `schedule-list.test.ts`).
 */
import type { FilterLayoutDef } from "@/components/data-table/filter-layout";
import type { SortValue } from "@/components/data-table/sort";
import type { ScheduleOverviewOut } from "@/lib/api";
import { parseServerDate } from "@/lib/format-utils";

/** Sentinel "no filter" value shared by every Schedules filter control. */
export const SCHEDULE_FILTER_ALL = "all";

export type ScheduleSourceType = ScheduleOverviewOut["source_type"];

/** Health of a schedule as shown in the Status column and Status filter. */
export type ScheduleHealth = "active" | "paused" | "failed" | "partial";

/** Values of the Status filter. `failing` groups failed + partial failures. */
export type ScheduleStatusFilter = typeof SCHEDULE_FILTER_ALL | "active" | "paused" | "failing";

export interface ScheduleFilters {
  search: string;
  type: typeof SCHEDULE_FILTER_ALL | ScheduleSourceType;
  status: ScheduleStatusFilter;
  owner: string;
}

export const EMPTY_SCHEDULE_FILTERS: ScheduleFilters = {
  search: "",
  type: SCHEDULE_FILTER_ALL,
  status: SCHEDULE_FILTER_ALL,
  owner: SCHEDULE_FILTER_ALL,
};

/** Stable identity for a schedule row: a table and a collection can share an
 *  id space, so the source type is part of the key. */
export function scheduleRowKey(row: Pick<ScheduleOverviewOut, "source_type" | "source_id">): string {
  return `${row.source_type}:${row.source_id}`;
}

/** Paused (or disabled) wins over the last run's outcome: a paused schedule
 *  is not going to run, so its last failure is not what needs attention. */
export function scheduleHealth(row: ScheduleOverviewOut): ScheduleHealth {
  if (row.paused || row.enabled === false) return "paused";
  if (row.run_status === "failed") return "failed";
  if (row.run_status === "partial_failure") return "partial";
  return "active";
}

export function matchesScheduleFilters(row: ScheduleOverviewOut, filters: ScheduleFilters): boolean {
  if (filters.type !== SCHEDULE_FILTER_ALL && row.source_type !== filters.type) return false;
  if (filters.owner !== SCHEDULE_FILTER_ALL && (row.owner ?? "") !== filters.owner) return false;
  const health = scheduleHealth(row);
  if (filters.status === "active" && health !== "active") return false;
  if (filters.status === "paused" && health !== "paused") return false;
  if (filters.status === "failing" && health !== "failed" && health !== "partial") return false;
  const q = filters.search.trim().toLowerCase();
  if (!q) return true;
  return [row.name, row.target, row.owner, row.updated_by].some((value) => value?.toLowerCase().includes(q));
}

/** The filter pills, as listed in Edit Columns > Filters. */
export type ScheduleFilterKey = keyof ScheduleFilters;

export const SCHEDULE_FILTER_ORDER: readonly ScheduleFilterKey[] = ["search", "type", "status", "owner"];

export const SCHEDULE_FILTER_DEFS: Record<ScheduleFilterKey, FilterLayoutDef> = {
  search: { labelKey: "schedules.filterSearch", defaultVisible: true },
  type: { labelKey: "schedules.type", defaultVisible: true },
  status: { labelKey: "schedules.health", defaultVisible: true },
  owner: { labelKey: "schedules.owner", defaultVisible: true },
};

/** Clears one filter back to "no filter" — used when its pill is hidden, so a
 *  hidden filter can never keep narrowing the rows. */
export function resetScheduleFilter(filters: ScheduleFilters, key: ScheduleFilterKey): ScheduleFilters {
  return { ...filters, [key]: EMPTY_SCHEDULE_FILTERS[key] };
}

export function hasActiveScheduleFilters(filters: ScheduleFilters): boolean {
  return (
    filters.search.trim() !== "" ||
    filters.type !== SCHEDULE_FILTER_ALL ||
    filters.status !== SCHEDULE_FILTER_ALL ||
    filters.owner !== SCHEDULE_FILTER_ALL
  );
}

/** Distinct, sorted owners across the rows, for the Owner filter options. */
export function scheduleOwners(rows: readonly ScheduleOverviewOut[]): string[] {
  return Array.from(new Set(rows.map((row) => row.owner).filter((value): value is string => !!value))).sort();
}

export type ScheduleSortKey =
  | "name"
  | "type"
  | "cadence"
  | "kind"
  | "owner"
  | "health"
  | "lastRun"
  | "nextRun"
  | "updatedAt";

/** Status sort rank: a first-click ASC sort leads with the schedules that
 *  need attention (failed, then partial), then healthy, then paused. */
const HEALTH_RANK: Record<ScheduleHealth, number> = { failed: 0, partial: 1, active: 2, paused: 3 };

const TYPE_RANK: Record<ScheduleSourceType, number> = { table: 0, collection: 1, scope: 2 };

function timeOf(iso: string | null | undefined): number | null {
  return parseServerDate(iso)?.getTime() ?? null;
}

/** Comparable value for a column + row; `null` marks a missing value that
 *  `compareSortValues` pins per the column's `nullsFirst` flag. */
export function getScheduleSortValue(key: ScheduleSortKey, row: ScheduleOverviewOut): SortValue {
  switch (key) {
    case "name":
      return row.name.toLowerCase() || null;
    case "type":
      return TYPE_RANK[row.source_type];
    case "cadence":
      return (row.cron ?? row.frequency ?? "").toLowerCase() || null;
    case "kind":
      return row.schedule_kind ?? null;
    case "owner":
      return (row.owner ?? "").toLowerCase() || null;
    case "health":
      return HEALTH_RANK[scheduleHealth(row)];
    case "lastRun":
      return timeOf(row.last_run_at);
    case "nextRun":
      return timeOf(row.next_run_at);
    case "updatedAt":
      return timeOf(row.updated_at);
  }
}

/** Relative time-until breakdown for a next-run timestamp ("in 5m" /
 *  "in 3h" / "in 2d"); anything at or before *now* is due. */
export type RelativeFutureParts = { key: "dueNow" } | { key: "inMinutes" | "inHours" | "inDays"; count: number };

export function getRelativeFutureParts(
  iso: string | null | undefined,
  now: number = Date.now(),
): RelativeFutureParts | null {
  const d = parseServerDate(iso);
  if (!d) return null;
  const minutes = Math.floor((d.getTime() - now) / 60000);
  if (minutes < 1) return { key: "dueNow" };
  if (minutes < 60) return { key: "inMinutes", count: minutes };
  if (minutes < 24 * 60) return { key: "inHours", count: Math.floor(minutes / 60) };
  return { key: "inDays", count: Math.floor(minutes / (24 * 60)) };
}

/** Table FQNs that a scheduled collection already runs — a table schedule on
 *  one of these runs its checks a second time. */
export function tablesScheduledByCollections(
  products: readonly { schedule_cron?: string | null; members?: readonly { table_fqn: string }[] | null }[],
): Set<string> {
  return new Set(
    products
      .filter((product) => !!product.schedule_cron)
      .flatMap((product) => (product.members ?? []).map((member) => member.table_fqn)),
  );
}
