/**
 * Pure helpers for the New schedule flow (`/schedules/new`): normalizing a
 * picked monitored table or collection into one `ScheduleTarget` shape, and
 * the picker's search match. React-free so it can be unit tested.
 */
import type { ScheduleKind } from "@/components/common/ScheduleEditor";
import type { DataProductOut, MonitoredTableOut } from "@/lib/api";

/** Prefix of synthetic cross-table-check members, which have no source table. */
const SQL_CHECK_PREFIX = "__sql_check__/";

/** The schedule a target already carries, used to seed the form. */
export interface ExistingSchedule {
  cron: string;
  timezone: string;
  kind: ScheduleKind | null;
  /** Rows each run samples; 0 = the whole table. */
  sampleSize: number;
}

export interface ScheduleTarget {
  kind: "table" | "collection";
  /** binding_id for tables, product_id for collections. */
  id: string;
  name: string;
  /** Secondary line: the table FQN, or the collection's member count. */
  detail: string;
  owner: string | null;
  /** Real tables the schedule reads — what the grant preflight checks. */
  tableFqns: string[];
  existing: ExistingSchedule | null;
}

function existingSchedule(entity: {
  schedule_cron?: string | null;
  schedule_tz?: string | null;
  schedule_kind?: ScheduleKind | null;
  schedule_sample_size?: number | null;
}): ExistingSchedule | null {
  if (!entity.schedule_cron) return null;
  return {
    cron: entity.schedule_cron,
    timezone: entity.schedule_tz || "UTC",
    kind: entity.schedule_kind ?? null,
    sampleSize: entity.schedule_sample_size ?? 0,
  };
}

export function tableTarget(table: MonitoredTableOut): ScheduleTarget {
  return {
    kind: "table",
    id: table.binding_id,
    name: table.table_fqn.split(".").pop() || table.table_fqn,
    detail: table.table_fqn,
    owner: table.owner_display_name || table.owner || null,
    tableFqns: [table.table_fqn],
    existing: existingSchedule(table),
  };
}

export function collectionTarget(product: DataProductOut, detail: string): ScheduleTarget {
  return {
    kind: "collection",
    id: product.product_id,
    name: product.name,
    detail,
    owner: product.owner_display_name || product.owner || null,
    tableFqns: (product.members ?? [])
      .map((member) => member.table_fqn)
      .filter((fqn) => !!fqn && !fqn.startsWith(SQL_CHECK_PREFIX)),
    existing: existingSchedule(product),
  };
}

/** Case-insensitive substring match over the target's visible text. */
export function matchesTargetSearch(target: ScheduleTarget, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return [target.name, target.detail, target.owner].some((value) => value?.toLowerCase().includes(q));
}
