import { useQuery } from "@tanstack/react-query";
import { preflightScheduleGrants, type SchedulePreflightTableOut } from "@/lib/api";

export interface ScheduleGrantPreflight {
  /** Tables the caller can't grant the scheduler service principals SELECT on. */
  blockedTables: SchedulePreflightTableOut[];
  /** Tables whose catalog or schema the scheduler service principals can't use yet. */
  missingGrantTables: SchedulePreflightTableOut[];
  /** Tables whose access couldn't be checked; ``unverified_reason`` says why. */
  unverifiedTables: SchedulePreflightTableOut[];
  /** True when either list is non-empty — the backend would reject the save. */
  hasGrantIssue: boolean;
  /** True while the preflight is in flight — callers hold the save until it settles. */
  isFetching: boolean;
  /** Re-run the preflight (e.g. once the warehouse has started). */
  retry: () => void;
}

/** Split preflight rows into denied (needs a MANAGE holder), missing scheduler
 *  catalog/schema grants, and unverified (access couldn't be checked). Any kind
 *  blocks the save. */
export function splitPreflightTables(
  tables: readonly SchedulePreflightTableOut[],
): Pick<ScheduleGrantPreflight, "blockedTables" | "missingGrantTables" | "unverifiedTables" | "hasGrantIssue"> {
  const hasMissingGrants = (table: SchedulePreflightTableOut) => (table.missing_grants ?? []).length > 0;
  const unverifiedTables = tables.filter((table) => table.access_unverified);
  const missingGrantTables = tables.filter((table) => !table.access_unverified && hasMissingGrants(table));
  const blockedTables = tables.filter(
    (table) => !table.can_manage && !table.access_unverified && !hasMissingGrants(table),
  );
  const hasGrantIssue = blockedTables.length > 0 || missingGrantTables.length > 0 || unverifiedTables.length > 0;
  return { blockedTables, missingGrantTables, unverifiedTables, hasGrantIssue };
}

/**
 * Schedule-grant preflight shared by every schedule form (table tab,
 * collection tab, New schedule page). Scheduled runs read tables as a
 * service account, so setting a schedule needs the caller to be able to
 * grant those SPs SELECT; the backend hard-blocks the save otherwise.
 *
 * *enabled* should be gated on scheduling intent (a cron is set) as well as
 * edit rights: the preflight issues ownership reads plus paginated
 * grants.get_effective calls per table.
 */
export function useScheduleGrantPreflight(tableFqns: readonly string[], enabled: boolean): ScheduleGrantPreflight {
  const query = useQuery({
    queryKey: ["scheduleGrantPreflight", tableFqns],
    queryFn: async () => (await preflightScheduleGrants({ table_fqns: [...tableFqns] })).data,
    enabled: enabled && tableFqns.length > 0,
    staleTime: 60_000,
  });
  return {
    ...splitPreflightTables(query.data?.tables ?? []),
    isFetching: query.isFetching,
    retry: () => void query.refetch(),
  };
}
