import { useQuery } from "@tanstack/react-query";
import { preflightScheduleGrants, type SchedulePreflightTableOut } from "@/lib/api";

export interface ScheduleGrantPreflight {
  /** Tables the caller can't grant the scheduler service principals SELECT on. */
  blockedTables: SchedulePreflightTableOut[];
  /** True while the preflight is in flight — callers hold the save until it settles. */
  isFetching: boolean;
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
    blockedTables: (query.data?.tables ?? []).filter((table) => !table.can_manage),
    isFetching: query.isFetching,
  };
}
