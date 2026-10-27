import { hashKey, useQuery, useQueryClient, type QueryClient, type UseMutationOptions } from "@tanstack/react-query";
import axios, { type AxiosError, type AxiosResponse } from "axios";
import {
  preflightScheduleGrants,
  useApplyScheduleGrants,
  type HTTPValidationError,
  type ScheduleGrantApplyIn,
  type ScheduleGrantApplyOut,
  type SchedulePreflightOut,
  type SchedulePreflightTableOut,
} from "@/lib/api";
import { grantRunOutcome, type GrantRunOutcome } from "@/lib/setup-grants";

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
  /** Run the missing catalog/schema grants for *missingGrantTables* with the user's own access. */
  applyGrants: () => void;
  /** True while those grants run. */
  isApplying: boolean;
  /** What went wrong in the last run, or null when nothing did (or nothing ran yet). */
  applyOutcome: GrantRunOutcome | null;
  /** How many grants ran when the check after them failed, or null when it ran. */
  recheckFailed: { applied: number; total: number } | null;
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

/** React Query key of the preflight for *tableFqns*. */
export function scheduleGrantPreflightQueryKey(tableFqns: readonly string[]) {
  return ["scheduleGrantPreflight", tableFqns] as const;
}

/** Table names to send when running the missing grants: only the tables that list some.
 *  The backend re-derives the statements from these names; no SQL is sent. */
export function missingGrantTableFqns(missingGrantTables: readonly SchedulePreflightTableOut[]): string[] {
  return missingGrantTables.map((table) => table.fqn);
}

/** The preflight key a run of the grants was started for, kept on the mutation. */
export interface ScheduleGrantApplyContext {
  queryKey: ReturnType<typeof scheduleGrantPreflightQueryKey>;
}

/**
 * After the grants run, show the backend's fresh check straight away, then refetch it.
 * The check goes to the tables the run started for, even if the schedule's tables
 * changed meanwhile. When the check after the grants failed, the response carries the
 * check from before them, so it isn't shown as current: the check is only refetched.
 */
export function applyScheduleGrantsCallbacks(
  queryClient: QueryClient,
  queryKey: ReturnType<typeof scheduleGrantPreflightQueryKey>,
): UseMutationOptions<
  AxiosResponse<ScheduleGrantApplyOut>,
  AxiosError<HTTPValidationError>,
  { data: ScheduleGrantApplyIn },
  ScheduleGrantApplyContext
> {
  return {
    onMutate: () => ({ queryKey }),
    onSuccess: (response, _variables, context) => {
      if (response.data.recheck_failed) return;
      queryClient.setQueryData<SchedulePreflightOut>(context?.queryKey ?? queryKey, response.data.preflight);
    },
    onSettled: (_data, _error, _variables, context) =>
      queryClient.invalidateQueries({ queryKey: context?.queryKey ?? queryKey }),
  };
}

/** The counts of a run whose re-check failed, so the card can still say how many ran. */
export function scheduleGrantsRecheckFailure(
  result: ScheduleGrantApplyOut,
): { applied: number; total: number } | null {
  return result.recheck_failed ? { applied: result.applied, total: result.total } : null;
}

/** Describe a request that failed before any statement ran. */
export function applyScheduleGrantsFailure(error: unknown): GrantRunOutcome {
  const unavailable = axios.isAxiosError(error) && error.response?.status === 503;
  return { summary: null, failedSql: "", errorCodes: [unavailable ? "warehouse_unavailable" : "unknown"] };
}

/**
 * What the last run of the grants shows for the schedule's current *tableFqns*. A run
 * started for another set of tables shows nothing, so its failed SQL doesn't linger
 * after the tables change.
 */
export function scheduleGrantApplyResult(
  apply: {
    isSuccess: boolean;
    isError: boolean;
    isPending: boolean;
    data: { data: ScheduleGrantApplyOut } | undefined;
    error: unknown;
    context: ScheduleGrantApplyContext | undefined;
  },
  tableFqns: readonly string[],
): Pick<ScheduleGrantPreflight, "applyOutcome" | "recheckFailed" | "isApplying"> {
  const current =
    !!apply.context && hashKey(apply.context.queryKey) === hashKey(scheduleGrantPreflightQueryKey(tableFqns));
  if (!current) return { applyOutcome: null, recheckFailed: null, isApplying: false };
  let applyOutcome: GrantRunOutcome | null = null;
  if (apply.isSuccess && apply.data) applyOutcome = grantRunOutcome(apply.data.data);
  else if (apply.isError) applyOutcome = applyScheduleGrantsFailure(apply.error);
  return {
    applyOutcome,
    recheckFailed: apply.isSuccess && apply.data ? scheduleGrantsRecheckFailure(apply.data.data) : null,
    isApplying: apply.isPending,
  };
}

/**
 * Schedule-grant preflight shared by every schedule form (table tab,
 * collection tab, New schedule page). Scheduled runs read tables as a
 * service principal, so setting a schedule needs the caller to be able to
 * grant those SPs SELECT; the backend hard-blocks the save otherwise.
 *
 * *enabled* should be gated on scheduling intent (a cron is set) as well as
 * edit rights: the preflight issues ownership reads plus paginated
 * grants.get_effective calls per table.
 *
 * The missing catalog/schema grants only run when the user asks for it
 * (*applyGrants*); saving a schedule never runs them.
 */
export function useScheduleGrantPreflight(tableFqns: readonly string[], enabled: boolean): ScheduleGrantPreflight {
  const queryClient = useQueryClient();
  const queryKey = scheduleGrantPreflightQueryKey(tableFqns);
  const query = useQuery({
    queryKey,
    queryFn: async () => (await preflightScheduleGrants({ table_fqns: [...tableFqns] })).data,
    enabled: enabled && tableFqns.length > 0,
    staleTime: 60_000,
  });
  const apply = useApplyScheduleGrants({ mutation: applyScheduleGrantsCallbacks(queryClient, queryKey) });
  const split = splitPreflightTables(query.data?.tables ?? []);
  return {
    ...split,
    ...scheduleGrantApplyResult(apply, tableFqns),
    isFetching: query.isFetching,
    retry: () => {
      // A fresh check replaces the outcome of the last run.
      apply.reset();
      void query.refetch();
    },
    applyGrants: () => apply.mutate({ data: { table_fqns: missingGrantTableFqns(split.missingGrantTables) } }),
  };
}
