/**
 * Run-completion invalidation for the DQ score / failing-records queries.
 *
 * Score and quarantine-sample data only changes when a validation run
 * finishes, so those queries are configured to never refetch on their own
 * (`staleTime: Infinity`, no window-focus refetch, no polling). The ONLY
 * refresh trigger is this helper, called from the places that already
 * observe a run reaching a terminal state:
 *
 * - `routes/_sidebar/runs-history.tsx` — polls RUNNING validation runs and
 *   sees each one settle (knows the run's `source_table_fqn`)
 * - `hooks/use-product-run-sets.ts` — polls a data product's run sets and
 *   sees each set settle (member table FQNs unknown at summary level, so
 *   it invalidates broadly)
 *
 * A finished run for ONE table moves every aggregate above it (global,
 * product, and rule scores), so all `/api/v1/dq-score/*` AND all
 * `/api/v1/dq-results/*` queries (runs / table / product / global / rule
 * breakdowns, failed rows, registries) are always invalidated by path
 * prefix; quarantine samples are per-table, so they are narrowed to the
 * finished tables' exact paths when the caller knows them. Tasks 10-12
 * (global/product/rule score views) and the Phase 2 dqlake-port results
 * tabs reuse this helper as-is.
 */
import type { QueryClient } from "@tanstack/react-query";
import { getGetQuarantineSampleQueryKey } from "@/lib/api";

/**
 * Query options for every score / failing-records query covered by this
 * module: the data only changes when a validation run finishes, so the
 * queries never refetch on their own — `invalidateResultsAfterRunCompletion`
 * below is their ONLY refresh trigger. Spread into the generated hooks'
 * `query` options (table Results tab, product Results tab, …).
 */
export const RESULTS_QUERY_OPTIONS = {
  staleTime: Infinity,
  refetchOnWindowFocus: false,
} as const;

/** Path prefix shared by the global, table, product, and rule score endpoints. */
export const DQ_SCORE_PATH_PREFIX = "/api/v1/dq-score/";
/** Path prefix shared by the dqlake-shape results endpoints (runs, table /
 *  product / global / rule breakdowns+trends, filtered failed rows, and the
 *  severity/dimension registries). Aggregates over many tables, so it is
 *  always invalidated wholesale, like the score prefix. */
export const DQ_RESULTS_PATH_PREFIX = "/api/v1/dq-results/";
/** Path prefix of the per-table failing-records sample endpoint. */
export const QUARANTINE_SAMPLE_PATH_PREFIX = "/api/v1/quarantine-samples/";

export interface ResultsInvalidationMatcher {
  /** Query-key paths matched by `startsWith`. */
  pathPrefixes: string[];
  /** Query-key paths matched by exact equality (per-table quarantine samples). */
  exactPaths: string[];
}

/**
 * Builds the matcher for one run-completion event. With *tableFqns*, the
 * quarantine-sample invalidation narrows to exactly those tables; without,
 * every quarantine-sample query is invalidated. Score queries are always
 * matched wholesale (see module doc).
 */
export function buildResultsInvalidationMatcher(tableFqns?: readonly string[]): ResultsInvalidationMatcher {
  if (!tableFqns || tableFqns.length === 0) {
    return {
      pathPrefixes: [DQ_SCORE_PATH_PREFIX, DQ_RESULTS_PATH_PREFIX, QUARANTINE_SAMPLE_PATH_PREFIX],
      exactPaths: [],
    };
  }
  return {
    pathPrefixes: [DQ_SCORE_PATH_PREFIX, DQ_RESULTS_PATH_PREFIX],
    exactPaths: tableFqns.map((fqn) => getGetQuarantineSampleQueryKey(fqn)[0]),
  };
}

/**
 * True when a React Query key belongs to a score / quarantine-sample query
 * covered by *matcher*. Orval keys put the request path first (an optional
 * params object follows), so only `queryKey[0]` is inspected — exact
 * equality for per-table paths deliberately ignores the params element so
 * every `limit` variant of a sample query is invalidated together.
 */
export function matchesResultsInvalidation(
  queryKey: readonly unknown[],
  matcher: ResultsInvalidationMatcher,
): boolean {
  const path = queryKey[0];
  if (typeof path !== "string") return false;
  return matcher.pathPrefixes.some((prefix) => path.startsWith(prefix)) || matcher.exactPaths.includes(path);
}

/**
 * Invalidate every score and failing-records query affected by a finished
 * run. Pass the finished runs' table FQNs when known (narrows the
 * quarantine-sample invalidation); omit them to invalidate broadly.
 */
export function invalidateResultsAfterRunCompletion(
  queryClient: QueryClient,
  tableFqns?: readonly string[],
): void {
  const matcher = buildResultsInvalidationMatcher(tableFqns);
  void queryClient.invalidateQueries({
    predicate: (query) => matchesResultsInvalidation(query.queryKey, matcher),
  });
}
