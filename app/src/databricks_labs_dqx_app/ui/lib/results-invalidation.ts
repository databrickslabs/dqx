/**
 * Run-completion invalidation for the DQ score / failing-records queries.
 *
 * Score and failing-rows data only changes when a validation run finishes,
 * so those queries are configured to never refetch on their own
 * (`staleTime: Infinity`, no window-focus refetch, no polling). The refresh
 * triggers are the helpers in this module, called from the places that
 * already observe the underlying change:
 *
 * - `routes/_sidebar/runs-history.tsx` — polls RUNNING validation runs and
 *   sees each one settle (knows the run's `source_table_fqn`)
 * - `hooks/use-product-run-sets.ts` — polls a data product's run sets and
 *   sees each set settle (member table FQNs unknown at summary level)
 * - the rule apply/unapply mutation call sites (the monitored-table detail
 *   page's staged-save/submit/delete flows and the registry ApplyRuleModal)
 *   — via `invalidateResultsAfterRuleApplicationChange`, since applying a
 *   rule changes score aggregates (e.g. `applied_to_count`) without any run
 *
 * A finished run for ONE table moves every aggregate above it (global,
 * product, and rule scores), so all `/api/v1/dq-score/*`, `/api/v1/dq-results/*`
 * (runs / table / product / global / rule breakdowns, filtered failed rows,
 * registries) and `/api/v1/home/*` queries are always invalidated wholesale
 * by path prefix. Tasks 10-12 (global/product/rule score views) and the
 * Phase 2 dqlake-port results tabs reuse this helper as-is.
 *
 * P3.4 addition: when the finished tables ARE known, the run-completion
 * helper also fires a fire-and-forget `POST /api/v1/dq-results/refresh-scores`
 * so the server-side `dq_score_cache` (the list pages' instant score
 * columns) is recomputed at the same moments — see
 * `triggerScoreCacheRefresh`. Broad invalidations skip it (never a
 * refresh-all).
 */
import type { QueryClient } from "@tanstack/react-query";
import { refreshDqScores } from "@/lib/api";

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
/** Path prefix of the homepage stats endpoint (P3.5): its score card is the
 *  cached global aggregate, so it is score-shaped and refreshes on the same
 *  run-completion / rule-application events as the dq-score queries. */
export const HOME_STATS_PATH_PREFIX = "/api/v1/home/";

/**
 * Every query-key path prefix covered by the results invalidation. Run
 * completion and rule-application changes both move the aggregates behind
 * all three, so both events invalidate the same set, wholesale.
 */
export const RESULTS_INVALIDATION_PATH_PREFIXES = [
  DQ_SCORE_PATH_PREFIX,
  DQ_RESULTS_PATH_PREFIX,
  HOME_STATS_PATH_PREFIX,
] as const;

/**
 * True when a React Query key belongs to a score / results query covered by
 * this module. Orval keys put the request path first (an optional params
 * object follows), so only `queryKey[0]` is inspected — every params variant
 * of a query is invalidated together.
 */
export function matchesResultsInvalidation(queryKey: readonly unknown[]): boolean {
  const path = queryKey[0];
  if (typeof path !== "string") return false;
  return RESULTS_INVALIDATION_PATH_PREFIXES.some((prefix) => path.startsWith(prefix));
}

/** Server-side cap on one refresh-scores call (`RefreshScoresIn.table_fqns`). */
export const REFRESH_SCORES_MAX_FQNS = 100;

/**
 * Query-key paths of the list queries that render the cached DQ score
 * columns (P3.4): the monitored-tables and table-spaces list endpoints
 * LEFT JOIN `dq_score_cache`, so they are re-fetched once a score-cache
 * recompute lands. Exact first-element matches — detail-page queries
 * (`/api/v1/monitored-tables/{id}`) have a different path element and are
 * deliberately untouched.
 */
export const SCORE_CACHE_LIST_PATHS = [
  "/api/v1/monitored-tables",
  "/api/v1/data-products",
  // The homepage stats endpoint serves the cached GLOBAL score row, so it
  // must re-read once a recompute lands too (P3.5).
  "/api/v1/home/stats",
] as const;

/**
 * Fire-and-forget score-cache recompute for the just-finished tables
 * (`POST /api/v1/dq-results/refresh-scores` — recomputes those tables,
 * every table space containing them, and the global rollup, server-side).
 * Skipped entirely on broad invalidations (no FQNs known): the endpoint is
 * a scoped run-completion trigger, never a refresh-all. Once the recompute
 * lands, the score-cache-backed list queries are invalidated so a mounted
 * list re-reads the fresh cache; failures are swallowed — the cache just
 * stays stale until the next completed run.
 */
export function triggerScoreCacheRefresh(queryClient: QueryClient, tableFqns?: readonly string[]): void {
  if (!tableFqns || tableFqns.length === 0) return;
  void refreshDqScores({ table_fqns: tableFqns.slice(0, REFRESH_SCORES_MAX_FQNS) })
    .then(() => {
      for (const path of SCORE_CACHE_LIST_PATHS) {
        void queryClient.invalidateQueries({ queryKey: [path] });
      }
      // Re-invalidate the results/score BREAKDOWN queries a SECOND time, now
      // that the server-side recompute has landed. The caller already
      // invalidated them once at run-settle, but that fired BEFORE this
      // refresh-scores round-trip finished — so those queries refetched
      // against not-yet-recomputed server data, got stale numbers, and (with
      // `staleTime: Infinity`) cached them until a manual page reload. This is
      // the bug where the Results tab "refreshes" on run completion but shows
      // the previous run's results. Invalidating again here forces a final
      // refetch against the fresh data. (Rule-application changes recompute
      // synchronously in-request, so they don't need this second pass.)
      void queryClient.invalidateQueries({
        predicate: (query) => matchesResultsInvalidation(query.queryKey),
      });
    })
    .catch(() => {
      // Fire-and-forget: a failed refresh only leaves the cached score
      // columns stale; the next run completion covers it.
    });
}

/**
 * Invalidate every score and failing-records query affected by a finished
 * run. Pass the finished runs' table FQNs when known — they trigger the
 * server-side score-cache recompute; omit them to invalidate without the
 * recompute (the query invalidation itself is always prefix-wide).
 */
export function invalidateResultsAfterRunCompletion(
  queryClient: QueryClient,
  tableFqns?: readonly string[],
): void {
  void queryClient.invalidateQueries({
    predicate: (query) => matchesResultsInvalidation(query.queryKey),
  });
  triggerScoreCacheRefresh(queryClient, tableFqns);
}

/**
 * Invalidate the score/results aggregates after a rule's APPLICATIONS change
 * — applying or unapplying a registry rule on a monitored table (including
 * severity overrides and deleting a monitored table, which unapplies
 * everything on it). Without this, the rule score's `applied_to_count`
 * (staleTime Infinity) keeps the registry rule's Results tab stale-disabled
 * until a full reload. These are rare admin actions, so the broad
 * prefix-wide invalidation is deliberate.
 */
export function invalidateResultsAfterRuleApplicationChange(queryClient: QueryClient): void {
  void queryClient.invalidateQueries({
    predicate: (query) => matchesResultsInvalidation(query.queryKey),
  });
}
