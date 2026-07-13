/**
 * A monitored table's lifecycle actions (save-as-draft, submit, approve,
 * reject, delete) change server-derived state read by several *separate*
 * queries: the Monitored Tables list (status/rules/checks columns), the
 * table's own detail/versions/profile/applied-rules queries, and Data
 * Products' member-table summaries (which read applied-rule/check counts
 * derived from this binding). Each call site used to invalidate its own
 * subset by hand, which is how the overview page's row-level approve/reject
 * ended up missing the row's own detail query key while the detail page's
 * `invalidateCountConsumers` missed nothing but had to be reimplemented.
 *
 * This is the single place listing those query keys so the two call sites
 * (Monitored Tables overview and detail page) can't drift out of sync.
 */
import type { QueryClient } from "@tanstack/react-query";
import { getListMonitoredTablesQueryKey, getListDataProductsQueryKey } from "@/lib/api";

const MONITORED_TABLE_DETAIL_PATH_PREFIX = "/api/v1/monitored-tables/";

/**
 * Invalidate every query that could be showing a stale monitored-table
 * status, version, or rule/check count after a lifecycle action changes it.
 *
 * Covers:
 * - the monitored-tables list (parameter-less key matches every filtered
 *   variant — including any draft/pending-approval queue view — since they
 *   all key off the same `/api/v1/monitored-tables` base path)
 * - Data Products' member-table summaries, which derive counts from this
 *   binding's applied rules
 * - when *bindingId* is known: that binding's detail, versions, profile, and
 *   applied-rules queries, matched by URL-path predicate since they're all
 *   nested under `/api/v1/monitored-tables/{bindingId}`
 */
export function invalidateAfterMonitoredTableChange(queryClient: QueryClient, bindingId?: string): void {
  queryClient.invalidateQueries({ queryKey: getListMonitoredTablesQueryKey() });
  queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
  if (!bindingId) return;
  const detailPrefix = `${MONITORED_TABLE_DETAIL_PATH_PREFIX}${bindingId}`;
  queryClient.invalidateQueries({
    predicate: (query) => {
      const key = query.queryKey[0];
      return typeof key === "string" && key.startsWith(detailPrefix);
    },
  });
}
