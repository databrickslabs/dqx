/**
 * Approving or rejecting a registry rule can make the backend
 * re-materialize any monitored tables the rule is applied to — e.g.
 * auto-upgrading an approved override or dropping a binding's
 * `pending_approval` state (see `RegistryService.approve` /
 * `RegistryService.reject`). The approve/reject response doesn't tell the
 * client which bindings were touched, so every call site that fires those
 * mutations needs to invalidate the *entire* monitored-tables surface, not
 * just the registry rule itself.
 *
 * This is the single place listing those query keys so the three call
 * sites (Rules Registry detail page, Rules Registry list page, and the
 * Drafts & Review approval queue) can't drift out of sync with each other.
 */
import type { QueryClient } from "@tanstack/react-query";
import { getListRegistryRulesQueryKey, getListMonitoredTablesQueryKey } from "@/lib/api";

const MONITORED_TABLES_PATH_PREFIX = "/api/v1/monitored-tables/";

/**
 * Invalidate every query that could be showing a stale rule-set, version,
 * or status after a registry rule's approval state changes.
 *
 * Covers:
 * - the registry rules list (parameter-less key matches every filtered
 *   variant, including the Drafts & Review pending-approval queue, since
 *   both key off the same `/api/v1/registry-rules` base path)
 * - the monitored-tables list (all filter variants)
 * - every monitored-table detail, versions, and applied-rules query,
 *   matched by URL-path predicate since we don't know which specific
 *   binding IDs were re-materialized
 */
export function invalidateAfterRegistryRuleApprovalChange(queryClient: QueryClient): void {
  queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() });
  queryClient.invalidateQueries({ queryKey: getListMonitoredTablesQueryKey() });
  queryClient.invalidateQueries({
    predicate: (query) => {
      const key = query.queryKey[0];
      return typeof key === "string" && key.startsWith(MONITORED_TABLES_PATH_PREFIX);
    },
  });
}
