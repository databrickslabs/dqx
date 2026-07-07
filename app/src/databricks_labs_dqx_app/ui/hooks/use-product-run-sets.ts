import { useEffect } from "react";
import { useListRunSets, type RunSetSummaryOut } from "@/lib/api";

/** Shared page size for the header's activity probe and the Runs tab's
 *  listing — MUST stay identical across every caller so React Query
 *  dedupes them into a single cached query (same queryKey) instead of
 *  each mounting its own poll. Ported from dqlake's
 *  `useDataProductRunsActivity` pattern, adapted to run sets. */
const SHARED_LIMIT = 50;

/**
 * Single shared `listRunSets` query for a data product, consumed by both
 * `ProductHeader` (Run now/Run draft busy state) and `ProductRunsTab`
 * (the run-set listing). Because both call this hook with the exact same
 * params, React Query serves them from one cached query — fixing the
 * ~25 duplicate `GET /run-sets` requests per page view measured when the
 * header polled independently.
 *
 * Polls every 4s ONLY while a run set is `running` (or the caller passes
 * `extraPoll` to bridge the gap right after a Run now submit) and only
 * while the tab/window is visible — idle otherwise.
 */
export function useProductRunSets(productId: string, { extraPoll = false }: { extraPoll?: boolean } = {}) {
  const { data, refetch, isLoading } = useListRunSets(
    { product_id: productId, limit: SHARED_LIMIT },
    { query: { select: (d) => d.data, staleTime: 0 } },
  );

  const runSets: RunSetSummaryOut[] = data ?? [];
  const hasActive = runSets.some((rs) => rs.status === "running");
  const shouldPoll = hasActive || extraPoll;

  useEffect(() => {
    if (!shouldPoll) return;
    const tick = () => {
      if (document.visibilityState === "visible") void refetch();
    };
    const id = window.setInterval(tick, 4000);
    return () => window.clearInterval(id);
  }, [shouldPoll, refetch]);

  return { runSets, hasActive, isLoading, refetch };
}
