import { useCallback, useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useGetRunSet } from "@/lib/api";
import { invalidateResultsAfterRunCompletion } from "@/lib/results-invalidation";

/** sessionStorage namespace for the run set a monitored-table detail page is
 *  actively tracking, keyed by binding id so the signal survives a reload —
 *  the old in-component `trackedRunSetId` state did not, so the spinner and
 *  in-progress banner vanished if you refreshed mid-run. */
const STORAGE_PREFIX = "dqx-mt-active-run:";

function storageKey(bindingId: string): string {
  return `${STORAGE_PREFIX}${bindingId}`;
}

function readStored(bindingId: string): string | null {
  try {
    return sessionStorage.getItem(storageKey(bindingId));
  } catch {
    return null;
  }
}

function writeStored(bindingId: string, runSetId: string | null): void {
  try {
    if (runSetId) sessionStorage.setItem(storageKey(bindingId), runSetId);
    else sessionStorage.removeItem(storageKey(bindingId));
  } catch {
    // sessionStorage unavailable (private mode / quota) — degrade to
    // in-memory tracking only; the signal simply won't survive a reload.
  }
}

/**
 * Binding-scoped "run in progress" signal for the monitored-table detail page.
 *
 * The table-space equivalent is {@link import("./use-product-run-sets").useProductRunSets};
 * this mirrors it for a single binding. It tracks the run set the page most
 * recently triggered — persisted per binding in sessionStorage so it survives
 * a reload — polls that run set's status every 4s while it is `running` (and
 * only while the tab is visible), then clears itself and refreshes the table's
 * results once the run settles. Every consumer (the Run button spinner/disable
 * state and the Results-tab in-progress banner) reads the one `hasActive` this
 * returns, and because the poll is a single `useGetRunSet(activeRunSetId)`
 * query they share one cached request (deduped by query key) instead of each
 * mounting a poll of its own.
 *
 * This replaces the page's old in-component `trackedRunSetId` state, which was
 * lost on reload and lived inline in the route rather than in a shared hook.
 *
 * Known limitation: a binding-direct run set carries no product id (see
 * `BindingRunService`) and there is no binding-scoped run-set listing endpoint,
 * so this only tracks runs triggered from THIS page in THIS session (persisted
 * across reloads). Surfacing runs started elsewhere — a scheduled run, or one
 * triggered from the parent table space — would need a new backend endpoint.
 */
export function useMonitoredTableRunActivity(
  bindingId: string,
  { tableFqn }: { tableFqn?: string } = {},
): { hasActive: boolean; activeRunSetId: string | null; registerRun: (runSetId: string) => void } {
  const queryClient = useQueryClient();
  const [activeRunSetId, setActiveRunSetId] = useState<string | null>(() => readStored(bindingId));

  // Re-seed from storage when the binding changes (navigating between two
  // monitored-table detail pages reuses this hook instance).
  const seededBindingRef = useRef(bindingId);
  useEffect(() => {
    if (seededBindingRef.current !== bindingId) {
      seededBindingRef.current = bindingId;
      setActiveRunSetId(readStored(bindingId));
    }
  }, [bindingId]);

  const runSetQuery = useGetRunSet(activeRunSetId ?? "", {
    query: {
      enabled: activeRunSetId != null,
      refetchInterval: 4000,
      refetchIntervalInBackground: false,
      staleTime: 0,
    },
  });
  const status = activeRunSetId != null ? runSetQuery.data?.data?.status : undefined;
  // A 404 (stale/rolled-back run set) is terminal for our purposes — treat it
  // like a settled run so `hasActive` can't get stuck true forever.
  const errored = activeRunSetId != null && runSetQuery.isError;

  // Terminal (success/failed/canceled/errored) → stop tracking (dropping the
  // id disables the poll and clears storage) and refresh this table's
  // results/score so the tab reflects the finished run without a reload.
  useEffect(() => {
    if (activeRunSetId == null) return;
    if (!errored && (status == null || status === "running")) return;
    setActiveRunSetId(null);
    writeStored(bindingId, null);
    invalidateResultsAfterRunCompletion(queryClient, tableFqn ? [tableFqn] : undefined);
  }, [status, errored, activeRunSetId, bindingId, tableFqn, queryClient]);

  /** Register a just-submitted run set as the one to track to completion. */
  const registerRun = useCallback(
    (runSetId: string) => {
      writeStored(bindingId, runSetId);
      setActiveRunSetId(runSetId);
    },
    [bindingId],
  );

  // Active while we hold a tracked run set whose status is `running` — or not
  // yet fetched (just registered), which bridges the gap before the first poll
  // so the button/banner don't flicker off for a cycle.
  const hasActive =
    activeRunSetId != null && !errored && (status === undefined || status === "running");

  return { hasActive, activeRunSetId, registerRun };
}
