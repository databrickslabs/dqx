import { useEffect, useMemo, useRef } from "react";
import { useNavigate } from "@tanstack/react-router";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";
import { useListValidationRuns } from "@/lib/api-custom";
import { useListProfileRuns } from "@/lib/api";
import {
  runFailureTableLabel,
  selectFailureToasts,
  type RunFailureCandidate,
} from "@/lib/run-failure-toasts";

/**
 * App-wide watcher that fires a toast when a validation or profiling run
 * transitions to FAILED (item 58).
 *
 * Mounted once in the sidebar layout (`_sidebar/route.tsx`) so it lives for the
 * authenticated session and surfaces failures on any page — not just Runs
 * History, which only polls while it's mounted.
 *
 * Cheap by construction:
 *   - Reuses the SAME list hooks Runs History uses, so it shares React Query's
 *     cache rather than issuing bespoke fetches.
 *   - Refetches on a 5s interval ONLY while ≥1 run is RUNNING and the document
 *     is visible (`refetchInterval` returns `false` otherwise), mirroring Runs
 *     History's poll guard. When everything is settled it goes idle.
 *
 * Dedup + storm avoidance live in the pure `selectFailureToasts` helper: the
 * first pass seeds every already-terminal run as handled (no load-time storm),
 * and a session-lifetime ref of seen run keys guarantees each failed run toasts
 * exactly once — never again on re-poll or remount.
 */
export function useRunFailureToasts(): void {
  const { t } = useTranslation();
  const navigate = useNavigate();

  const hasRunningRef = useRef(false);
  const refetchInterval = () =>
    hasRunningRef.current && document.visibilityState === "visible" ? 5000 : (false as const);

  // Served from the shared React Query cache across navigations so the two run-list
  // queries don't refetch on every page mount (this hook is mounted app-wide in the
  // sidebar layout). The RUNNING-poll (refetchInterval, below) still drives live
  // updates while a run is in flight — staleTime does not affect interval polling.
  const RUNS_STALE_TIME = 30 * 60 * 1000; // 30 min
  const { data: validationResp } = useListValidationRuns({
    query: { refetchInterval, staleTime: RUNS_STALE_TIME },
  });
  const { data: profileResp } = useListProfileRuns(undefined, {
    query: { refetchInterval, staleTime: RUNS_STALE_TIME },
  });

  const candidates: RunFailureCandidate[] = useMemo(() => {
    const validation = Array.isArray(validationResp?.data)
      ? validationResp.data.map(
          (r): RunFailureCandidate => ({
            key: `validation:${r.run_id}`,
            run_id: r.run_id,
            kind: "validation",
            status: r.status,
            source_table_fqn: r.source_table_fqn,
          }),
        )
      : [];
    const profiling = Array.isArray(profileResp?.data)
      ? profileResp.data.map(
          (r): RunFailureCandidate => ({
            key: `profiling:${r.run_id}`,
            run_id: r.run_id,
            kind: "profiling",
            status: r.status ?? null,
            source_table_fqn: r.source_table_fqn,
          }),
        )
      : [];
    return [...validation, ...profiling];
  }, [validationResp, profileResp]);

  // Keep the poll gate in sync with the current listing.
  hasRunningRef.current = candidates.some((r) => r.status === "RUNNING");

  // Session-lifetime dedup state. `seeded` flips true after the first pass with
  // data, so the initial burst of already-terminal runs is absorbed silently.
  const seenRef = useRef<Set<string>>(new Set());
  const seededRef = useRef(false);

  useEffect(() => {
    // Wait until BOTH listings have returned before seeding — otherwise the
    // faster query alone seeds the "handled" set, and when the slower query
    // resolves its pre-existing FAILED runs are unseen and storm-toast the
    // backlog (the exact thing seeding prevents). Must be `||`, not `&&`: hold
    // the guard while EITHER is still undefined.
    if (validationResp === undefined || profileResp === undefined) return;

    const { toToast, seen } = selectFailureToasts(candidates, seenRef.current, seededRef.current);
    seenRef.current = seen;
    seededRef.current = true;

    for (const run of toToast) {
      const tableFqn = run.source_table_fqn;
      toast.error(
        t("runsHistory.failureToastTitle", { table: runFailureTableLabel(tableFqn) }),
        {
          duration: 9000,
          action: {
            label: t("runsHistory.failureToastAction"),
            onClick: () => void navigate({ to: "/runs-history", search: { tableFqn } }),
          },
        },
      );
    }
  }, [candidates, validationResp, profileResp, t, navigate]);
}
