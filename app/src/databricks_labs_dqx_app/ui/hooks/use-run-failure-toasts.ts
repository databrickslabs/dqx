import { useEffect, useMemo, useRef } from "react";
import { useNavigate } from "@tanstack/react-router";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";
import { useListRecentValidationFailures, useListRecentProfileFailures } from "@/lib/api";
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
 *   - Polls two lightweight ``recent-failures`` endpoints that return FAILED
 *     runs only with minimal fields (run_id, source_table_fqn, status,
 *     created_at). No full run-history fetch on every page load.
 *   - Refetches on a 60s interval with background polling disabled so the tab
 *     doesn't issue requests when not visible. Failures are not
 *     time-critical — 60s detection lag is acceptable.
 *
 * Dedup + storm avoidance live in the pure `selectFailureToasts` helper: the
 * first pass seeds every already-terminal run as handled (no load-time storm),
 * and a session-lifetime ref of seen run keys guarantees each failed run toasts
 * exactly once — never again on re-poll or remount.
 *
 * Since the endpoints return only FAILED runs, every returned row is a failure
 * candidate. The seeding logic (seed on first pass, toast only NEW ones on
 * subsequent passes) still applies: pre-existing failures are absorbed on the
 * first fetch and never toasted.
 */
export function useRunFailureToasts(): void {
  const { t } = useTranslation();
  const navigate = useNavigate();

  // 60s poll interval — failures are not time-critical.
  // refetchIntervalInBackground: false so the tab doesn't poll when hidden.
  const POLL_INTERVAL = 60_000;
  // Cache recent-failure listings for 5 min. The poll interval drives
  // freshness; staleTime just avoids redundant refetches on navigation.
  const STALE_TIME = 5 * 60 * 1000;

  const { data: validationResp } = useListRecentValidationFailures({
    query: {
      refetchInterval: POLL_INTERVAL,
      refetchIntervalInBackground: false,
      staleTime: STALE_TIME,
    },
  });

  const { data: profileResp } = useListRecentProfileFailures({
    query: {
      refetchInterval: POLL_INTERVAL,
      refetchIntervalInBackground: false,
      staleTime: STALE_TIME,
    },
  });

  // Both endpoints return only FAILED rows — map each directly to a
  // RunFailureCandidate (status is always "FAILED").
  const candidates: RunFailureCandidate[] = useMemo(() => {
    const validation = Array.isArray(validationResp?.data)
      ? validationResp.data.map(
          (r): RunFailureCandidate => ({
            key: `validation:${r.run_id}`,
            run_id: r.run_id,
            kind: "validation",
            status: "FAILED",
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
            status: "FAILED",
            source_table_fqn: r.source_table_fqn,
          }),
        )
      : [];
    return [...validation, ...profiling];
  }, [validationResp, profileResp]);

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
