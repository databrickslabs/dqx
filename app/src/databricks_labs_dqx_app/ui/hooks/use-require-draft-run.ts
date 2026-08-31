import { useGetRequireDraftRunSettings } from "@/lib/api";

/**
 * Read the app-wide "require a draft run before submit" gate (issue B2-12).
 *
 * When on, a monitored table / table space / per-table rule cannot be submitted
 * for review (nor take the approvals-mode auto-approve shortcut) until a draft
 * run has been recorded for its target table(s). The RR/MT/TS submit surfaces
 * read this to disable Submit with an explanatory hint until a run exists.
 *
 * Defaults to ``false`` (no requirement) while loading or on error, so the
 * Submit button is never wrongly disabled before we positively know the admin
 * turned the gate on.
 */
export function useRequireDraftRunBeforeSubmit(): boolean {
  const { data } = useGetRequireDraftRunSettings({ query: { select: (d) => d.data } });
  return data?.require_draft_run_before_submit ?? false;
}

/**
 * B2-118: is the last recorded draft run STALE relative to the last edit?
 *
 * A run is stale when the object's most-recent-change instant (``updatedAt`` —
 * the binding's or product's ``updated_at``, bumped on every rule/membership
 * edit) is strictly after the run instant (``lastRunAt`` — the denormalized
 * ``last_run_at``). Both are ISO strings; ``Date`` parsing handles timezone
 * offsets, so the comparison is timezone-safe.
 *
 * Returns ``false`` when either timestamp is missing or unparsable — a missing
 * run is handled by the caller's separate "no run at all" branch, and an
 * unparsable pair must never wrongly block submit. Mirrors the backend's
 * ``DraftRunGateService`` "fresh run since the last change" comparison.
 */
export function isRunStale(lastRunAt?: string | null, updatedAt?: string | null): boolean {
  if (!lastRunAt || !updatedAt) return false;
  const runMs = new Date(lastRunAt).getTime();
  const changeMs = new Date(updatedAt).getTime();
  if (Number.isNaN(runMs) || Number.isNaN(changeMs)) return false;
  return runMs < changeMs;
}
