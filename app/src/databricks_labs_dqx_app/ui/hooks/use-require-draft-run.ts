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
