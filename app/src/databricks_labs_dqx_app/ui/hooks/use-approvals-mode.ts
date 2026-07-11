import { useGetApprovalsMode } from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";

export type ApprovalsMode = "enabled" | "auto_bypass" | "disabled";

export interface UseApprovalsModeResult {
  /** The effective approvals mode. Defaults to ``enabled`` while loading. */
  mode: ApprovalsMode;
  /**
   * Whether *this user's* submit will auto-approve in the same call:
   * always in ``disabled`` mode, and in ``auto_bypass`` only when the caller
   * can approve (holds ``approve_rules`` — i.e. admin/approver). Mirrors the
   * backend ``should_auto_approve`` × ``can_edit_and_approve`` decision so the
   * UI shows "Save & publish" vs "Submit for review" consistently.
   */
  willAutoApprove: boolean;
}

/**
 * Read the app-wide approvals mode (issue #94) and derive whether the current
 * user's submit will auto-approve. Used by the RR / MT / TS submit-and-approve
 * surfaces to relabel their buttons and hide the now-redundant Approve action.
 */
export function useApprovalsMode(): UseApprovalsModeResult {
  const { data } = useGetApprovalsMode();
  const { canApproveRules } = usePermissions();

  const raw = data?.data?.mode;
  const mode: ApprovalsMode =
    raw === "auto_bypass" || raw === "disabled" ? raw : "enabled";

  const willAutoApprove = mode === "disabled" || (mode === "auto_bypass" && canApproveRules);

  return { mode, willAutoApprove };
}
