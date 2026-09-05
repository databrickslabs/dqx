import type { SetupActionId, SetupReport, SetupStatusResponse, SetupStep } from "./api";

export type SetupViewAction = {
  id: SetupActionId;
  stepId: SetupStep["id"];
};

export type SetupViewModel = {
  kind: "checking" | "ready" | "waiting" | "wizard";
  report: SetupReport;
  actions: SetupViewAction[];
  canManage: boolean;
  adminGroup: string;
};

/**
 * Translate the server-published setup status into a UI state without
 * inferring any remediation actions on the client's behalf.
 */
export function setupView(status: SetupStatusResponse): SetupViewModel {
  const { report, can_manage: canManage, admin_group: adminGroup } = status;
  const actions = canManage
    ? report.steps.flatMap((step) =>
        (step.actions ?? []).map((id) => ({ id, stepId: step.id })),
      )
    : [];

  if (report.state === "ready") {
    return { kind: "ready", report, actions: [], canManage, adminGroup };
  }

  if (report.state === "checking" || report.state === "initializing") {
    return { kind: "checking", report, actions: [], canManage, adminGroup };
  }

  return {
    kind: canManage ? "wizard" : "waiting",
    report,
    actions,
    canManage,
    adminGroup,
  };
}
