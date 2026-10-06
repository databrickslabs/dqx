import type {
  SetupActionId,
  SetupConfigurationView,
  SetupReport,
  SetupStatusResponse,
  SetupStep,
} from "./api";

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
  configuration: SetupConfigurationView | null;
  showConfigurationForm: boolean;
};

/**
 * Translate the server-published setup status into a UI state without
 * inferring any remediation actions on the client's behalf.
 */
export function setupView(status: SetupStatusResponse): SetupViewModel {
  const { report, can_manage: canManage, admin_group: adminGroup } = status;
  const configuration = status.configuration ?? null;
  const actions = canManage
    ? report.steps.flatMap((step) =>
        (step.actions ?? [])
          .filter((id) => id !== "configure")
          .map((id) => ({ id, stepId: step.id })),
      )
    : [];
  const showConfigurationForm =
    canManage &&
    report.steps.some(
      (step) =>
        step.id === "configuration" && step.actions?.includes("configure"),
    );

  if (report.state === "ready") {
    return {
      kind: "ready",
      report,
      actions: [],
      canManage,
      adminGroup,
      configuration,
      showConfigurationForm,
    };
  }

  if (report.state === "checking" || report.state === "initializing") {
    return {
      kind: "checking",
      report,
      actions: [],
      canManage,
      adminGroup,
      configuration,
      showConfigurationForm,
    };
  }

  return {
    kind: canManage ? "wizard" : "waiting",
    report,
    actions,
    canManage,
    adminGroup,
    configuration,
    showConfigurationForm,
  };
}
