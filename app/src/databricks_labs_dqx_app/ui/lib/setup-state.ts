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
  kind: "checking" | "ready" | "review" | "waiting" | "wizard";
  report: SetupReport;
  actions: SetupViewAction[];
  canManage: boolean;
  adminGroup: string;
  configuration: SetupConfigurationView | null;
  showConfigurationForm: boolean;
};

/** Key identifying a warning for acknowledgement: its diagnostic code, else its step. */
export function warningKey(step: SetupStep): string {
  return step.code ?? step.id;
}

/** Warning steps of a ready report the administrator has not acknowledged yet. */
export function unacknowledgedWarnings(
  report: SetupReport,
  acknowledged: ReadonlySet<string>,
): SetupStep[] {
  return report.steps.filter(
    (step) => step.state === "warning" && !acknowledged.has(warningKey(step)),
  );
}

/**
 * Translate the server-published setup status into a UI state without
 * inferring any remediation actions on the client's behalf.
 *
 * A ready report whose warnings an administrator has not acknowledged yet is a
 * "review": the administrator sees the warnings once in setup before entering
 * Studio. Non-administrators always enter a ready Studio.
 */
export function setupView(
  status: SetupStatusResponse,
  acknowledgedWarnings: ReadonlySet<string> = new Set(),
): SetupViewModel {
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
    const review =
      canManage &&
      unacknowledgedWarnings(report, acknowledgedWarnings).length > 0;
    return {
      kind: review ? "review" : "ready",
      report,
      actions: review ? actions : [],
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
