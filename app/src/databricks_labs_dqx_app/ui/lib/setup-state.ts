import axios from "axios";

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
  /** Signed-in user, shown when they cannot run setup themselves. */
  userName: string;
  /** Resolved task-runner job, linked from the task-runner step (administrators only). */
  taskRunnerJobId: number | null;
  configuration: SetupConfigurationView | null;
  showConfigurationForm: boolean;
  /** Saved choices can still change, but the form stays closed until the administrator asks. */
  canEditConfiguration: boolean;
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

/** The configuration step, when the caller manages setup and the backend offers the form. */
function configurableStep(status: SetupStatusResponse): SetupStep | undefined {
  if (!status.can_manage) return undefined;
  return status.report.steps.find(
    (step) =>
      step.id === "configuration" && step.actions?.includes("configure"),
  );
}

/**
 * Whether the configuration form's catalog and group lists should load: only while
 * the form shows, not merely because the backend still offers to change saved choices.
 */
export function configurationListsWanted(
  status: SetupStatusResponse | undefined,
  acknowledgedWarnings: ReadonlySet<string> = new Set(),
  editingConfiguration = false,
): boolean {
  if (!status) return false;
  const view = setupView(status, acknowledgedWarnings, editingConfiguration);
  return view.kind !== "ready" && view.showConfigurationForm;
}

/**
 * Translate the server-published setup status into a UI state without
 * inferring any remediation actions on the client's behalf.
 *
 * A ready report whose warnings an administrator has not acknowledged yet is a
 * "review": the administrator sees the warnings once in setup before entering
 * Studio. Non-administrators always enter a ready Studio.
 *
 * The configuration form shows while the configuration step needs action. Once
 * saved choices pass, the backend keeps advertising "configure" until storage is
 * locked; the form then stays closed unless *editingConfiguration* is set.
 */
export function setupView(
  status: SetupStatusResponse,
  acknowledgedWarnings: ReadonlySet<string> = new Set(),
  editingConfiguration = false,
): SetupViewModel {
  const { report, can_manage: canManage, admin_group: adminGroup } = status;
  const userName = status.user_name ?? "";
  const taskRunnerJobId = status.task_runner_job_id ?? null;
  const configuration = status.configuration ?? null;
  const actions = canManage
    ? report.steps.flatMap((step) =>
        (step.actions ?? [])
          .filter((id) => id !== "configure")
          .map((id) => ({ id, stepId: step.id })),
      )
    : [];
  const configurationStep = configurableStep(status);
  const configurationPassed = configurationStep?.state === "passed";
  const showConfigurationForm =
    !!configurationStep && (!configurationPassed || editingConfiguration);
  const canEditConfiguration =
    !!configurationStep && configurationPassed && !editingConfiguration;

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
      userName,
      taskRunnerJobId,
      configuration,
      showConfigurationForm,
      canEditConfiguration,
    };
  }

  if (report.state === "checking" || report.state === "initializing") {
    return {
      kind: "checking",
      report,
      actions: [],
      canManage,
      adminGroup,
      userName,
      taskRunnerJobId,
      configuration,
      showConfigurationForm,
      canEditConfiguration,
    };
  }

  return {
    kind: canManage ? "wizard" : "waiting",
    report,
    actions,
    canManage,
    adminGroup,
    userName,
    taskRunnerJobId,
    configuration,
    showConfigurationForm,
    canEditConfiguration,
  };
}

/** Extract the backend error code from a failed setup request. */
export function setupErrorCode(error: unknown): string | undefined {
  if (!axios.isAxiosError(error)) return undefined;
  const detail: unknown = error.response?.data?.detail;
  if (typeof detail === "object" && detail !== null && "code" in detail) {
    const { code } = detail;
    return typeof code === "string" ? code : undefined;
  }
  return undefined;
}
