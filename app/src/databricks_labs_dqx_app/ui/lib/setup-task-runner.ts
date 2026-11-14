import type {
  TaskRunnerAccessResponse,
  TaskRunnerPrincipal,
  TaskRunnerPrincipalsResponse,
} from "./api";

/** Service principals offered by the task-runner step's picker. */
export type TaskRunnerPrincipalList =
  | { status: "loading"; appName: string }
  /** *truncated*: the listing stopped at its cap, so an Application ID may need pasting. */
  | { status: "ready"; principals: TaskRunnerPrincipal[]; appName: string; truncated?: boolean }
  /** Listing failed or found nothing: the step falls back to pasting an Application ID. */
  | { status: "unavailable"; appName: string };

/** The service principal picked in step 1; *id* is known only when it came from the list. */
export type PickedPrincipal = {
  applicationId: string;
  displayName: string;
  id?: string;
};

/** The step 2 result the other steps depend on. */
export type TaskRunnerAccessResult = Required<TaskRunnerAccessResponse>;

/** Most service principals rendered at once; typing narrows the rest. */
export const MAX_VISIBLE_PRINCIPALS = 100;

const APPLICATION_ID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SERVICE_PRINCIPALS_PATH =
  "/settings/workspace/identity-and-access/service-principals";
const RUN_AS_ERROR_CODES = new Set([
  "task_runner_run_as_forbidden",
  "task_runner_principal_not_found",
  "task_runner_principal_invalid",
  "task_runner_principal_is_app",
  "task_runner_managed_by_deployment",
  "task_runner_job_unavailable",
]);
const ACCESS_ERROR_CODES = new Set([
  "task_runner_principal_invalid",
  "task_runner_principal_is_app",
  "task_runner_managed_by_deployment",
  "task_runner_job_unavailable",
]);

/** Whether *value* looks like a service principal Application ID (a dashed UUID). */
export function isApplicationId(value: string): boolean {
  return APPLICATION_ID.test(value.trim());
}

/**
 * Whether a step 2 result for *applicationId* still belongs to the picked service
 * principal. A request can finish after the administrator picks another one; its
 * result must not unlock the later steps for the new pick.
 */
export function isResultForPick(
  picked: PickedPrincipal | null,
  applicationId: string,
): boolean {
  return (
    !!picked &&
    picked.applicationId.trim().toLowerCase() === applicationId.trim().toLowerCase()
  );
}

/** Map the service principal query onto what the picker should render. */
export function taskRunnerPrincipalList(query: {
  isPending: boolean;
  isError: boolean;
  data?: TaskRunnerPrincipalsResponse;
}): TaskRunnerPrincipalList {
  const appName = query.data?.app_name ?? "";
  if (query.isError) return { status: "unavailable", appName };
  if (query.isPending) return { status: "loading", appName };
  const principals = [...(query.data?.principals ?? [])];
  return query.data?.available && principals.length > 0
    ? { status: "ready", principals, appName, truncated: !!query.data?.truncated }
    : { status: "unavailable", appName };
}

/**
 * Service principals whose name or Application ID contains *search* (ignoring case),
 * plus the typed Application ID itself when the list doesn't show it.
 */
export function principalMatches(
  principals: readonly TaskRunnerPrincipal[],
  search: string,
): { matches: TaskRunnerPrincipal[]; typedId: string | null } {
  const needle = search.trim().toLowerCase();
  const matches = (
    needle
      ? principals.filter(
          (principal) =>
            principal.display_name.toLowerCase().includes(needle) ||
            principal.application_id.toLowerCase().includes(needle),
        )
      : [...principals]
  ).slice(0, MAX_VISIBLE_PRINCIPALS);
  const listed = principals.some(
    (principal) => principal.application_id.toLowerCase() === needle,
  );
  return {
    matches,
    typedId: isApplicationId(needle) && !listed ? needle : null,
  };
}

/** Link to a service principal's page, or to the service principals list when its ID isn't known. */
export function servicePrincipalLink(
  workspaceHost?: string,
  principalId?: string,
): string | null {
  const host = workspaceHost?.trim().replace(/\/+$/, "");
  if (!host) return null;
  const id = principalId?.trim();
  return id
    ? `${host}${SERVICE_PRINCIPALS_PATH}/${encodeURIComponent(id)}`
    : `${host}${SERVICE_PRINCIPALS_PATH}`;
}

/** Translation keys explaining a step 2 result, in display order. */
export function accessResultMessageKeys(
  access: TaskRunnerAccessResult,
): string[] {
  const prefix = "setup.taskRunner.access.result";
  const pending = access.grant_statements.length > 0;
  const automatic = access.automatic_grants.length > 0;
  const permissionOnly =
    access.sp_permission === "confirmed" &&
    !pending &&
    !automatic &&
    !access.grants_unverified;
  const keys = [
    permissionOnly
      ? `${prefix}.confirmed`
      : access.sp_permission === "confirmed"
        ? `${prefix}.permissionConfirmed`
        : `${prefix}.${access.sp_permission}`,
  ];
  if (pending) keys.push(`${prefix}.grantsPending`);
  if (automatic) keys.push(`${prefix}.automaticGrants`);
  if (!pending && access.grants_unverified) keys.push(`${prefix}.grantsUnverified`);
  return keys;
}

/**
 * Which of the task-runner steps can be used. Step 3 needs a step 2 result. A role that
 * looks missing is only a warning, because Studio can't see every way the app may hold it
 * (for example through an admin group); setting Run as in step 3 is the real proof. Data
 * grants still pending don't block it either, because setup re-checks them once the job
 * runs as the service principal.
 */
export function taskRunnerSteps(state: {
  selected: boolean;
  access: TaskRunnerAccessResult | null;
  busy?: boolean;
}): {
  confirmEnabled: boolean;
  applyEnabled: boolean;
  applyHint: "pickFirst" | "confirmFirst" | null;
  grantsStillMissing: boolean;
} {
  const { selected, access, busy = false } = state;
  const checked = !!access;
  return {
    confirmEnabled: selected && !busy,
    applyEnabled: selected && checked && !busy,
    applyHint: !selected ? "pickFirst" : checked ? null : "confirmFirst",
    grantsStillMissing: selected && !!access && access.grant_statements.length > 0,
  };
}

/** Translation key for a failed step 2 request. */
export function accessErrorMessageKey(code: string | undefined): string {
  return `setup.taskRunner.access.errors.${code && ACCESS_ERROR_CODES.has(code) ? code : "default"}`;
}

/** Translation key for a failed step 3 request. */
export function runAsErrorMessageKey(code: string | undefined): string {
  return `setup.taskRunner.apply.errors.${code && RUN_AS_ERROR_CODES.has(code) ? code : "default"}`;
}
