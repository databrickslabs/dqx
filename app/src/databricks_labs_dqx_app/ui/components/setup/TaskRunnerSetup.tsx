import { useRef, useState } from "react";
import {
  useQueryClient,
  type QueryClient,
  type UseMutationOptions,
} from "@tanstack/react-query";
import type { AxiosError, AxiosResponse } from "axios";
import { ExternalLink, Loader2 } from "lucide-react";
import { useTranslation } from "react-i18next";

import { SqlCodeBlock } from "@/components/common/SqlCodeBlock";
import { GrantOutcomeNotice } from "@/components/setup/GrantOutcomeNotice";
import type { JobsLink } from "@/components/setup/SetupWizard";
import { TaskRunnerPrincipalPicker } from "@/components/setup/TaskRunnerPrincipalPicker";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  getGetSetupStatusQueryKey,
  useApplyTaskRunnerGrants,
  useCheckTaskRunnerAccess,
  useListTaskRunnerServicePrincipals,
  useSetTaskRunnerRunAs,
  type HTTPValidationError,
  type SetupReport,
  type SetupStatusResponse,
  type TaskRunnerAccessResponse,
  type TaskRunnerPrincipalRequest,
} from "@/lib/api";
import {
  grantRequestFailure,
  grantRunOutcome,
  grantStatementsSql,
  type GrantRunOutcome,
} from "@/lib/setup-grants";
import { setupErrorCode } from "@/lib/setup-state";
import {
  accessErrorMessageKey,
  accessResultMessageKeys,
  isApplicationId,
  isResultForPick,
  runAsErrorMessageKey,
  servicePrincipalLink,
  taskRunnerPrincipalList,
  taskRunnerSteps,
  type PickedPrincipal,
  type TaskRunnerAccessResult,
  type TaskRunnerPrincipalList,
} from "@/lib/setup-task-runner";

const LINK_CLASS =
  "inline-flex items-center gap-1 text-sm text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";

/** A step 2 response with its optional fields filled in. */
export function accessResult(response: TaskRunnerAccessResponse): TaskRunnerAccessResult {
  return {
    sp_permission: response.sp_permission,
    grant_statements: response.grant_statements ?? [],
    automatic_grants: response.automatic_grants ?? [],
    grants_unverified: response.grants_unverified ?? false,
  };
}

/**
 * After the job's run_as is set, show the re-checked report straight away, then
 * refetch the full status.
 */
export function runAsCallbacks(
  queryClient: QueryClient,
): UseMutationOptions<
  AxiosResponse<SetupReport>,
  AxiosError<HTTPValidationError>,
  { data: TaskRunnerPrincipalRequest }
> {
  return {
    onSuccess: (response) => {
      queryClient.setQueryData<AxiosResponse<SetupStatusResponse>>(
        getGetSetupStatusQueryKey(),
        (current) =>
          current && { ...current, data: { ...current.data, report: response.data } },
      );
    },
    onSettled: () =>
      queryClient.invalidateQueries({ queryKey: getGetSetupStatusQueryKey() }),
  };
}

type TaskRunnerSetupProps = {
  workspaceHost?: string;
  jobsLink: JobsLink | null;
  /** Setup is re-checking, so nothing here can run. */
  isBusy: boolean;
};

/**
 * The task-runner step's three guided steps: pick a service principal, confirm its
 * access, then apply it to the task-runner job. Only shown for a job Studio created;
 * a bundle-deployed job's service principal is set by the deployment.
 */
export function TaskRunnerSetup({ workspaceHost, jobsLink, isBusy }: TaskRunnerSetupProps) {
  const queryClient = useQueryClient();
  const principals = useListTaskRunnerServicePrincipals({
    query: { retry: false, staleTime: 5 * 60_000 },
  });
  const [picked, setPicked] = useState<PickedPrincipal | null>(null);
  // The latest pick, read when a request finishes: its result is dropped if the
  // administrator picked another service principal meanwhile.
  const pickedRef = useRef<PickedPrincipal | null>(null);
  pickedRef.current = picked;
  const [access, setAccess] = useState<TaskRunnerAccessResult | null>(null);
  const checkAccess = useCheckTaskRunnerAccess({
    mutation: {
      onSuccess: (response, variables) => {
        if (isResultForPick(pickedRef.current, variables.data.application_id)) {
          setAccess(accessResult(response.data));
        }
      },
    },
  });
  const applyGrants = useApplyTaskRunnerGrants({
    mutation: {
      onSuccess: (response, variables) => {
        if (!isResultForPick(pickedRef.current, variables.data.application_id)) return;
        setAccess((current) =>
          current && {
            ...current,
            grant_statements: response.data.grant_statements ?? [],
            grants_unverified: response.data.grants_unverified ?? false,
          },
        );
      },
    },
  });
  const runAs = useSetTaskRunnerRunAs({ mutation: runAsCallbacks(queryClient) });

  let grantOutcome: GrantRunOutcome | undefined;
  if (applyGrants.isSuccess) {
    grantOutcome = grantRunOutcome(applyGrants.data.data) ?? undefined;
  } else if (applyGrants.isError) {
    grantOutcome = grantRequestFailure(setupErrorCode(applyGrants.error));
  }
  const request = () => ({ data: { application_id: picked?.applicationId ?? "" } });

  return (
    <TaskRunnerSteps
      principalList={taskRunnerPrincipalList({
        isPending: principals.isPending,
        isError: principals.isError,
        data: principals.data?.data,
      })}
      picked={picked}
      onPick={(next) => {
        pickedRef.current = next;
        setPicked(next);
        setAccess(null);
        checkAccess.reset();
        applyGrants.reset();
        runAs.reset();
      }}
      workspaceHost={workspaceHost}
      jobsLink={jobsLink}
      isBusy={isBusy || runAs.isPending}
      access={access}
      accessErrorCode={
        checkAccess.isError ? (setupErrorCode(checkAccess.error) ?? "default") : undefined
      }
      isCheckingAccess={checkAccess.isPending}
      onConfirmAccess={() => {
        applyGrants.reset();
        checkAccess.mutate(request());
      }}
      grantOutcome={grantOutcome}
      isApplyingGrants={applyGrants.isPending}
      onApplyGrants={() => applyGrants.mutate(request())}
      runAsErrorCode={runAs.isError ? (setupErrorCode(runAs.error) ?? "default") : undefined}
      isApplying={runAs.isPending}
      onApply={() => runAs.mutate(request())}
    />
  );
}

export type TaskRunnerStepsProps = {
  principalList: TaskRunnerPrincipalList;
  picked: PickedPrincipal | null;
  onPick: (principal: PickedPrincipal | null) => void;
  workspaceHost?: string;
  jobsLink: JobsLink | null;
  isBusy: boolean;
  /** The last step 2 result for the picked service principal. */
  access: TaskRunnerAccessResult | null;
  accessErrorCode?: string;
  isCheckingAccess: boolean;
  onConfirmAccess: () => void;
  /** What happened when the picked service principal's GRANT statements last ran. */
  grantOutcome?: GrantRunOutcome;
  isApplyingGrants: boolean;
  onApplyGrants: () => void;
  runAsErrorCode?: string;
  isApplying: boolean;
  onApply: () => void;
};

function SubStep({
  number,
  title,
  disabled = false,
  children,
}: {
  number: 1 | 2 | 3;
  title: string;
  disabled?: boolean;
  children: React.ReactNode;
}) {
  return (
    <li
      className={`space-y-2 rounded-md border p-3${disabled ? " opacity-60" : ""}`}
      aria-disabled={disabled || undefined}
    >
      <p className="text-sm font-medium">
        <span className="mr-1.5 text-muted-foreground">{number}.</span>
        {title}
      </p>
      {children}
    </li>
  );
}

function Spinner() {
  return <Loader2 className="animate-spin motion-reduce:animate-none" aria-hidden="true" />;
}

/** The three task-runner steps, rendered from their current state. */
export function TaskRunnerSteps({
  principalList,
  picked,
  onPick,
  workspaceHost,
  jobsLink,
  isBusy,
  access,
  accessErrorCode,
  isCheckingAccess,
  onConfirmAccess,
  grantOutcome,
  isApplyingGrants,
  onApplyGrants,
  runAsErrorCode,
  isApplying,
  onApply,
}: TaskRunnerStepsProps) {
  const { t } = useTranslation();
  const [pasted, setPasted] = useState(picked?.applicationId ?? "");
  const steps = taskRunnerSteps({
    selected: !!picked,
    access,
    busy: isBusy || isCheckingAccess || isApplyingGrants || isApplying,
  });
  const servicePrincipalsHref = servicePrincipalLink(workspaceHost);
  const pickedHref = servicePrincipalLink(workspaceHost, picked?.id);
  const appName = principalList.appName || t("setup.taskRunner.access.appFallback");
  const pending = access?.grant_statements ?? [];
  const pastedInvalid = !!pasted.trim() && !isApplicationId(pasted);

  return (
    <ol className="space-y-3">
      <SubStep number={1} title={t("setup.taskRunner.steps.pick")}>
        <div className="space-y-1">
          <Label htmlFor="task-runner-principal">{t("setup.taskRunner.pick.label")}</Label>
          {principalList.status === "unavailable" ? (
            <>
              <Input
                id="task-runner-principal"
                name="application_id"
                value={pasted}
                placeholder={t("setup.taskRunner.pick.pastePlaceholder")}
                autoComplete="off"
                disabled={isBusy}
                onChange={(event) => {
                  const value = event.target.value;
                  setPasted(value);
                  const id = value.trim().toLowerCase();
                  onPick(
                    isApplicationId(id)
                      ? { applicationId: id, displayName: id }
                      : null,
                  );
                }}
              />
              <p className="text-xs text-muted-foreground">{t("setup.taskRunner.pick.listUnavailable")}</p>
              {pastedInvalid && (
                <p className="text-xs text-destructive">{t("setup.taskRunner.pick.invalidId")}</p>
              )}
            </>
          ) : (
            <TaskRunnerPrincipalPicker
              id="task-runner-principal"
              value={picked}
              principals={principalList.status === "ready" ? principalList.principals : []}
              isLoading={principalList.status === "loading"}
              disabled={isBusy}
              onChange={onPick}
            />
          )}
          {principalList.status === "ready" && principalList.truncated && (
            <p className="text-xs text-muted-foreground">
              {t("setup.taskRunner.pick.listTruncated", { count: principalList.principals.length })}
            </p>
          )}
        </div>
        <details className="text-sm">
          <summary className="cursor-pointer text-primary underline-offset-4 hover:underline">
            {t("setup.taskRunner.pick.createTitle")}
          </summary>
          <ol className="mt-2 list-decimal space-y-1 pl-5 text-muted-foreground">
            <li>{t("setup.taskRunner.pick.createStep1")}</li>
            <li>{t("setup.taskRunner.pick.createStep2")}</li>
          </ol>
          {servicePrincipalsHref && (
            <a
              href={servicePrincipalsHref}
              target="_blank"
              rel="noopener noreferrer"
              className={`${LINK_CLASS} mt-2`}
            >
              {t("setup.taskRunner.pick.openServicePrincipals")}
              <ExternalLink className="size-3.5" aria-hidden="true" />
            </a>
          )}
        </details>
      </SubStep>

      <SubStep number={2} title={t("setup.taskRunner.steps.access")} disabled={!picked}>
        <p className="text-sm text-muted-foreground">
          {t("setup.taskRunner.access.intro", {
            name: picked?.displayName || t("setup.taskRunner.access.nameFallback"),
          })}
        </p>
        <ol className="list-[lower-alpha] space-y-1 pl-5 text-sm text-muted-foreground">
          <li>{t("setup.taskRunner.access.self")}</li>
          <li>{t("setup.taskRunner.access.app", { appName })}</li>
        </ol>
        {picked && pickedHref && (
          <a href={pickedHref} target="_blank" rel="noopener noreferrer" className={LINK_CLASS}>
            {t(
              picked.id
                ? "setup.taskRunner.access.openServicePrincipal"
                : "setup.taskRunner.pick.openServicePrincipals",
            )}
            <ExternalLink className="size-3.5" aria-hidden="true" />
          </a>
        )}
        <div className="flex flex-wrap items-center gap-2">
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={!steps.confirmEnabled}
            onClick={onConfirmAccess}
          >
            {isCheckingAccess && <Spinner />}
            {t("setup.taskRunner.access.button")}
          </Button>
          {!picked && (
            <p className="text-xs text-muted-foreground">{t("setup.taskRunner.pickFirst")}</p>
          )}
        </div>
        {accessErrorCode && (
          <p role="alert" className="text-sm text-destructive">
            {t(accessErrorMessageKey(accessErrorCode))}
          </p>
        )}
        {access && (
          <div className="space-y-2">
            {accessResultMessageKeys(access).map((key) => (
              <p
                key={key}
                className={`text-sm${key.endsWith(".missing") ? " text-amber-600 dark:text-amber-500" : ""}`}
              >
                {t(key, { appName })}
              </p>
            ))}
            {pending.length > 0 && (
              <SqlCodeBlock
                sql={grantStatementsSql(pending)}
                onRun={onApplyGrants}
                isRunning={isApplyingGrants}
                runDisabled={isBusy || isCheckingAccess || isApplying}
                result={grantOutcome ? <GrantOutcomeNotice outcome={grantOutcome} /> : undefined}
              />
            )}
            {pending.length === 0 && grantOutcome && <GrantOutcomeNotice outcome={grantOutcome} showFailedSql />}
          </div>
        )}
      </SubStep>

      <SubStep number={3} title={t("setup.taskRunner.steps.apply")} disabled={!steps.applyEnabled}>
        <p className="text-sm text-muted-foreground">{t("setup.taskRunner.apply.help")}</p>
        {steps.grantsStillMissing && (
          <p className="text-sm text-amber-600 dark:text-amber-500">
            {t("setup.taskRunner.apply.grantsStillMissing")}
          </p>
        )}
        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" size="sm" disabled={!steps.applyEnabled} onClick={onApply}>
            {isApplying && <Spinner />}
            {t("setup.taskRunner.apply.button")}
          </Button>
          {steps.applyHint && (
            <p className="text-xs text-muted-foreground">
              {t(
                steps.applyHint === "pickFirst"
                  ? "setup.taskRunner.pickFirst"
                  : "setup.taskRunner.apply.confirmFirst",
              )}
            </p>
          )}
        </div>
        {runAsErrorCode && (
          <p role="alert" className="text-sm text-destructive">
            {t(runAsErrorMessageKey(runAsErrorCode))}
          </p>
        )}
        {jobsLink && (
          <a href={jobsLink.href} target="_blank" rel="noopener noreferrer" className={LINK_CLASS}>
            {jobsLink.isTaskRunnerJob
              ? t("setup.actions.openTaskRunnerJob")
              : t("setup.actions.openJobs")}
            <ExternalLink className="size-3.5" aria-hidden="true" />
          </a>
        )}
      </SubStep>
    </ol>
  );
}
