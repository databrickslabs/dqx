import {
  CheckCircle2,
  CircleAlert,
  CircleCheckBig,
  Clock3,
  ExternalLink,
  Loader2,
} from "lucide-react";
import { useTranslation } from "react-i18next";

import { HelpTooltip } from "@/components/HelpTooltip";
import { SqlCodeBlock } from "@/components/common/SqlCodeBlock";
import Logo from "@/components/layout/Logo";
import { ModeToggle } from "@/components/layout/mode-toggle";
import { ThemeProvider } from "@/components/layout/theme-provider";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  SetupConfigurationForm,
  type SetupConfigurationValues,
} from "@/components/setup/SetupConfigurationForm";
import type { SetupConfigurationView, SetupStep, StepState } from "@/lib/api";
import type { SetupOptionList } from "@/lib/setup-options";
import { GrantOutcomeNotice } from "@/components/setup/GrantOutcomeNotice";
import { TaskRunnerSetup } from "@/components/setup/TaskRunnerSetup";
import { grantStatementsSql, type StepGrantOutcome } from "@/lib/setup-grants";
import type { SetupViewAction, SetupViewModel } from "@/lib/setup-state";

type SetupWizardProps = {
  view: SetupViewModel;
  workspaceHost?: string;
  isReconciling: boolean;
  onReconcile: () => void;
  reconciliationFailed: boolean;
  onOverride?: (stepId: SetupStep["id"]) => void;
  isOverriding?: boolean;
  onAcknowledgeWarnings?: () => void;
  onConfigure?: (values: SetupConfigurationValues) => void;
  isConfiguring?: boolean;
  configurationError?: string;
  catalogList?: SetupOptionList;
  groupList?: SetupOptionList;
  /** Opens (true) or closes (false) the form for changing saved choices. */
  onEditConfiguration?: (editing: boolean) => void;
  /** Runs a step's GRANT statements as the signed-in administrator. */
  onApplyGrants?: (stepId: SetupStep["id"]) => void;
  /** Step whose GRANT statements are running. */
  applyingGrantsStepId?: SetupStep["id"];
  /** Outcome of the last GRANT run, shown under that step's statements. */
  grantOutcome?: StepGrantOutcome;
};

function progressSteps(view: SetupViewModel): SetupStep[] {
  const steps = [...view.report.steps];
  const currentStep = view.report.current_step;
  if (view.kind !== "checking" || !currentStep) return steps;

  const currentIndex = steps.findIndex((step) => step.id === currentStep);
  if (currentIndex >= 0) {
    steps[currentIndex] = { ...steps[currentIndex], state: "running" };
  } else {
    steps.push({ id: currentStep, state: "running" });
  }
  return steps;
}

export function SetupShell({ children }: { children: React.ReactNode }) {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="cdh-ui-theme">
      <div className="min-h-screen bg-background text-foreground">
        <header className="border-b bg-background/95">
          <div className="mx-auto flex h-12 max-w-5xl items-center justify-between px-4 sm:px-6">
            <Logo to="" branded={false} />
            <ModeToggle />
          </div>
        </header>
        <main className="mx-auto w-full max-w-5xl px-4 py-8 sm:px-6 sm:py-12">
          {children}
        </main>
      </div>
    </ThemeProvider>
  );
}

export type JobsLink = { href: string; isTaskRunnerJob: boolean };

/** Link to the task-runner job when it is known, else to the workspace jobs list. */
export function workspaceJobsLink(
  workspaceHost?: string,
  jobId?: number | null,
): JobsLink | null {
  const host = workspaceHost?.trim().replace(/\/+$/, "");
  if (!host) return null;
  return typeof jobId === "number" && jobId > 0
    ? { href: `${host}/jobs/${jobId}`, isTaskRunnerJob: true }
    : { href: `${host}/#job/list`, isTaskRunnerJob: false };
}

function StepIcon({ state }: { state: StepState }) {
  switch (state) {
    case "passed":
      return (
        <CheckCircle2 className="size-5 text-primary" aria-hidden="true" />
      );
    case "running":
      return (
        <Loader2
          className="size-5 animate-spin motion-reduce:animate-none text-primary"
          aria-hidden="true"
        />
      );
    case "action_required":
    case "failed":
      return (
        <CircleAlert className="size-5 text-destructive" aria-hidden="true" />
      );
    case "warning":
      return (
        <CircleAlert className="size-5 text-amber-500" aria-hidden="true" />
      );
    case "overridden":
      return (
        <CircleCheckBig
          className="size-5 text-amber-500"
          aria-hidden="true"
        />
      );
    case "pending":
      return (
        <Clock3 className="size-5 text-muted-foreground" aria-hidden="true" />
      );
  }
}

function ConfigurationSummary({
  configuration,
}: {
  configuration: SetupConfigurationView;
}) {
  const { t } = useTranslation();
  const rows: [string, string | undefined][] = [
    ["catalog", configuration.catalog],
    ["prefix", configuration.prefix],
    ["schemas", configuration.schemas?.join(", ")],
    ["audience_group", configuration.audience_group],
  ];

  return (
    <div className="space-y-2">
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
        {rows
          .filter(([, value]) => value)
          .map(([key, value]) => (
            <div key={key} className="contents">
              <dt className="text-muted-foreground">
                {t(`setup.configuration.${key}`)}
              </dt>
              <dd className="break-all font-mono text-xs">{value}</dd>
            </div>
          ))}
      </dl>
      {configuration.broad_audience && (
        <Badge variant="outline">
          {t("setup.configuration.broadAudience")}
        </Badge>
      )}
    </div>
  );
}

/** Steps with their own override wording; every other step uses the default wording. */
const OVERRIDE_COPY_STEPS = new Set<SetupStep["id"]>(["app_sharing", "ai"]);
const CONFIGURATION_PROBLEM_STATES = new Set<StepState>([
  "action_required",
  "failed",
  "warning",
]);

function overrideCopyKey(stepId: SetupStep["id"]): string {
  return OVERRIDE_COPY_STEPS.has(stepId) ? stepId : "default";
}

function StepActions({
  actions,
  isBusy,
  onReconcile,
  onOverride,
}: {
  actions: SetupViewAction[];
  isBusy: boolean;
  onReconcile: () => void;
  onOverride?: (stepId: SetupStep["id"]) => void;
}) {
  const { t } = useTranslation();

  return actions.map((action) => {
    const isOverride = action.id === "override";
    if (isOverride && !onOverride) return null;
    return (
      <Button
        key={`${action.stepId}-${action.id}`}
        type="button"
        size="sm"
        variant={isOverride ? "outline" : "default"}
        disabled={isBusy}
        onClick={() =>
          isOverride ? onOverride?.(action.stepId) : onReconcile()
        }
      >
        {isBusy && (
          <Loader2
            className="animate-spin motion-reduce:animate-none"
            aria-hidden="true"
          />
        )}
        {isOverride
          ? t(`setup.override.button.${overrideCopyKey(action.stepId)}`)
          : t(`setup.actions.${action.id}`)}
      </Button>
    );
  });
}

function StepCard({
  step,
  actions,
  canManage,
  jobsLink,
  workspaceHost,
  isReconciling,
  onReconcile,
  onOverride,
  configuration,
  showConfigurationForm,
  canEditConfiguration,
  onConfigure,
  isConfiguring,
  configurationError,
  catalogList,
  groupList,
  onEditConfiguration,
  onApplyGrants,
  applyingGrantsStepId,
  grantOutcome,
}: {
  step: SetupStep;
  actions: SetupViewAction[];
  canManage: boolean;
  jobsLink: JobsLink | null;
  workspaceHost?: string;
  isReconciling: boolean;
  onReconcile: () => void;
  onOverride?: (stepId: SetupStep["id"]) => void;
  configuration: SetupConfigurationView | null;
  showConfigurationForm: boolean;
  canEditConfiguration: boolean;
  onConfigure?: (values: SetupConfigurationValues) => void;
  isConfiguring: boolean;
  configurationError?: string;
  catalogList?: SetupOptionList;
  groupList?: SetupOptionList;
  onEditConfiguration?: (editing: boolean) => void;
  onApplyGrants?: (stepId: SetupStep["id"]) => void;
  applyingGrantsStepId?: SetupStep["id"];
  grantOutcome?: StepGrantOutcome;
}) {
  const { t } = useTranslation();
  const isConfigurationStep = step.id === "configuration";
  const showForm = isConfigurationStep && showConfigurationForm;
  const showEdit =
    isConfigurationStep && canEditConfiguration && !!onEditConfiguration;
  // A passed step's form is open only because the administrator chose to change it.
  const isEditingSaved = showForm && step.state === "passed";
  const showSummary =
    isConfigurationStep &&
    !!configuration &&
    (configuration.source === "deployment" || configuration.source === "saved");
  const stepActions = actions.filter((action) => action.stepId === step.id);
  // Non-administrators see the configuration step without its form, so whatever it needs
  // is for an administrator; the diagnostic code below stays visible to pass on.
  const summary =
    !canManage &&
    isConfigurationStep &&
    CONFIGURATION_PROBLEM_STATES.has(step.state)
      ? t("setup.notAdmin.configurationPending")
      : step.summary;
  const canOverride =
    !!onOverride && stepActions.some((action) => action.id === "override");
  const grantStatements = step.grant_statements ?? [];
  const choosesTaskRunner =
    canManage &&
    stepActions.some((action) => action.id === "choose_task_runner");
  // Choosing the task runner is the step's own guided form, which links the job itself.
  const buttonActions = stepActions.filter(
    (action) => action.id !== "choose_task_runner",
  );
  const showJobsLink =
    canManage && step.id === "task_runner" && !!jobsLink && !choosesTaskRunner;

  return (
    <li className="relative pl-10 sm:pl-12">
      <div className="absolute left-0 top-6 grid size-8 place-items-center rounded-full border bg-background sm:size-9">
        <StepIcon state={step.state} />
      </div>
      <Card className="gap-4 py-4">
        <CardHeader className="gap-2 px-4 sm:px-5">
          <div className="flex min-w-0 items-start justify-between gap-3">
            <CardTitle className="flex items-center gap-1.5 text-base">
              <span>{t(`setup.steps.${step.id}`)}</span>
              <HelpTooltip text={t(`setup.purposes.${step.id}`)} />
            </CardTitle>
            <Badge variant={step.state === "passed" ? "secondary" : "outline"}>
              {t(`setup.states.${step.state}`)}
            </Badge>
          </div>
          {summary && (
            <CardDescription className="whitespace-pre-wrap break-words">
              {summary}
            </CardDescription>
          )}
        </CardHeader>
        {(step.code ||
          step.instructions?.length ||
          grantStatements.length > 0 ||
          buttonActions.length > 0 ||
          choosesTaskRunner ||
          canOverride ||
          showForm ||
          showEdit ||
          showSummary ||
          showJobsLink) && (
          <CardContent className="space-y-3 px-4 sm:px-5">
            {step.code && (
              <p className="font-mono text-xs text-muted-foreground break-all">
                {t("setup.diagnosticCode", { code: step.code })}
              </p>
            )}
            {step.instructions?.length || grantStatements.length > 0 ? (
              <div className="space-y-2">
                <p className="text-sm font-medium">{t("setup.instructions")}</p>
                {step.instructions?.map((instruction, index) => (
                  <p
                    key={`${step.id}-${index}`}
                    className="whitespace-pre-wrap break-words text-sm leading-relaxed"
                  >
                    {instruction}
                  </p>
                ))}
                {grantStatements.length > 0 && (
                  <p className="text-sm">
                    {t(
                      canManage && onApplyGrants
                        ? "setup.grants.leadRun"
                        : "setup.grants.leadCopy",
                    )}
                  </p>
                )}
                {grantStatements.length > 0 && (
                  <SqlCodeBlock
                    sql={grantStatementsSql(grantStatements)}
                    onRun={
                      canManage && onApplyGrants
                        ? () => onApplyGrants(step.id)
                        : undefined
                    }
                    isRunning={applyingGrantsStepId === step.id}
                    runDisabled={isReconciling}
                    result={
                      grantOutcome?.stepId === step.id ? (
                        <GrantOutcomeNotice outcome={grantOutcome.outcome} />
                      ) : undefined
                    }
                  />
                )}
              </div>
            ) : null}
            {showSummary && configuration && !isEditingSaved && (
              <ConfigurationSummary configuration={configuration} />
            )}
            {showEdit && (
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={() => onEditConfiguration?.(true)}
              >
                {t("setup.configuration.edit")}
              </Button>
            )}
            {showForm && (
              <SetupConfigurationForm
                initialValues={
                  configuration?.source === "saved" ? configuration : undefined
                }
                isSubmitting={isConfiguring}
                errorCode={configurationError}
                onSubmit={(values) => onConfigure?.(values)}
                catalogList={catalogList}
                groupList={groupList}
                onCancel={
                  isEditingSaved && onEditConfiguration
                    ? () => onEditConfiguration(false)
                    : undefined
                }
              />
            )}
            {choosesTaskRunner && (
              <TaskRunnerSetup
                workspaceHost={workspaceHost}
                jobsLink={jobsLink}
                isBusy={isReconciling}
              />
            )}
            {showJobsLink && jobsLink && (
              <a
                href={jobsLink.href}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
              >
                {jobsLink.isTaskRunnerJob
                  ? t("setup.actions.openTaskRunnerJob")
                  : t("setup.actions.openJobs")}
                <ExternalLink className="size-3.5" aria-hidden="true" />
              </a>
            )}
            {canOverride && (
              <p className="text-sm text-muted-foreground">
                {t(`setup.override.help.${overrideCopyKey(step.id)}`)}
              </p>
            )}
            {buttonActions.length > 0 && (
              <div className="flex flex-wrap gap-2">
                <StepActions
                  actions={buttonActions}
                  isBusy={isReconciling}
                  onReconcile={onReconcile}
                  onOverride={onOverride}
                />
              </div>
            )}
          </CardContent>
        )}
      </Card>
    </li>
  );
}

export function SetupLoading() {
  const { t } = useTranslation();

  return (
    <SetupShell>
      <div className="mx-auto flex max-w-lg flex-col items-center gap-4 py-20 text-center">
        <Loader2
          className="size-10 animate-spin motion-reduce:animate-none text-primary"
          aria-hidden="true"
        />
        <div className="space-y-2">
          <h1 className="text-xl font-semibold">{t("setup.checkingTitle")}</h1>
          <p className="text-sm text-muted-foreground">
            {t("setup.checkingDescription")}
          </p>
        </div>
      </div>
    </SetupShell>
  );
}

export function SetupStatusUnavailable() {
  const { t } = useTranslation();

  return (
    <SetupShell>
      <Card className="mx-auto mt-12 max-w-xl">
        <CardHeader>
          <CardTitle>{t("setup.statusUnavailableTitle")}</CardTitle>
          <CardDescription>
            {t("setup.statusUnavailableDescription")}
          </CardDescription>
        </CardHeader>
      </Card>
    </SetupShell>
  );
}

export function SetupWizard({
  view,
  workspaceHost,
  isReconciling,
  onReconcile,
  reconciliationFailed,
  onOverride,
  isOverriding = false,
  onAcknowledgeWarnings,
  onConfigure,
  isConfiguring = false,
  configurationError,
  catalogList,
  groupList,
  onEditConfiguration,
  onApplyGrants,
  applyingGrantsStepId,
  grantOutcome,
}: SetupWizardProps) {
  const { t } = useTranslation();
  const isWaiting = view.kind === "waiting";
  const isReview = view.kind === "review";
  const jobsLink = workspaceJobsLink(workspaceHost, view.taskRunnerJobId);
  // A custom administrator group is named; the default workspace admins group is implied.
  const customAdminGroup =
    view.adminGroup && view.adminGroup.toLowerCase() !== "admins"
      ? view.adminGroup
      : null;
  const steps = progressSteps(view);

  if (view.kind === "checking" && steps.length === 0) return <SetupLoading />;

  return (
    <SetupShell>
      <section className="mx-auto max-w-3xl space-y-8">
        <div className="space-y-2">
          <h1 className="text-2xl font-semibold tracking-tight">
            {isWaiting
              ? t("setup.waitingTitle")
              : view.kind === "checking"
                ? t("setup.checkingTitle")
                : isReview
                  ? t("setup.warningsReview.title")
                  : t("setup.title")}
          </h1>
          {/* Non-administrators get the fuller notice below instead of a description. */}
          {!isWaiting && (
            <p className="max-w-2xl text-sm leading-6 text-muted-foreground">
              {view.kind === "checking"
                ? t("setup.checkingDescription")
                : isReview
                  ? t("setup.warningsReview.description")
                  : t("setup.description")}
            </p>
          )}
          {reconciliationFailed && (
            <p className="text-sm text-destructive">
              {t("setup.reconcileFailed")}
            </p>
          )}
        </div>
        {isWaiting && (
          <div
            role="status"
            className="flex gap-3 rounded-lg border border-amber-500/50 bg-amber-500/10 p-4 text-sm"
          >
            <CircleAlert
              className="mt-0.5 size-4 shrink-0 text-amber-500"
              aria-hidden="true"
            />
            <div className="space-y-1">
              <p className="font-medium">{t("setup.notAdmin.title")}</p>
              <p className="text-muted-foreground">
                {view.userName &&
                  `${t("setup.notAdmin.signedInAs", { userName: view.userName })} `}
                {customAdminGroup
                  ? t("setup.notAdmin.description", {
                      adminGroup: customAdminGroup,
                    })
                  : t("setup.notAdmin.adminsOnly")}
              </p>
            </div>
          </div>
        )}
        <ol className="relative space-y-3 before:absolute before:bottom-6 before:left-4 before:top-6 before:w-px before:bg-border sm:before:left-[18px]">
          {steps.map((step) => (
            <StepCard
              key={step.id}
              step={step}
              actions={view.actions}
              canManage={view.canManage}
              jobsLink={jobsLink}
              workspaceHost={workspaceHost}
              isReconciling={
                isReconciling || isOverriding || !!applyingGrantsStepId
              }
              onReconcile={onReconcile}
              onOverride={onOverride}
              configuration={view.configuration}
              showConfigurationForm={view.showConfigurationForm}
              canEditConfiguration={view.canEditConfiguration}
              onConfigure={onConfigure}
              isConfiguring={isConfiguring}
              configurationError={configurationError}
              catalogList={catalogList}
              groupList={groupList}
              onEditConfiguration={onEditConfiguration}
              onApplyGrants={onApplyGrants}
              applyingGrantsStepId={applyingGrantsStepId}
              grantOutcome={grantOutcome}
            />
          ))}
        </ol>
        {isReview && onAcknowledgeWarnings && (
          <div className="flex justify-end">
            <Button
              type="button"
              disabled={isReconciling || isOverriding || !!applyingGrantsStepId}
              onClick={onAcknowledgeWarnings}
            >
              {t("setup.warningsReview.acknowledge")}
            </Button>
          </div>
        )}
      </section>
    </SetupShell>
  );
}
