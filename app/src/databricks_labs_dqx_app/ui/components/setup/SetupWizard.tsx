import {
  CheckCircle2,
  CircleAlert,
  Clock3,
  ExternalLink,
  Loader2,
} from "lucide-react";
import { useTranslation } from "react-i18next";

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
import type { SetupStep, StepState } from "@/lib/api";
import type { SetupViewAction, SetupViewModel } from "@/lib/setup-state";

type SetupWizardProps = {
  view: SetupViewModel;
  workspaceHost?: string;
  isReconciling: boolean;
  onReconcile: () => void;
  reconciliationFailed: boolean;
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
            <Logo to="" />
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

export function workspaceJobsUrl(workspaceHost?: string): string | null {
  const host = workspaceHost?.trim().replace(/\/+$/, "");
  return host ? `${host}/#job/list` : null;
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
    case "pending":
      return (
        <Clock3 className="size-5 text-muted-foreground" aria-hidden="true" />
      );
  }
}

function StepActions({
  actions,
  isReconciling,
  onReconcile,
}: {
  actions: SetupViewAction[];
  isReconciling: boolean;
  onReconcile: () => void;
}) {
  const { t } = useTranslation();

  return actions.map((action) => (
    <Button
      key={`${action.stepId}-${action.id}`}
      type="button"
      size="sm"
      disabled={isReconciling}
      onClick={onReconcile}
    >
      {isReconciling && (
        <Loader2
          className="animate-spin motion-reduce:animate-none"
          aria-hidden="true"
        />
      )}
      {t(`setup.actions.${action.id}`)}
    </Button>
  ));
}

function StepCard({
  step,
  actions,
  canManage,
  jobsUrl,
  isReconciling,
  onReconcile,
}: {
  step: SetupStep;
  actions: SetupViewAction[];
  canManage: boolean;
  jobsUrl: string | null;
  isReconciling: boolean;
  onReconcile: () => void;
}) {
  const { t } = useTranslation();
  const stepActions = actions.filter((action) => action.stepId === step.id);

  return (
    <li className="relative pl-10 sm:pl-12">
      <div className="absolute left-0 top-6 grid size-8 place-items-center rounded-full border bg-background sm:size-9">
        <StepIcon state={step.state} />
      </div>
      <Card className="gap-4 py-4">
        <CardHeader className="gap-2 px-4 sm:px-5">
          <div className="flex min-w-0 items-start justify-between gap-3">
            <CardTitle className="text-base">
              {t(`setup.steps.${step.id}`)}
            </CardTitle>
            <Badge variant={step.state === "passed" ? "secondary" : "outline"}>
              {t(`setup.states.${step.state}`)}
            </Badge>
          </div>
          <div className="rounded-md border-l-2 border-primary/50 bg-muted/35 px-3 py-2">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              {t("setup.whyNeeded")}
            </p>
            <p className="mt-1 text-sm leading-5 text-foreground/90">
              {t(`setup.purposes.${step.id}`)}
            </p>
          </div>
          {step.summary && (
            <CardDescription className="whitespace-pre-wrap break-words">
              {step.summary}
            </CardDescription>
          )}
        </CardHeader>
        {(step.code ||
          step.instructions?.length ||
          stepActions.length > 0 ||
          (canManage && step.id === "task_runner" && jobsUrl)) && (
          <CardContent className="space-y-3 px-4 sm:px-5">
            {step.code && (
              <p className="font-mono text-xs text-muted-foreground break-all">
                {t("setup.diagnosticCode", { code: step.code })}
              </p>
            )}
            {step.instructions?.length ? (
              <div className="space-y-2">
                <p className="text-sm font-medium">{t("setup.instructions")}</p>
                {step.instructions.map((instruction, index) => (
                  <pre
                    key={`${step.id}-${index}`}
                    className="overflow-x-auto whitespace-pre-wrap break-words rounded-md border bg-muted/40 p-3 font-mono text-xs leading-relaxed"
                  >
                    {instruction}
                  </pre>
                ))}
              </div>
            ) : null}
            {canManage && step.id === "task_runner" && jobsUrl && (
              <a
                href={jobsUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
              >
                {t("setup.actions.openJobs")}
                <ExternalLink className="size-3.5" aria-hidden="true" />
              </a>
            )}
            {stepActions.length > 0 && (
              <div className="flex flex-wrap gap-2">
                <StepActions
                  actions={stepActions}
                  isReconciling={isReconciling}
                  onReconcile={onReconcile}
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
}: SetupWizardProps) {
  const { t } = useTranslation();
  const isWaiting = view.kind === "waiting";
  const jobsUrl = workspaceJobsUrl(workspaceHost);
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
                : t("setup.title")}
          </h1>
          <p className="max-w-2xl text-sm leading-6 text-muted-foreground">
            {isWaiting
              ? t("setup.waitingDescription", { adminGroup: view.adminGroup })
              : t("setup.checkingDescription")}
          </p>
          {reconciliationFailed && (
            <p className="text-sm text-destructive">
              {t("setup.reconcileFailed")}
            </p>
          )}
        </div>
        <ol className="relative space-y-3 before:absolute before:bottom-6 before:left-4 before:top-6 before:w-px before:bg-border sm:before:left-[18px]">
          {steps.map((step) => (
            <StepCard
              key={step.id}
              step={step}
              actions={view.actions}
              canManage={view.canManage}
              jobsUrl={jobsUrl}
              isReconciling={isReconciling}
              onReconcile={onReconcile}
            />
          ))}
        </ol>
      </section>
    </SetupShell>
  );
}
