import type { AxiosError } from "axios";
import { CheckCircle2, Loader2, Rocket } from "lucide-react";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { SetupShell } from "@/components/setup/SetupWizard";
import { useDemoContentStatus, useDeployDemoContent } from "@/lib/api";

/** What the demo step shows, from the seed's status and this screen's deploy request. */
export type DemoOffer = "available" | "deploying" | "started" | "running" | "deployed";

export function demoOffer(
  seedState: string | undefined,
  isDeploying: boolean,
  started: boolean,
): DemoOffer {
  if (started) return "started";
  if (isDeploying) return "deploying";
  if (seedState === "running") return "running";
  if (seedState === "succeeded") return "deployed";
  return "available";
}

export type SetupCompleteViewProps = {
  offer: DemoOffer;
  errorMessage?: string;
  onDeploy: () => void;
  onContinue: () => void;
};

/** The last, optional setup step: deploy the built-in demo, or go straight to Studio. */
export function SetupCompleteView({
  offer,
  errorMessage,
  onDeploy,
  onContinue,
}: SetupCompleteViewProps) {
  const { t } = useTranslation();
  const canDeploy = offer === "available" || offer === "deploying";
  const note =
    offer === "started" || offer === "running" || offer === "deployed"
      ? t(`setup.complete.demo.${offer}`)
      : null;

  return (
    <SetupShell>
      <section className="mx-auto max-w-3xl space-y-8">
        <div className="space-y-2">
          <h1 className="flex items-center gap-2 text-2xl font-semibold tracking-tight">
            <CheckCircle2 className="size-6 text-primary" aria-hidden="true" />
            {t("setup.complete.title")}
          </h1>
          <p className="max-w-2xl text-sm leading-6 text-muted-foreground">
            {t("setup.complete.description")}
          </p>
        </div>
        <Card className="gap-4 py-4">
          <CardHeader className="gap-2 px-4 sm:px-5">
            <CardTitle className="flex items-center gap-2 text-base">
              <Rocket className="size-4" aria-hidden="true" />
              {t("setup.complete.demo.title")}
              <span className="text-xs font-normal text-muted-foreground">
                {t("setup.complete.demo.optional")}
              </span>
            </CardTitle>
            <CardDescription>{t("setup.complete.demo.body")}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 px-4 sm:px-5">
            {note && (
              <p role="status" className="text-sm">
                {note}
              </p>
            )}
            {errorMessage && (
              <p role="alert" className="text-sm text-destructive">
                {errorMessage}
              </p>
            )}
            {canDeploy && (
              <Button
                type="button"
                size="sm"
                disabled={offer === "deploying"}
                onClick={onDeploy}
              >
                {offer === "deploying" && (
                  <Loader2
                    className="animate-spin motion-reduce:animate-none"
                    aria-hidden="true"
                  />
                )}
                {t("setup.complete.demo.deploy")}
              </Button>
            )}
          </CardContent>
        </Card>
        <div className="flex justify-end">
          <Button
            type="button"
            variant={canDeploy ? "outline" : "default"}
            disabled={offer === "deploying"}
            onClick={onContinue}
          >
            {t(canDeploy ? "setup.complete.skip" : "setup.complete.continue")}
          </Button>
        </div>
      </section>
    </SetupShell>
  );
}

/** Shown once to the administrator who just finished setup. */
export function SetupComplete({ onContinue }: { onContinue: () => void }) {
  const { t } = useTranslation();
  // Admin-only; a caller without the Studio admin role just gets the default offer.
  const status = useDemoContentStatus({ query: { retry: false } });
  const deploy = useDeployDemoContent();
  const detail = (deploy.error as AxiosError<{ detail?: string }> | null)
    ?.response?.data?.detail;

  return (
    <SetupCompleteView
      offer={demoOffer(
        status.data?.data.state,
        deploy.isPending,
        deploy.isSuccess,
      )}
      errorMessage={
        deploy.isError ? (detail ?? t("setup.complete.demo.failed")) : undefined
      }
      // Wipe is never offered here: setup has only just created Studio's storage.
      onDeploy={() => deploy.mutate({ data: { wipe_first: false } })}
      onContinue={onContinue}
    />
  );
}
