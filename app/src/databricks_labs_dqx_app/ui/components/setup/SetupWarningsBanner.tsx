import { useState } from "react";
import { CircleAlert, X } from "lucide-react";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import type { SetupViewModel } from "@/lib/setup-state";

/**
 * Non-blocking setup warnings for administrators once Studio is ready.
 *
 * A READY report can still carry warning steps (for example, app sharing that
 * could not be verified). Studio content renders normally; administrators see
 * the step summary and remediation instructions until they dismiss the banner.
 */
export function SetupWarningsBanner({ view }: { view: SetupViewModel }) {
  const { t } = useTranslation();
  const [dismissed, setDismissed] = useState(false);
  const warnings = view.report.steps.filter((step) => step.state === "warning");

  if (!view.canManage || warnings.length === 0 || dismissed) return null;

  return (
    <div
      role="status"
      className="border-b border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm sm:px-6"
    >
      <div className="mx-auto flex max-w-5xl items-start gap-3">
        <CircleAlert
          className="mt-0.5 size-4 shrink-0 text-amber-500"
          aria-hidden="true"
        />
        <div className="min-w-0 flex-1 space-y-2">
          <p className="font-medium">{t("setup.warningsBanner.title")}</p>
          <p className="text-muted-foreground">
            {t("setup.warningsBanner.description")}
          </p>
          {warnings.map((step) => (
            <div key={step.id} className="space-y-1">
              <p className="font-medium">{t(`setup.steps.${step.id}`)}</p>
              {step.summary && (
                <p className="whitespace-pre-wrap break-words">
                  {step.summary}
                </p>
              )}
              {step.instructions?.map((instruction, index) => (
                <pre
                  key={`${step.id}-${index}`}
                  className="overflow-x-auto whitespace-pre-wrap break-words rounded-md border bg-background/60 p-2 font-mono text-xs"
                >
                  {instruction}
                </pre>
              ))}
            </div>
          ))}
        </div>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          onClick={() => setDismissed(true)}
        >
          <X aria-hidden="true" />
          {t("setup.warningsBanner.dismiss")}
        </Button>
      </div>
    </div>
  );
}
