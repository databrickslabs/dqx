import { useTranslation } from "react-i18next";
import { Info } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";

/**
 * Persistent, plain-language explainer of the two-step rule lifecycle:
 * a rule is approved in the Rules Registry, then applying it to a table
 * and publishing is what actually activates it. Shown on both the
 * Rules Registry list and a monitored table's Apply Rules area, since
 * those are the two moments a non-expert steward needs the reminder.
 */
export function ApprovalStepsBanner() {
  const { t } = useTranslation();
  return (
    <Card className="border-blue-500/40 bg-blue-500/5">
      <CardContent className="py-3 flex items-start gap-2.5">
        <Info className="h-4 w-4 text-blue-600 dark:text-blue-400 shrink-0 mt-0.5" />
        <p className="text-xs text-blue-700 dark:text-blue-400">
          <span className="block">{t("approvalSteps.stepOne")}</span>
          <span className="block">{t("approvalSteps.stepTwo")}</span>
        </p>
      </CardContent>
    </Card>
  );
}
