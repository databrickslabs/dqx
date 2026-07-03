import { useTranslation } from "react-i18next";
import { Info } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";

/**
 * Persistent, plain-language explainer of the two-step rule lifecycle:
 * a rule is approved in the Rules Registry, then applying it to a table
 * and publishing is what actually activates it. Shown on a monitored
 * table's Apply Rules area, where a non-expert steward most needs the
 * reminder. (No longer shown on the Rules Registry list itself — that
 * page now mirrors dqlake's bare-table layout with no explainer banner.)
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
