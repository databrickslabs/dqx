/** Notice shown above the schedule settings when the table or collection has
 *  no approved version yet. Scheduled runs always use the latest approved
 *  rules, so the scheduler skips it until it is approved; saving is still
 *  allowed so the schedule starts on its own after approval.
 */
import { useTranslation } from "react-i18next";
import { Info } from "lucide-react";

export function ScheduleApprovalNotice({ entity }: { entity: "table" | "collection" }) {
  const { t } = useTranslation();
  return (
    <div
      role="status"
      className="flex items-start gap-2 rounded-lg border border-sky-500/50 bg-sky-500/10 p-4 text-sky-900 dark:text-sky-100"
    >
      <Info className="mt-0.5 h-4 w-4 shrink-0 text-sky-600 dark:text-sky-400" />
      <div className="space-y-1">
        <h4 className="text-sm font-medium">{t("schedule.approvalNotice.title")}</h4>
        <p className="text-xs text-sky-800 dark:text-sky-200/90">
          {entity === "table"
            ? t("schedule.approvalNotice.descriptionTable")
            : t("schedule.approvalNotice.descriptionCollection")}
        </p>
      </div>
    </div>
  );
}
