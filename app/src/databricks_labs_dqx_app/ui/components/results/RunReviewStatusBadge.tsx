import { useTranslation } from "react-i18next";
import { ShieldCheck } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { useRunReviewStatus, useRunReviewStatuses } from "@/lib/api-custom";
// Re-use the colour token table (and the badge look) from the Configuration
// page and the Runs History RunReviewStatusPanel so this badge is visually
// identical to the one shown there.
import { reviewStatusBadgeClasses } from "@/routes/_sidebar/config";

/**
 * Read-only per-run review-status badge for the results surfaces (#18). It
 * mirrors the badge inside RunReviewStatusPanel (Runs History) — same hooks,
 * same colour tokens, same "(auto)" hint for the unreviewed default — but
 * WITHOUT the editing controls: the results views only display the selected
 * run's status (runs-history is where it is edited, per #15).
 *
 * Renders nothing until a run id is known and the status resolves. The query
 * is non-suspense and non-blocking: if the review-status endpoint errors or is
 * empty the badge simply doesn't appear, never an error boundary.
 */
export function RunReviewStatusBadge({ runId }: { runId: string | null | undefined }) {
  const { t } = useTranslation();
  const { data: current } = useRunReviewStatus(runId ?? "");
  const { data: catalogue } = useRunReviewStatuses();

  if (!runId || !current) return null;

  const color =
    catalogue?.statuses.find((o) => o.value === current.status)?.color ?? "gray";

  return (
    <span
      className="flex items-center gap-1.5 text-xs text-muted-foreground"
      title={t("runReviewPanel.label")}
    >
      <ShieldCheck className="h-3.5 w-3.5 shrink-0" />
      <Badge
        variant="outline"
        className={cn("text-[10px] font-normal", reviewStatusBadgeClasses(color))}
      >
        {current.status || t("runReviewPanel.none")}
      </Badge>
      {current.is_default && (
        <span className="text-[10px] uppercase tracking-wide">
          {t("runReviewPanel.auto")}
        </span>
      )}
    </span>
  );
}
