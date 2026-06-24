/**
 * Per-run review status panel. Surfaced inside the expanded row on the
 * Runs History page, immediately above the existing comments thread.
 *
 * UX outline:
 * - A coloured badge shows the effective review status. Unreviewed runs
 *   carry the catalogue default (e.g. "Pending review") with an "(auto)"
 *   hint so the row is visually distinct from one where someone
 *   explicitly picked the same value.
 * - A dropdown lets any authenticated user move the run to another
 *   value from the admin-managed catalogue.
 * - A small "Last changed by X at Y" line records who moved the run
 *   most recently — only shown when the value is explicit (matching the
 *   ``is_default`` flag returned by the API).
 * - "Revert to default" appears only when the status is explicit; it
 *   POSTs DELETE so the run goes back to the catalogue default.
 * - A collapsible history list shows the audit trail. We deliberately
 *   keep this collapsed by default because the dropdown + recent-change
 *   line answers 95% of questions and the panel sits inside an already
 *   expanded run row.
 */
import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  AlertCircle,
  Check,
  ChevronDown,
  History,
  Loader2,
  RotateCcw,
  ShieldCheck,
} from "lucide-react";
import type { AxiosError } from "axios";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { formatDateTime } from "@/lib/format-utils";
import { cn } from "@/lib/utils";
import {
  useRunReviewStatus,
  useRunReviewStatusHistory,
  useRunReviewStatuses,
  useSetRunReviewStatus,
  useClearRunReviewStatus,
  getRunReviewStatusQueryKey,
  getRunReviewStatusHistoryQueryKey,
} from "@/lib/api-custom";
// Re-use the colour token table from the Configuration page so the
// badge here is visually identical to the swatch admins picked there.
import { reviewStatusBadgeClasses } from "@/routes/_sidebar/config";

interface RunReviewStatusPanelProps {
  runId: string;
}

export function RunReviewStatusPanel({ runId }: RunReviewStatusPanelProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [historyOpen, setHistoryOpen] = useState(false);

  const { data: current, isLoading: currentLoading } = useRunReviewStatus(runId);
  const { data: catalogue, isLoading: catalogueLoading } = useRunReviewStatuses();
  const { data: history, isLoading: historyLoading } = useRunReviewStatusHistory(runId, {
    // Don't pay for the audit-trail roundtrip until the user opens it.
    // Audit views are a minority of interactions and the data is rarely
    // useful at-a-glance.
    query: { enabled: historyOpen },
  });

  const setMutation = useSetRunReviewStatus();
  const clearMutation = useClearRunReviewStatus();

  const refresh = () => {
    queryClient.invalidateQueries({ queryKey: getRunReviewStatusQueryKey(runId) });
    queryClient.invalidateQueries({ queryKey: getRunReviewStatusHistoryQueryKey(runId) });
    // Bust the listing cache too so the History page row reflects the
    // change immediately when the user collapses the row.
    queryClient.invalidateQueries({ queryKey: ["listValidationRuns"] });
    queryClient.invalidateQueries({ queryKey: ["/listValidationRuns"] });
  };

  const handleSelect = (value: string) => {
    if (!value || value === current?.status) return;
    setMutation.mutate(
      { runId, data: { status: value } },
      {
        onSuccess: () => {
          refresh();
          toast.success(t("runReviewPanel.markedAs", { value }));
        },
        onError: (err: unknown) => {
          const axErr = err as AxiosError<{ detail?: string }>;
          toast.error(axErr?.response?.data?.detail ?? t("runReviewPanel.failedUpdate"));
        },
      },
    );
  };

  const handleClear = () => {
    clearMutation.mutate(
      { runId },
      {
        onSuccess: () => {
          refresh();
          toast.success(t("runReviewPanel.revertedToDefault"));
        },
        onError: () => toast.error(t("runReviewPanel.failedRevert")),
      },
    );
  };

  if (currentLoading || catalogueLoading || !current || !catalogue) {
    return (
      <div className="flex items-center gap-2 text-xs text-muted-foreground py-1">
        <Loader2 className="h-3 w-3 animate-spin" />
        {t("runReviewPanel.loading")}
      </div>
    );
  }

  const options = catalogue.statuses;
  // The current status might point to a value that's no longer in the
  // catalogue (e.g. admin renamed/removed it after the row was written).
  // We still want to render the badge and keep the dropdown open with a
  // disabled "orphan" hint so the operator can pick a replacement.
  const matchingOption = options.find((o) => o.value === current.status);
  const badgeColor = matchingOption?.color ?? "gray";
  const isOrphan = !matchingOption && !current.is_default;

  const busy = setMutation.isPending || clearMutation.isPending;

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <span className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
          <ShieldCheck className="h-4 w-4" />
          {t("runReviewPanel.label")}
        </span>

        <Popover>
          <PopoverTrigger asChild>
            <Button
              size="sm"
              variant="outline"
              className={cn(
                "h-7 gap-1.5 text-xs",
                busy && "opacity-60 pointer-events-none",
              )}
              disabled={busy}
            >
              <Badge
                variant="outline"
                className={cn("text-[10px] font-normal", reviewStatusBadgeClasses(badgeColor))}
              >
                {current.status || t("runReviewPanel.none")}
              </Badge>
              {current.is_default && (
                <span className="text-[10px] uppercase tracking-wide text-muted-foreground">
                  {t("runReviewPanel.auto")}
                </span>
              )}
              <ChevronDown className="h-3 w-3 opacity-60" />
            </Button>
          </PopoverTrigger>
          <PopoverContent align="start" className="w-64 p-1">
            <div className="space-y-0.5">
              {isOrphan && (
                <div className="px-2 py-1.5 text-[11px] text-muted-foreground border-b mb-1">
                  {t("runReviewPanel.orphanPrefix")}{" "}
                  <code className="font-mono">{current.status}</code>{" "}
                  {t("runReviewPanel.orphanSuffix")}
                </div>
              )}
              {options.map((opt) => (
                <button
                  key={opt.value}
                  type="button"
                  className={cn(
                    "flex w-full items-start gap-2 rounded px-2 py-1.5 text-xs hover:bg-muted",
                    opt.value === current.status && "bg-muted",
                  )}
                  disabled={busy}
                  onClick={() => handleSelect(opt.value)}
                >
                  <Badge
                    variant="outline"
                    className={cn(
                      "text-[10px] font-normal mt-0.5 shrink-0",
                      reviewStatusBadgeClasses(opt.color),
                    )}
                  >
                    {opt.value}
                  </Badge>
                  {opt.description && (
                    <span className="text-muted-foreground text-[11px] leading-tight flex-1">
                      {opt.description}
                    </span>
                  )}
                  {opt.value === current.status && !current.is_default && (
                    <Check className="h-3 w-3 ml-auto self-center opacity-70" />
                  )}
                </button>
              ))}
            </div>
          </PopoverContent>
        </Popover>

        {!current.is_default && (
          <Button
            size="sm"
            variant="ghost"
            onClick={handleClear}
            disabled={busy}
            className="h-7 gap-1.5 text-xs text-muted-foreground"
            title={t("runReviewPanel.revertTitle")}
          >
            <RotateCcw className="h-3 w-3" />
            {t("runReviewPanel.revert")}
          </Button>
        )}

        {/* Inline "who & when" line — only meaningful for an explicit
            value. The default is virtual so updated_by/updated_at are
            null and showing them would be misleading. */}
        {!current.is_default && current.updated_by && (
          <span className="text-[11px] text-muted-foreground">
            {t("runReviewPanel.byPrefix")}{" "}
            <span className="font-medium text-foreground">{current.updated_by}</span>
            {current.updated_at && <> · {formatDateTime(current.updated_at)}</>}
          </span>
        )}

        {current.is_default && (
          <span className="text-[11px] text-muted-foreground italic">
            {t("runReviewPanel.defaultForUnreviewed")}
          </span>
        )}
      </div>

      <button
        type="button"
        onClick={() => setHistoryOpen((o) => !o)}
        aria-expanded={historyOpen}
        className="flex items-center gap-1.5 text-[11px] text-muted-foreground hover:text-foreground transition-colors"
      >
        <History className="h-3 w-3" />
        {historyOpen ? t("runReviewPanel.hideHistory") : t("runReviewPanel.showHistory")}
      </button>

      {historyOpen && (
        <div className="pl-5 border-l-2 border-muted space-y-1.5">
          {historyLoading && (
            <div className="flex items-center gap-2 text-[11px] text-muted-foreground py-1">
              <Loader2 className="h-3 w-3 animate-spin" />
              {t("runReviewPanel.loadingHistory")}
            </div>
          )}
          {!historyLoading && (history?.history.length ?? 0) === 0 && (
            <p className="text-[11px] text-muted-foreground py-1">
              {t("runReviewPanel.noExplicitChanges")}
            </p>
          )}
          {history?.history.map((entry, i) => (
            <div
              key={`${entry.changed_at}-${i}`}
              className="flex flex-wrap items-center gap-1.5 text-[11px]"
            >
              <span className="text-muted-foreground">
                {formatDateTime(entry.changed_at)}
              </span>
              <span className="text-muted-foreground">·</span>
              <span className="font-medium">{entry.changed_by}</span>
              <span className="text-muted-foreground">{t("runReviewPanel.changedStatus")}</span>
              {entry.previous_status ? (
                <>
                  <span className="text-muted-foreground">{t("runReviewPanel.from")}</span>
                  <code className="rounded bg-muted px-1 font-mono">
                    {entry.previous_status}
                  </code>
                </>
              ) : null}
              <span className="text-muted-foreground">{t("runReviewPanel.to")}</span>
              <code className="rounded bg-muted px-1 font-mono">{entry.status}</code>
            </div>
          ))}
        </div>
      )}

      {isOrphan && (
        <p className="flex items-center gap-1 text-[11px] text-amber-700 dark:text-amber-300">
          <AlertCircle className="h-3 w-3" />
          {t("runReviewPanel.orphanWarning")}
        </p>
      )}
    </div>
  );
}
