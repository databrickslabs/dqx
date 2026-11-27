import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { ChevronDown, Download, Loader2, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { getDqResultsFailedRows } from "@/lib/api";
import { failedRecordsFilename, type ExportFormat } from "./failedRecordsExport";
import {
  outcomeNotices,
  runFailedRecordsDownload,
  type ExportProgress,
  type FetchFailedRowsPage,
} from "./failedRecordsDownload";

/**
 * "Download ▾" dropdown for the failed-records export. Lives inline with the
 * subheader (Failed Records / Invalid samples). On either item, it pages
 * through the WHOLE undrilled set (not the 200-capped, filtered
 * on-screen rows), then builds the file named
 * `<friendly table>_<YYYY-MM-DD>_invalid_records`. Shows the count while
 * fetching; click it to cancel.
 */
export function DownloadFailedRecordsMenu({
  tableFqn,
  runId,
  includeDrafts,
  tableName,
}: {
  /** Table to export. */
  tableFqn: string;
  /** Run to export; latest when omitted. */
  runId?: string | null;
  /** Count draft runs too. */
  includeDrafts?: boolean;
  /** Friendly table name (not catalog.schema.table) for the filename. */
  tableName?: string | null;
}) {
  const { t } = useTranslation();
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<ExportProgress | null>(null);
  const controllerRef = useRef<AbortController | null>(null);

  useEffect(() => () => controllerRef.current?.abort(), []);

  const fetchPage: FetchFailedRowsPage = async ({
    runId: pageRunId,
    after,
    limit,
    signal,
  }) => {
    const res = await getDqResultsFailedRows(
      tableFqn,
      { run_id: pageRunId, after, limit, include_drafts: includeDrafts },
      { signal },
    );
    return res.data;
  };

  const run = async (format: ExportFormat) => {
    const controller = new AbortController();
    controllerRef.current = controller;
    setRunning(true);
    const outcome = await runFailedRecordsDownload({
      fetchPage,
      format,
      runId: runId ?? undefined,
      signal: controller.signal,
      onProgress: setProgress,
      filenameBase: failedRecordsFilename(tableName),
    });
    setRunning(false);
    setProgress(null);
    for (const notice of outcomeNotices(outcome)) {
      const message = t(notice.key, notice.params);
      if (notice.level === "error") toast.error(message);
      else if (notice.level === "warning") toast.warning(message, { duration: 15000 });
      else toast.info(message);
    }
  };

  if (running) {
    const cancelLabel = t("resultsUi.downloadCancel");
    return (
      <Button
        variant="outline"
        size="sm"
        className="h-7 select-none"
        title={cancelLabel}
        onClick={() => controllerRef.current?.abort()}
      >
        <span className="sr-only">{cancelLabel}</span>
        <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
        {progress &&
          (progress.total !== null
            ? t("resultsUi.downloadProgress", {
                loaded: progress.loaded.toLocaleString(),
                total: progress.total.toLocaleString(),
              })
            : t("resultsUi.downloadProgressNoTotal", {
                loaded: progress.loaded.toLocaleString(),
              }))}
        <X className="h-3.5 w-3.5 ml-1" />
      </Button>
    );
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-7 select-none">
          <Download className="h-3.5 w-3.5 mr-1" />
          {t("resultsUi.download")}
          <ChevronDown className="h-3.5 w-3.5 ml-1" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onSelect={() => void run("csv")}>
          {t("resultsUi.downloadCsv")}
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => void run("xlsx")}>
          {t("resultsUi.downloadExcel")}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
