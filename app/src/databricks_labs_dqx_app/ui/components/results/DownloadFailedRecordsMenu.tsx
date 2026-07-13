import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, Download, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { FailingRecord } from "./FailingRecordsTable";
import {
  downloadFailedRecordsCsv,
  downloadFailedRecordsXlsx,
  failedRecordsFilename,
} from "./failedRecordsExport";

/**
 * "Download ▾" dropdown for the failed-records export. Lives inline with the
 * subheader (Failed Records / Invalid samples). On either item, it calls
 * `fetchRows` to pull the WHOLE undrilled set (not the 200-capped, filtered
 * on-screen rows), then builds the file named
 * `<friendly table>_<YYYY-MM-DD>_invalid_records`. Shows a spinner while fetching.
 */
export function DownloadFailedRecordsMenu({
  fetchRows,
  tableName,
}: {
  /** Fetch ALL failed rows for the binding with no facet filters. */
  fetchRows: () => Promise<FailingRecord[]>;
  /** Friendly table name (not catalog.schema.table) for the filename. */
  tableName?: string | null;
}) {
  const { t } = useTranslation();
  const [pending, setPending] = useState(false);

  const run = async (
    download: (rows: FailingRecord[], filenameBase: string) => void,
  ) => {
    setPending(true);
    try {
      download(await fetchRows(), failedRecordsFilename(tableName));
    } finally {
      setPending(false);
    }
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-7 select-none" disabled={pending}>
          {pending ? (
            <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
          ) : (
            <Download className="h-3.5 w-3.5 mr-1" />
          )}
          {t("resultsUi.download")}
          <ChevronDown className="h-3.5 w-3.5 ml-1" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onSelect={() => run(downloadFailedRecordsCsv)}>
          {t("resultsUi.downloadCsv")}
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => run(downloadFailedRecordsXlsx)}>
          {t("resultsUi.downloadExcel")}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
