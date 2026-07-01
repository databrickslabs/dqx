import { useState, useMemo, useCallback, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  CheckCircle2,
  XCircle,
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  Download,
  Info,
  X,
  Filter,
} from "lucide-react";
import type { DryRunResultsOut } from "@/lib/api";
import {
  useListQuarantineRecords,
  useQuarantineCount,
  exportQuarantineRecords,
  type QuarantineRecordOut,
} from "@/lib/api-custom";

// DQX check identifiers come in two shapes: a bare ``check_function`` name
// (``is_not_null``) or a ``name`` field assigned by the rule author
// (``fare_amount_not_in_range``). Both round-trip through the quarantine
// ``errors[]`` / ``warnings[]`` arrays under the ``name`` key, so a single
// substring is enough to identify either flavour.
type CheckId = string;

interface DryRunResultsProps {
  result: DryRunResultsOut;
}

const PAGE_SIZE_OPTIONS = [10, 25, 50] as const;
const EXPORT_MAX_ROWS = 50_000;
const ERROR_SUMMARY_DEFAULT_LIMIT = 5;

function formatError(err: unknown): string {
  if (typeof err === "string") return err;
  if (err && typeof err === "object") {
    const obj = err as Record<string, unknown>;
    if (typeof obj.message === "string") return obj.message;
    if (typeof obj.name === "string" && typeof obj.message === "string")
      return `${obj.name}: ${obj.message}`;
    try {
      return JSON.stringify(err);
    } catch {
      return "[error]";
    }
  }
  return String(err ?? "");
}

// Format a percentage so the failed-check rows stay aligned in a fixed
// width column. Small ratios get one decimal so a check that fired on a
// handful of rows out of millions doesn't collapse to "0%" and look
// equivalent to one that fired on none.
function formatRatio(pct: number): string {
  if (!isFinite(pct) || pct <= 0) return "0";
  if (pct < 0.1) return "<0.1";
  if (pct < 10) return pct.toFixed(1);
  return Math.round(pct).toString();
}

function summarizeErrorText(raw: string): string {
  const msgMatch = raw.match(/message='([^']+)'/);
  if (msgMatch) return msgMatch[1];
  const msgMatch2 = raw.match(/message="([^"]+)"/);
  if (msgMatch2) return msgMatch2[1];
  if (raw.length > 200) return raw.slice(0, 200) + "...";
  return raw;
}

function _triggerDownload(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function _cellString(v: unknown): string {
  return typeof v === "object" && v !== null ? JSON.stringify(v) : String(v ?? "");
}

function downloadAsCSV(rows: Record<string, unknown>[], filename: string): void {
  if (rows.length === 0) return;
  const keys = Array.from(new Set(rows.flatMap(Object.keys)));
  const escape = (v: unknown) => {
    const s = _cellString(v);
    return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [keys.join(","), ...rows.map((r) => keys.map((k) => escape(r[k])).join(","))];
  _triggerDownload(new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" }), filename);
}

function downloadAsExcel(rows: Record<string, unknown>[], filename: string): void {
  if (rows.length === 0) return;
  const keys = Array.from(new Set(rows.flatMap(Object.keys)));
  const escXml = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  let xml = '<?xml version="1.0"?><?mso-application progid="Excel.Sheet"?>';
  xml += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">';
  xml += "<Worksheet ss:Name=\"Sample\"><Table>";
  xml += "<Row>" + keys.map((k) => `<Cell><Data ss:Type="String">${escXml(k)}</Data></Cell>`).join("") + "</Row>";
  for (const r of rows) {
    xml += "<Row>" + keys.map((k) => `<Cell><Data ss:Type="String">${escXml(_cellString(r[k]))}</Data></Cell>`).join("") + "</Row>";
  }
  xml += "</Table></Worksheet></Workbook>";
  _triggerDownload(new Blob([xml], { type: "application/vnd.ms-excel" }), filename);
}

export function DryRunResults({ result }: DryRunResultsProps) {
  const { t } = useTranslation();
  const totalRows = result.total_rows ?? 0;
  const validRows = result.valid_rows ?? 0;
  // ``invalid_rows`` (rows that failed any check) is kept on the model for
  // backwards compatibility, but we surface DQX's authoritative observer
  // counts instead: ``error_rows`` (= ``error_row_count``) and
  // ``warning_rows`` (= ``warning_row_count``). Pre-v5 history rows have
  // ``error_rows = null`` — fall back to ``invalid_rows`` so the card
  // doesn't show ``0`` for runs created before the rename.
  const errorRows = result.error_rows ?? result.invalid_rows ?? 0;
  const warningRows = result.warning_rows;
  const showWarnings = warningRows != null;
  // The bottom data table still drives off the quarantine endpoint, which
  // contains rows that failed any check (errors OR warnings). We use
  // ``hasFailedRows`` so the table renders whenever there's anything to
  // show — not just when there are errors.
  const hasFailedRows = errorRows > 0 || (warningRows ?? 0) > 0;
  const errorSummary = result.error_summary ?? [];
  const sampleInvalid = result.sample_invalid ?? [];

  const passRate = totalRows > 0 ? Math.round((validRows / totalRows) * 100) : 0;

  const [showAllErrors, setShowAllErrors] = useState(false);
  const visibleErrorSummary = showAllErrors
    ? errorSummary
    : errorSummary.slice(0, ERROR_SUMMARY_DEFAULT_LIMIT);
  const hiddenErrorCount = errorSummary.length - ERROR_SUMMARY_DEFAULT_LIMIT;

  const cleanedSampleRows = useMemo(() => {
    return sampleInvalid.map((r) => {
      const { _warnings, _rule_name, _errors, ...rest } = r as Record<string, unknown>;
      return {
        ...rest,
        errors: Array.isArray(_errors) ? _errors.map(formatError).join("; ") : String(_errors ?? ""),
        warnings: Array.isArray(_warnings) ? _warnings.map(formatError).join("; ") : String(_warnings ?? ""),
      };
    });
  }, [sampleInvalid]);

  const downloadSampleCSV = useCallback(() => {
    downloadAsCSV(cleanedSampleRows, `sample_invalid_${result.run_id}.csv`);
  }, [cleanedSampleRows, result.run_id]);

  const downloadSampleExcel = useCallback(() => {
    downloadAsExcel(cleanedSampleRows, `sample_invalid_${result.run_id}.xls`);
  }, [cleanedSampleRows, result.run_id]);

  const [pageSize, setPageSize] = useState<number>(10);
  const [currentPage, setCurrentPage] = useState(0);
  const offset = currentPage * pageSize;

  // Active check-name filter. Set by clicking a row in the failed-checks
  // table; clears via the chip next to "Failed rows". When set, both the
  // listing endpoint and the count endpoint apply the same
  // server-side filter so pagination stays consistent.
  const [activeCheck, setActiveCheck] = useState<CheckId | null>(null);

  // Reset back to page 0 whenever the filter changes — otherwise the
  // user could land on page 4 of a filtered set that only has page 0.
  useEffect(() => {
    setCurrentPage(0);
  }, [activeCheck]);

  const { data: quarantineResp, isLoading: quarantineLoading } =
    useListQuarantineRecords(
      result.run_id,
      { offset, limit: pageSize, check_name: activeCheck ?? undefined },
      { query: { enabled: hasFailedRows } },
    );
  const { data: countResp } = useQuarantineCount(
    result.run_id,
    { check_name: activeCheck ?? undefined },
    { query: { enabled: hasFailedRows } },
  );

  const quarantineRecords: QuarantineRecordOut[] = quarantineResp?.data?.records ?? [];
  const quarantineTotal = countResp?.data?.count ?? quarantineResp?.data?.total_count ?? 0;
  const hasQuarantine = quarantineTotal > 0;

  const displayRecords = hasQuarantine ? quarantineRecords : [];
  const displayTotal = hasQuarantine ? quarantineTotal : sampleInvalid.length;

  const fallbackRows = useMemo(() => {
    if (hasQuarantine || sampleInvalid.length === 0) return [];
    return sampleInvalid.slice(offset, offset + pageSize);
  }, [hasQuarantine, sampleInvalid, offset, pageSize]);

  const rows: Array<{
    rowData: Record<string, unknown>;
    errors: unknown[];
    warnings: unknown[];
  }> = useMemo(() => {
    if (hasQuarantine) {
      return displayRecords.map((r) => ({
        rowData: r.row_data ?? {},
        errors: r.errors ?? [],
        // ``warnings`` is null for rows written before migration v4 and
        // for SQL-check quarantines. Render an empty array so the column
        // shows nothing rather than ``null``.
        warnings: r.warnings ?? [],
      }));
    }
    return fallbackRows.map((r) => {
      const { _errors, _warnings, _rule_name, ...rest } = r as Record<string, unknown>;
      return {
        rowData: rest,
        errors: Array.isArray(_errors) ? (_errors as unknown[]) : [],
        warnings: Array.isArray(_warnings) ? (_warnings as unknown[]) : [],
      };
    });
  }, [hasQuarantine, displayRecords, fallbackRows]);

  // Only render the Warnings column when at least one displayed row has a
  // warning payload. Pre-v4 quarantine rows have ``null`` warnings — hiding
  // the column keeps the table compact for runs that have only errors.
  const hasAnyWarning = useMemo(
    () => rows.some((r) => Array.isArray(r.warnings) && r.warnings.length > 0),
    [rows],
  );

  const dataColumns = useMemo(() => {
    const keys = new Set<string>();
    for (const r of rows) {
      for (const k of Object.keys(r.rowData)) keys.add(k);
    }
    return Array.from(keys);
  }, [rows]);

  const totalPages = Math.max(1, Math.ceil(displayTotal / pageSize));

  return (
    <div className="space-y-4">
      {/* ``Errors`` and ``Warnings`` are independent buckets — a row can be in
          both. Both come from DQX's observer (``error_row_count`` /
          ``warning_row_count``) so they're bounded by the input row count
          and don't suffer from the fan-out artefacts that ``invalid_rows``
          can have on certain check types. */}
      <div className={`grid gap-4 ${showWarnings ? "grid-cols-4" : "grid-cols-3"}`}>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">{totalRows}</div>
          <div className="text-xs text-muted-foreground">{t("dryRun.totalRows")}</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums text-green-600">{validRows}</div>
          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
            <CheckCircle2 className="h-3 w-3" />
            {t("dryRun.valid")}
          </div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums text-red-600">{errorRows}</div>
          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
            <XCircle className="h-3 w-3" />
            Errors
          </div>
        </div>
        {showWarnings && (
          <div className="rounded-lg border p-3 text-center">
            <div className="text-2xl font-bold tabular-nums text-amber-600">{warningRows}</div>
            <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
              <AlertTriangle className="h-3 w-3" />
              Warnings
            </div>
          </div>
        )}
      </div>

      {/* Pass rate bar */}
      <div className="space-y-1">
        <div className="flex items-center justify-between text-xs">
          <span className="text-muted-foreground">{t("dryRun.passRate")}</span>
          <span className="font-medium">{passRate}%</span>
        </div>
        <div className="h-2 rounded-full bg-muted overflow-hidden">
          <div
            className={`h-full rounded-full transition-all ${
              passRate >= 90 ? "bg-green-500" : passRate >= 70 ? "bg-amber-500" : "bg-red-500"
            }`}
            style={{ width: `${passRate}%` }}
          />
        </div>
      </div>

      {/* Check failure summary — each row is one approved check that
          produced at least one error or warning. We split the total
          into ``error_count`` / ``warning_count`` columns so a
          warning-level check is visually distinct from an error-level
          one (otherwise warning-only checks look identical to errors,
          which is what users hit in practice).

          Each row is also clickable: it sets ``activeCheck`` so the
          failed-rows table below filters down to that single check's
          quarantine rows — the click target on the screen the user
          asked about ("click a failed check → open quarantined
          samples"). */}
      {errorSummary.length > 0 && (
        <div className="space-y-2">
          <h4 className="text-sm font-medium flex items-center gap-1.5">
            <AlertTriangle className="h-4 w-4 text-amber-500" />
            {t("dryRun.failedChecksHeader")}
            <span className="text-muted-foreground font-normal">
              ({errorSummary.length} {t("dryRun.distinctSuffix")})
            </span>
            <span className="ml-auto text-[10px] text-muted-foreground/70 font-normal hidden sm:inline">
              {t("dryRun.clickRowHint")}
            </span>
          </h4>
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left p-2 font-medium">{t("dryRun.checkColumn")}</th>
                  <th className="text-right p-2 font-medium w-32">{t("dryRun.errorsHeader")}</th>
                  <th className="text-right p-2 font-medium w-32">{t("dryRun.warningsHeader")}</th>
                </tr>
              </thead>
              <tbody>
                {visibleErrorSummary.map((item, idx) => {
                  const errCount = Number(item.error_count ?? 0);
                  const warnCount = Number(item.warning_count ?? 0);
                  // ``checkName`` is the click target. The summary
                  // shape doesn't expose the original DQX ``name``
                  // explicitly, but the API populates ``error`` with
                  // the same identifier — fall back to the formatted
                  // text only if for some reason it's missing.
                  const checkName = String(item.error ?? "").trim();
                  const isActive = activeCheck !== null && activeCheck === checkName;
                  const errRatio = totalRows > 0 ? (errCount / totalRows) * 100 : 0;
                  const warnRatio = totalRows > 0 ? (warnCount / totalRows) * 100 : 0;
                  return (
                    <tr
                      key={idx}
                      className={[
                        "border-b last:border-b-0 cursor-pointer transition-colors",
                        isActive
                          ? "bg-primary/5 ring-1 ring-inset ring-primary/30"
                          : "hover:bg-muted/40",
                      ].join(" ")}
                      onClick={() =>
                        setActiveCheck((prev) =>
                          prev === checkName ? null : checkName,
                        )
                      }
                      title={
                        isActive
                          ? t("dryRun.clickRowHintClear")
                          : t("dryRun.clickRowHintFull", { name: checkName })
                      }
                    >
                      <td className="p-2 text-muted-foreground">
                        <span className="font-mono">
                          {summarizeErrorText(checkName)}
                        </span>
                      </td>
                      <td className="p-2 text-right tabular-nums">
                        {errCount > 0 ? (
                          <span className="inline-flex items-center gap-1.5 justify-end">
                            <Badge variant="destructive" className="text-xs">
                              {errCount.toLocaleString()}
                            </Badge>
                            <span className="text-[10px] text-muted-foreground tabular-nums w-12 text-right">
                              ({formatRatio(errRatio)}%)
                            </span>
                          </span>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="p-2 text-right tabular-nums">
                        {warnCount > 0 ? (
                          <span className="inline-flex items-center gap-1.5 justify-end">
                            <Badge
                              variant="outline"
                              className="text-xs border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-300"
                            >
                              {warnCount.toLocaleString()}
                            </Badge>
                            <span className="text-[10px] text-muted-foreground tabular-nums w-12 text-right">
                              ({formatRatio(warnRatio)}%)
                            </span>
                          </span>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {hiddenErrorCount > 0 && (
              <button
                className="w-full py-1.5 text-xs text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors"
                onClick={() => setShowAllErrors((v) => !v)}
              >
                {showAllErrors ? t("dryRun.showLess") : t("dryRun.showMore", { count: hiddenErrorCount })}
              </button>
            )}
          </div>
        </div>
      )}

      {/* Failed rows data table — includes both error rows and warning rows
          since DQX's split puts anything that failed a check into the same
          quarantine bucket. When ``activeCheck`` is set the title chip
          makes the filter obvious and clicking the X clears it. */}
      {hasFailedRows && (
        <div className="space-y-2">
          <div className="flex items-center justify-between flex-wrap gap-2">
            <h4 className="text-sm font-medium flex items-center gap-1.5 flex-wrap">
              <AlertTriangle className="h-4 w-4 text-amber-500" />
              {t("dryRun.failedRowsHeader")}
              <span className="text-muted-foreground font-normal">
                ({hasQuarantine ? `${quarantineTotal} ${t("dryRun.quarantinedSuffix")}` : `${sampleInvalid.length} ${t("dryRun.samplesSuffix")}`})
              </span>
              {activeCheck && (
                <Badge
                  variant="secondary"
                  className="ml-1 gap-1 font-normal text-[11px]"
                >
                  <Filter className="h-3 w-3" />
                  <span className="font-mono">{activeCheck}</span>
                  <button
                    type="button"
                    aria-label={t("dryRun.clearCheckFilter")}
                    className="ml-0.5 -mr-0.5 rounded hover:bg-muted-foreground/20 p-0.5"
                    onClick={() => setActiveCheck(null)}
                  >
                    <X className="h-3 w-3" />
                  </button>
                </Badge>
              )}
            </h4>
            <div className="flex items-center gap-1.5 flex-wrap">
              {hasQuarantine ? (
                <>
                  <span className="text-xs text-muted-foreground">
                    {quarantineTotal > EXPORT_MAX_ROWS
                      ? t("dryRun.exportFirstOf", { first: EXPORT_MAX_ROWS.toLocaleString(), count: quarantineTotal.toLocaleString() })
                      : t("dryRun.exportRows", { count: quarantineTotal.toLocaleString() })}
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={() =>
                      exportQuarantineRecords(
                        result.run_id,
                        "csv",
                        activeCheck ?? undefined,
                      )
                    }
                  >
                    <Download className="h-3.5 w-3.5" />
                    {t("dryRun.csv")}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={() =>
                      exportQuarantineRecords(
                        result.run_id,
                        "xlsx",
                        activeCheck ?? undefined,
                      )
                    }
                  >
                    <Download className="h-3.5 w-3.5" />
                    {t("dryRun.excel")}
                  </Button>
                  {quarantineTotal > EXPORT_MAX_ROWS && (
                    <span className="text-[10px] text-amber-500 flex items-center gap-0.5">
                      <Info className="h-3 w-3" />
                      {t("dryRun.maxExport", { count: EXPORT_MAX_ROWS.toLocaleString() })}
                    </span>
                  )}
                </>
              ) : sampleInvalid.length > 0 ? (
                <>
                  <span className="text-xs text-muted-foreground">
                    {t("dryRun.downloadSampleRows", { count: sampleInvalid.length })}
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={downloadSampleCSV}
                  >
                    <Download className="h-3.5 w-3.5" />
                    {t("dryRun.csv")}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={downloadSampleExcel}
                  >
                    <Download className="h-3.5 w-3.5" />
                    {t("dryRun.excel")}
                  </Button>
                </>
              ) : null}
            </div>
          </div>

          <div className="border rounded-lg overflow-auto max-h-[420px]">
            {quarantineLoading ? (
              <div className="p-6 text-center text-sm text-muted-foreground">{t("dryRun.loadingQuarantine")}</div>
            ) : rows.length === 0 ? (
              <div className="p-6 text-center text-sm text-muted-foreground">{t("dryRun.noInvalidRows")}</div>
            ) : (
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-muted/80 backdrop-blur-sm">
                  <tr className="border-b">
                    <th className="text-left p-2 font-medium whitespace-nowrap">#</th>
                    {dataColumns.map((col) => (
                      <th key={col} className="text-left p-2 font-medium whitespace-nowrap">
                        {col}
                      </th>
                    ))}
                    <th className="text-left p-2 font-medium whitespace-nowrap">Errors</th>
                    {hasAnyWarning && (
                      <th className="text-left p-2 font-medium whitespace-nowrap">Warnings</th>
                    )}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, idx) => (
                    <tr key={idx} className="border-b last:border-b-0 hover:bg-muted/30">
                      <td className="p-2 tabular-nums text-muted-foreground">{offset + idx + 1}</td>
                      {dataColumns.map((col) => (
                        <td key={col} className="p-2 max-w-[200px] truncate" title={String(row.rowData[col] ?? "")}>
                          {String(row.rowData[col] ?? "")}
                        </td>
                      ))}
                      <td className="p-2">
                        <div className="flex flex-wrap gap-1">
                          {(row.errors ?? []).map((err, ei) => (
                            <Badge key={ei} variant="destructive" className="text-[10px] whitespace-nowrap">
                              {formatError(err)}
                            </Badge>
                          ))}
                        </div>
                      </td>
                      {hasAnyWarning && (
                        <td className="p-2">
                          <div className="flex flex-wrap gap-1">
                            {(row.warnings ?? []).map((w, wi) => (
                              <Badge
                                key={wi}
                                variant="outline"
                                className="text-[10px] whitespace-nowrap border-amber-500/50 bg-amber-500/10 text-amber-700 dark:text-amber-300"
                              >
                                {formatError(w)}
                              </Badge>
                            ))}
                          </div>
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Pagination controls */}
          {displayTotal > 10 && (
            <div className="flex items-center justify-between text-xs">
              <div className="flex items-center gap-2">
                <span className="text-muted-foreground">{t("common.rowsPerPage")}</span>
                <select
                  className="border rounded px-1.5 py-0.5 text-xs bg-background"
                  value={pageSize}
                  onChange={(e) => {
                    setPageSize(Number(e.target.value));
                    setCurrentPage(0);
                  }}
                >
                  {PAGE_SIZE_OPTIONS.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>

              <div className="flex items-center gap-2">
                <span className="text-muted-foreground tabular-nums">
                  {offset + 1}-{Math.min(offset + pageSize, displayTotal)} of {displayTotal}
                </span>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  disabled={currentPage === 0}
                  onClick={() => setCurrentPage((p) => Math.max(0, p - 1))}
                >
                  <ChevronLeft className="h-3.5 w-3.5" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  disabled={currentPage >= totalPages - 1}
                  onClick={() => setCurrentPage((p) => p + 1)}
                >
                  <ChevronRight className="h-3.5 w-3.5" />
                </Button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
