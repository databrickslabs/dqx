import { useState, useMemo, useCallback } from "react";
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
} from "lucide-react";
import type { DryRunResultsOut } from "@/lib/api";
import {
  useListQuarantineRecords,
  useQuarantineCount,
  exportQuarantineRecords,
  type QuarantineRecordOut,
} from "@/lib/api-custom";

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
  const totalRows = result.total_rows ?? 0;
  const validRows = result.valid_rows ?? 0;
  const invalidRows = result.invalid_rows ?? 0;
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

  const { data: quarantineResp, isLoading: quarantineLoading } =
    useListQuarantineRecords(result.run_id, { offset, limit: pageSize }, {
      query: { enabled: invalidRows > 0 },
    });
  const { data: countResp } = useQuarantineCount(result.run_id, {
    query: { enabled: invalidRows > 0 },
  });

  const quarantineRecords: QuarantineRecordOut[] = quarantineResp?.data?.records ?? [];
  const quarantineTotal = countResp?.data?.count ?? quarantineResp?.data?.total_count ?? 0;
  const hasQuarantine = quarantineTotal > 0;

  const displayRecords = hasQuarantine ? quarantineRecords : [];
  const displayTotal = hasQuarantine ? quarantineTotal : sampleInvalid.length;

  const fallbackRows = useMemo(() => {
    if (hasQuarantine || sampleInvalid.length === 0) return [];
    return sampleInvalid.slice(offset, offset + pageSize);
  }, [hasQuarantine, sampleInvalid, offset, pageSize]);

  const rows: Array<{ rowData: Record<string, unknown>; errors: unknown[] }> = useMemo(() => {
    if (hasQuarantine) {
      return displayRecords.map((r) => ({
        rowData: r.row_data ?? {},
        errors: r.errors ?? [],
      }));
    }
    return fallbackRows.map((r) => {
      const { _errors, _warnings, _rule_name, ...rest } = r as Record<string, unknown>;
      return {
        rowData: rest,
        errors: Array.isArray(_errors) ? (_errors as unknown[]) : [],
      };
    });
  }, [hasQuarantine, displayRecords, fallbackRows]);

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
      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">{totalRows}</div>
          <div className="text-xs text-muted-foreground">Total Rows</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums text-green-600">{validRows}</div>
          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
            <CheckCircle2 className="h-3 w-3" />
            Valid
          </div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums text-red-600">{invalidRows}</div>
          <div className="text-xs text-muted-foreground flex items-center justify-center gap-1">
            <XCircle className="h-3 w-3" />
            Invalid
          </div>
        </div>
      </div>

      {/* Pass rate bar */}
      <div className="space-y-1">
        <div className="flex items-center justify-between text-xs">
          <span className="text-muted-foreground">Pass rate</span>
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

      {/* Error summary */}
      {errorSummary.length > 0 && (
        <div className="space-y-2">
          <h4 className="text-sm font-medium flex items-center gap-1.5">
            <AlertTriangle className="h-4 w-4 text-amber-500" />
            Error Summary
            <span className="text-muted-foreground font-normal">
              ({errorSummary.length} distinct)
            </span>
          </h4>
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left p-2 font-medium">Error</th>
                  <th className="text-right p-2 font-medium w-20">Count</th>
                </tr>
              </thead>
              <tbody>
                {visibleErrorSummary.map((item, idx) => (
                  <tr key={idx} className="border-b last:border-b-0">
                    <td className="p-2 text-muted-foreground" title={String(item.error ?? "")}>
                      {summarizeErrorText(String(item.error ?? ""))}
                    </td>
                    <td className="p-2 text-right tabular-nums">
                      <Badge variant="secondary" className="text-xs">
                        {String(item.count ?? 0)}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {hiddenErrorCount > 0 && (
              <button
                className="w-full py-1.5 text-xs text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors"
                onClick={() => setShowAllErrors((v) => !v)}
              >
                {showAllErrors ? "Show less" : `Show ${hiddenErrorCount} more...`}
              </button>
            )}
          </div>
        </div>
      )}

      {/* Invalid rows data table */}
      {invalidRows > 0 && (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-medium flex items-center gap-1.5">
              <XCircle className="h-4 w-4 text-red-500" />
              Invalid Rows
              <span className="text-muted-foreground font-normal">
                ({hasQuarantine ? `${quarantineTotal} quarantined` : `${sampleInvalid.length} samples`})
              </span>
            </h4>
            <div className="flex items-center gap-1.5 flex-wrap">
              {hasQuarantine ? (
                <>
                  <span className="text-xs text-muted-foreground">
                    Export {quarantineTotal > EXPORT_MAX_ROWS ? `first ${EXPORT_MAX_ROWS.toLocaleString()} of ` : ""}
                    {quarantineTotal.toLocaleString()} rows:
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={() => exportQuarantineRecords(result.run_id, "csv")}
                  >
                    <Download className="h-3.5 w-3.5" />
                    CSV
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={() => exportQuarantineRecords(result.run_id, "xlsx")}
                  >
                    <Download className="h-3.5 w-3.5" />
                    Excel
                  </Button>
                  {quarantineTotal > EXPORT_MAX_ROWS && (
                    <span className="text-[10px] text-amber-500 flex items-center gap-0.5">
                      <Info className="h-3 w-3" />
                      Max {EXPORT_MAX_ROWS.toLocaleString()} rows per export
                    </span>
                  )}
                </>
              ) : sampleInvalid.length > 0 ? (
                <>
                  <span className="text-xs text-muted-foreground">
                    Download {sampleInvalid.length} sample rows:
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={downloadSampleCSV}
                  >
                    <Download className="h-3.5 w-3.5" />
                    CSV
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    onClick={downloadSampleExcel}
                  >
                    <Download className="h-3.5 w-3.5" />
                    Excel
                  </Button>
                </>
              ) : null}
            </div>
          </div>

          <div className="border rounded-lg overflow-auto max-h-[420px]">
            {quarantineLoading ? (
              <div className="p-6 text-center text-sm text-muted-foreground">Loading quarantine data...</div>
            ) : rows.length === 0 ? (
              <div className="p-6 text-center text-sm text-muted-foreground">No invalid rows to display</div>
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
                <span className="text-muted-foreground">Rows per page:</span>
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
