import * as XLSX from "xlsx";
import type { FailedRowsOut } from "@/lib/api";
import type { FailingRecord } from "./FailingRecordsTable";

/** Map the API's failed-rows payload into the table's FailingRecord shape,
 *  normalising nulls to undefined/[]. Shared by the tabs' on-screen mapping and
 *  the download path so both render/export identically. */
export function toFailingRecords(
  rows: FailedRowsOut["rows"] | undefined,
): FailingRecord[] {
  return (rows ?? []).map((r) => ({
    record_key: r.record_key ?? "",
    row_values: r.row_values ?? {},
    failed_columns: r.failed_columns ?? [],
    failures: (r.failures ?? []).map((f) => ({
      rule_name: f.rule_name ?? undefined,
      severity: f.severity ?? undefined,
      quality_dimension: f.quality_dimension ?? undefined,
      message: f.message ?? undefined,
      columns: f.columns ?? [],
    })),
  }));
}

/** Most failed records one download holds. */
export const EXPORT_ROW_LIMIT = 100000;

/** Excel's cell text limit; SheetJS throws above it. */
export const EXCEL_CELL_MAX_CHARS = 32767;

/** Source columns for the export/table = union of all rows' row_values keys
 *  AND their failed_columns, in first-seen order. Callers pass only rows; the
 *  columns are derived here.
 *
 *  failed_columns must be folded in because the backend renders row_values
 *  from `to_json(row_data)`, which OMITS null-valued fields — so a column that
 *  is null on every failing row (common when the failing check is exactly a
 *  not-null / is-present check on that column) never appears among the
 *  row_values keys. Without the fold, filtering the failing records by such a
 *  column made that very column vanish from the table (item 63). It renders
 *  with a "—" placeholder cell (the value genuinely is null). */
export function columnsFromRows(rows: FailingRecord[]): string[] {
  const columns = new Set<string>();
  for (const row of rows) {
    for (const key of Object.keys(row.row_values)) columns.add(key);
    for (const key of row.failed_columns) columns.add(key);
  }
  return [...columns];
}

/** Distinct failed rule names for a row, in first-seen order, comma-joined.
 *  A rule applied to several columns repeats across failures, so dedupe. */
export function distinctRules(row: FailingRecord): string {
  return Array.from(
    new Set(
      row.failures.map((f) => f.rule_name).filter((n): n is string => !!n),
    ),
  ).join(", ");
}

/** CSV-escape a cell: quote it if it has a comma, quote, CR or LF; double up
 *  embedded quotes. */
function csvCell(v: string): string {
  return /[",\r\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
}

/** Build a CSV of the failing records: every source column (derived from the
 *  rows), plus a trailing "Failed Rules" column with the distinct failed rule
 *  names comma-joined. */
export function buildFailedRecordsCsv(rows: FailingRecord[]): string {
  const columns = columnsFromRows(rows);
  const header = [...columns, "Failed Rules"].map(csvCell).join(",");
  const lines = rows.map((row) => {
    const cells = columns.map((c) => csvCell(row.row_values[c] ?? ""));
    cells.push(csvCell(distinctRules(row)));
    return cells.join(",");
  });
  return [header, ...lines].join("\n");
}

/** Build an .xlsx workbook (sheet "Failed records") with the same columns as
 *  the CSV plus the trailing "Failed Rules" column. Cells over
 *  EXCEL_CELL_MAX_CHARS are cut and counted in truncatedCells. */
export function buildFailedRecordsXlsx(rows: FailingRecord[]): {
  bytes: ArrayBuffer;
  truncatedCells: number;
} {
  let truncatedCells = 0;
  const fit = (value: string): string => {
    if (value.length <= EXCEL_CELL_MAX_CHARS) return value;
    truncatedCells += 1;
    // room for the ellipsis; don't split a surrogate pair
    let end = EXCEL_CELL_MAX_CHARS - 1;
    const last = value.charCodeAt(end - 1);
    if (last >= 0xd800 && last <= 0xdbff) end -= 1;
    return `${value.slice(0, end)}…`;
  };
  const columns = columnsFromRows(rows);
  const header = [...columns, "Failed Rules"].map(fit);
  const body = rows.map((row) => [
    ...columns.map((c) => fit(row.row_values[c] ?? "")),
    fit(distinctRules(row)),
  ]);
  const sheet = XLSX.utils.aoa_to_sheet([header, ...body], { dense: true });
  const book = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(book, sheet, "Failed records");
  // uncompressed, large sheets hit V8's "Invalid array length"
  const bytes = XLSX.write(book, {
    type: "array",
    bookType: "xlsx",
    compression: true,
  }) as ArrayBuffer;
  return { bytes, truncatedCells };
}

export type ExportFormat = "csv" | "xlsx";

export const EXPORT_MIME: Record<ExportFormat, string> = {
  csv: "text/csv;charset=utf-8;",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
};

/** Build the CSV or Excel file for the rows. */
export function buildExportFile(
  format: ExportFormat,
  rows: FailingRecord[],
): { data: BlobPart; truncatedCells: number } {
  if (format === "csv") return { data: buildFailedRecordsCsv(rows), truncatedCells: 0 };
  const { bytes, truncatedCells } = buildFailedRecordsXlsx(rows);
  return { data: bytes, truncatedCells };
}

/** Build the export filename base `<table>_<YYYY-MM-DD>_invalid_records` from a
 *  FRIENDLY table name (not catalog.schema.table). Sanitises to filename-safe
 *  chars; falls back to "table" when empty. */
export function failedRecordsFilename(tableName?: string | null): string {
  const safe = (tableName ?? "")
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  const date = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  return `${safe || "table"}_${date}_invalid_records`;
}

/** Trigger a browser download of `data` as `filename` with the given MIME. */
export function downloadBlob(
  data: BlobPart,
  filename: string,
  mime: string,
): void {
  const blob = new Blob([data], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  // attached anchor + late revoke, some browsers drop the download otherwise
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
}
