import { isAxiosError } from "axios";
import type { FailedRowsOut } from "@/lib/api";
import type { FailingRecord } from "./FailingRecordsTable";
import {
  buildExportFile,
  downloadBlob,
  EXCEL_CELL_MAX_CHARS,
  EXPORT_MIME,
  EXPORT_ROW_LIMIT,
  toFailingRecords,
  type ExportFormat,
} from "./failedRecordsExport";

export interface ExportLimits {
  pageRows: number;
  maxRows: number;
  maxChars: number;
  maxXlsxCells: number;
  retryDelaysMs: readonly number[];
}

/** Download limits. maxChars bounds tab memory; maxXlsxCells is roughly what
 *  SheetJS can write in a browser. */
export const EXPORT_LIMITS: ExportLimits = {
  pageRows: 5000,
  maxRows: EXPORT_ROW_LIMIT,
  maxChars: 150_000_000,
  maxXlsxCells: 2_000_000,
  retryDelaysMs: [1000, 3000],
};

export type FetchFailedRowsPage = (request: {
  runId?: string;
  after?: string;
  limit: number;
  signal: AbortSignal;
}) => Promise<FailedRowsOut>;

export type SaveFile = (data: BlobPart, filename: string, mime: string) => void;

export interface ExportProgress {
  loaded: number;
  total: number | null;
}

export type ExportCap = "rows" | "size" | "cells";

export interface CollectFailedRecordsInput {
  fetchPage: FetchFailedRowsPage;
  format: ExportFormat;
  runId?: string;
  signal: AbortSignal;
  onProgress?: (progress: ExportProgress) => void;
  limits?: Partial<ExportLimits>;
}

export interface CollectedFailedRecords {
  rows: FailingRecord[];
  capped: ExportCap | null;
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function isRetryable(error: unknown): boolean {
  const status = isAxiosError(error) ? error.response?.status : undefined;
  return status === undefined || status >= 500 || status === 429;
}

async function fetchWithRetry(
  load: () => Promise<FailedRowsOut>,
  signal: AbortSignal,
  delaysMs: readonly number[],
): Promise<FailedRowsOut> {
  for (let attempt = 0; ; attempt += 1) {
    signal.throwIfAborted();
    try {
      return await load();
    } catch (error) {
      if (signal.aborted || attempt >= delaysMs.length || !isRetryable(error)) throw error;
    }
    await abortableSleep(delaysMs[attempt], signal);
  }
}

/** An empty, suppressed or other-run page here means access or data changed mid-export. */
function assertContinues(page: FailedRowsOut, runId: string): void {
  if (page.suppressed || !page.rows?.length || page.run_id !== runId) {
    throw new Error("A continuation page of failed records was empty, suppressed or from another run.");
  }
}

function rowChars(row: FailingRecord): number {
  let chars = 0;
  for (const value of Object.values(row.row_values)) chars += value?.length ?? 0;
  return chars;
}

/** Sheet cells incl. the header row and the "Failed Rules" column. */
function sheetCells(rowCount: number, columnCount: number): number {
  return (rowCount + 1) * (columnCount + 1);
}

/** Page through one run's failed rows; *capped* is the limit that stopped it early, if any. */
export async function collectFailedRecords({
  fetchPage,
  format,
  runId,
  signal,
  onProgress,
  limits,
}: CollectFailedRecordsInput): Promise<CollectedFailedRecords> {
  const limit: ExportLimits = { ...EXPORT_LIMITS, ...limits };
  const fetchOnce = (load: () => Promise<FailedRowsOut>) =>
    fetchWithRetry(load, signal, limit.retryDelaysMs);

  const first = await fetchOnce(() =>
    fetchPage({ runId, limit: Math.min(limit.pageRows, limit.maxRows), signal }),
  );
  if (first.suppressed || !first.rows?.length) return { rows: [], capped: null };
  const pinnedRunId = first.run_id ?? null;
  const firstTotal = first.total;

  const rows: FailingRecord[] = [];
  const columns = new Set<string>();
  let chars = 0;

  const take = (page: FailedRowsOut): ExportCap | null => {
    for (const row of toFailingRecords(page.rows)) {
      if (rows.length >= limit.maxRows) return "rows";
      if (format === "xlsx") {
        for (const key of Object.keys(row.row_values)) columns.add(key);
        for (const key of row.failed_columns) columns.add(key);
        if (sheetCells(rows.length + 1, columns.size) > limit.maxXlsxCells) return "cells";
      }
      rows.push(row);
      chars += rowChars(row);
    }
    return null;
  };

  const capReached = (): ExportCap | null => {
    if (rows.length >= limit.maxRows) return "rows";
    if (chars >= limit.maxChars) return "size";
    if (format === "xlsx" && sheetCells(rows.length + 1, columns.size) > limit.maxXlsxCells) {
      return "cells";
    }
    return null;
  };

  let page = first;
  for (;;) {
    const dropped = take(page);
    onProgress?.({
      loaded: rows.length,
      total: typeof firstTotal === "number" && firstTotal > rows.length ? firstTotal : null,
    });
    if (dropped) return { rows, capped: dropped };
    const after = page.next_cursor;
    if (!after) return { rows, capped: null };
    const capped = capReached();
    if (capped) return { rows, capped };
    if (!pinnedRunId) {
      throw new Error("A failed-rows page announced more rows without naming its run.");
    }
    const pageLimit = Math.min(limit.pageRows, limit.maxRows - rows.length);
    page = await fetchOnce(async () => {
      const next = await fetchPage({ runId: pinnedRunId, after, limit: pageLimit, signal });
      assertContinues(next, pinnedRunId);
      return next;
    });
  }
}

/** Run a download pins: the picked run, else the one on screen (never
 *  placeholder data). Undefined means latest. */
export function downloadRunId({
  pickedRunId,
  shownRunId,
  isPlaceholderData,
}: {
  pickedRunId?: string;
  shownRunId?: string | null;
  isPlaceholderData: boolean;
}): string | undefined {
  if (pickedRunId) return pickedRunId;
  return isPlaceholderData ? undefined : (shownRunId ?? undefined);
}

export interface RunFailedRecordsDownloadInput extends CollectFailedRecordsInput {
  filenameBase: string;
  save?: SaveFile;
  reportError?: (error: unknown) => void;
}

export type DownloadOutcome =
  | {
      status: "saved";
      rowCount: number;
      capped: ExportCap | null;
      truncatedCells: number;
    }
  | { status: "empty" }
  | { status: "cancelled" }
  | { status: "failed" };

/** Collect the rows, build the file and save it. Never rejects; a failure is an outcome. */
export async function runFailedRecordsDownload({
  filenameBase,
  save = downloadBlob,
  reportError = (error) => console.error("Failed records download failed", error),
  ...input
}: RunFailedRecordsDownloadInput): Promise<DownloadOutcome> {
  try {
    const { rows, capped } = await collectFailedRecords(input);
    if (input.signal.aborted) return { status: "cancelled" };
    if (rows.length === 0) return { status: "empty" };
    const file = buildExportFile(input.format, rows);
    const suffix = capped ? `_first_${rows.length}` : "";
    save(file.data, `${filenameBase}${suffix}.${input.format}`, EXPORT_MIME[input.format]);
    return {
      status: "saved",
      rowCount: rows.length,
      capped,
      truncatedCells: file.truncatedCells,
    };
  } catch (error) {
    if (input.signal.aborted) return { status: "cancelled" };
    reportError(error);
    return { status: "failed" };
  }
}

export interface DownloadNotice {
  level: "error" | "warning" | "info";
  key: string;
  params?: Record<string, string | number>;
}

/** Notices for an outcome, as i18n keys and params. */
export function outcomeNotices(outcome: DownloadOutcome): DownloadNotice[] {
  switch (outcome.status) {
    case "failed":
      return [{ level: "error", key: "resultsUi.downloadFailed" }];
    case "empty":
      return [{ level: "info", key: "resultsUi.downloadNothing" }];
    case "cancelled":
      return [];
    case "saved": {
      const notices: DownloadNotice[] = [];
      const loaded = outcome.rowCount.toLocaleString();
      if (outcome.capped === "cells") {
        notices.push({
          level: "warning",
          key: "resultsUi.downloadCappedExcel",
          params: { loaded, cells: EXPORT_LIMITS.maxXlsxCells.toLocaleString() },
        });
      } else if (outcome.capped) {
        notices.push({
          level: "warning",
          key: "resultsUi.downloadCapped",
          params: { loaded },
        });
      }
      if (outcome.truncatedCells > 0) {
        notices.push({
          level: "warning",
          key: "resultsUi.downloadExcelCellsTruncated",
          params: { count: outcome.truncatedCells, limit: EXCEL_CELL_MAX_CHARS.toLocaleString() },
        });
      }
      return notices;
    }
  }
}
