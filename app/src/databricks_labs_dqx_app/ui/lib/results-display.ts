/**
 * Pure display logic for the shared DQ results components (ScoreBox and
 * FailingRecordsTable in components/results/) — score formatting, severity
 * band colors, failing-cell resolution, and column derivation. Extracted so
 * the logic is unit-testable under `bun test` without mounting components
 * (this repo has no component-test infrastructure); the .tsx files stay
 * thin JSX shells over these helpers.
 */
import type { FailingRecordOut } from "@/lib/api";

/** Scores at or above this fraction render in the green (healthy) band. */
export const SCORE_BAND_GREEN_MIN = 0.95;
/** Scores at or above this fraction (but below green) render in the amber band. */
export const SCORE_BAND_AMBER_MIN = 0.8;

/** Placeholder shown for null/absent cell values. */
const EMPTY_CELL = "—"; // em dash

/** Formats a 0..1 score fraction as a one-decimal percentage, e.g. "94.2%". */
export function formatScorePercent(score: number): string {
  return `${(score * 100).toFixed(1)}%`;
}

/**
 * Text color classes (light + dark variants) for a score's severity band:
 * green ≥ 0.95, amber ≥ 0.8, red below.
 */
export function scoreBandClass(score: number): string {
  if (score >= SCORE_BAND_GREEN_MIN) return "text-emerald-600 dark:text-emerald-400";
  if (score >= SCORE_BAND_AMBER_MIN) return "text-amber-600 dark:text-amber-400";
  return "text-red-600 dark:text-red-400";
}

/**
 * Union of row_values column names across all records, preserving
 * first-seen order — records may differ in shape (e.g. legacy rows), so
 * deriving from only the first record could drop columns.
 */
export function deriveRecordColumns(records: FailingRecordOut[]): string[] {
  const seen = new Set<string>();
  const columns: string[] = [];
  for (const record of records) {
    for (const column of Object.keys(record.row_values ?? {})) {
      if (!seen.has(column)) {
        seen.add(column);
        columns.push(column);
      }
    }
  }
  return columns;
}

/** True when the record's failed_columns attributes a failure to this column. */
export function isFailedCell(record: FailingRecordOut, column: string): boolean {
  return record.failed_columns?.includes(column) ?? false;
}

/**
 * Background tint for a failed cell (opacity-based red per the repo's
 * ResultGrid/ManualGrid convention, so it reads in both themes), or
 * undefined for passing cells.
 */
export function failedCellClass(record: FailingRecordOut, column: string): string | undefined {
  return isFailedCell(record, column) ? "bg-red-500/10 dark:bg-red-500/15" : undefined;
}

/**
 * Message of the first failure attributed to this column, for the cell
 * tooltip. Undefined for passing cells, unattributed (row-level/legacy)
 * failures, or failures without a message.
 */
export function failureMessageForCell(record: FailingRecordOut, column: string): string | undefined {
  if (!isFailedCell(record, column)) return undefined;
  const failure = record.failures?.find((f) => f.columns?.includes(column));
  return failure?.message ?? undefined;
}

/** Cell text for a record's column — the raw value, or an em dash for null/absent. */
export function displayCellValue(record: FailingRecordOut, column: string): string {
  return record.row_values?.[column] ?? EMPTY_CELL;
}
