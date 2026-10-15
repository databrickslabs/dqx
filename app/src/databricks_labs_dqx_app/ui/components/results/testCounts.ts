// Pure copy helpers for the row/table test split. Row tests are row-level
// checks over the rows each check evaluated; table tests are dataset-level
// checks, one test each. The two are never summed together.

/** The slice of i18next's *t* these helpers need (kept narrow so tests can
 *  pass a plain function). */
type Translate = (key: string, options: Record<string, string>) => string;

export type TestCounts = {
  failedTests?: number | null;
  totalTests?: number | null;
  tableFailedTests?: number | null;
  tableTotalTests?: number | null;
};

function passedOf(t: Translate, key: string, failed: number | null | undefined, total: number): string {
  return t(key, {
    passed: Math.max(0, total - (failed ?? 0)).toLocaleString(),
    total: total.toLocaleString(),
  });
}

/**
 * Score-box subtitle: "990,000 / 1,000,000 row tests passed · 0 / 1 table
 * tests passed". A part is omitted when its total is 0/null (a table with
 * only dataset checks has no row tests); "" when neither kind is present.
 */
export function formatTestCounts(t: Translate, counts: TestCounts): string {
  const parts: string[] = [];
  const rowTotal = counts.totalTests ?? 0;
  if (rowTotal > 0) {
    parts.push(passedOf(t, "resultsUi.rowTestsPassed", counts.failedTests, rowTotal));
  }
  const tableTotal = counts.tableTotalTests ?? 0;
  if (tableTotal > 0) {
    parts.push(passedOf(t, "resultsUi.tableTestsPassed", counts.tableFailedTests, tableTotal));
  }
  return parts.join(" · ");
}

/**
 * Per-row indicator for breakdown tables ("1 / 2 table tests failed"; the
 * *short* form "1 / 2 table" fits under the row-failure count), shown only
 * when the row has dataset checks; "" otherwise.
 */
export function formatTableTestsFailed(
  t: Translate,
  tableFailedTests: number | null | undefined,
  tableTotalTests: number | null | undefined,
  { short = false }: { short?: boolean } = {},
): string {
  const total = tableTotalTests ?? 0;
  if (total <= 0) return "";
  return t(short ? "resultsUi.tableTestsFailedShort" : "resultsUi.tableTestsFailed", {
    failed: (tableFailedTests ?? 0).toLocaleString(),
    total: total.toLocaleString(),
  });
}
