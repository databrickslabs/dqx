// Pure copy helpers for the row/table test split. Row tests are row-level
// checks over the rows each check evaluated; table tests are dataset-level
// checks, one test each. Score boxes show the two separately; drilldown
// tables add them together (combineTestCounts).

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
 * Score-box subtitle. With both kinds present the counts stay split
 * ("990,000 / 1,000,000 row tests passed · 0 / 1 table tests passed"); with
 * only one kind it is just "7 / 10 tests passed"; "" when neither is present.
 */
export function formatTestCounts(t: Translate, counts: TestCounts): string {
  const rowTotal = counts.totalTests ?? 0;
  const tableTotal = counts.tableTotalTests ?? 0;
  if (rowTotal > 0 && tableTotal > 0) {
    return [
      passedOf(t, "resultsUi.rowTestsPassed", counts.failedTests, rowTotal),
      passedOf(t, "resultsUi.tableTestsPassed", counts.tableFailedTests, tableTotal),
    ].join(" · ");
  }
  if (rowTotal > 0) return passedOf(t, "resultsUi.testsPassed", counts.failedTests, rowTotal);
  if (tableTotal > 0) return passedOf(t, "resultsUi.testsPassed", counts.tableFailedTests, tableTotal);
  return "";
}

/**
 * Drilldown-table counts: table-level checks count as tests (one each), so a
 * row's failed / total tests are the row-level and table-level counts added
 * together. Both are null when nothing was tested.
 */
export function combineTestCounts(counts: TestCounts): { failed: number | null; total: number | null } {
  const total = (counts.totalTests ?? 0) + (counts.tableTotalTests ?? 0);
  if (total <= 0) return { failed: null, total: null };
  return { failed: (counts.failedTests ?? 0) + (counts.tableFailedTests ?? 0), total };
}
