// Pure copy helpers for the score-box test counts. Row tests are row-level
// checks over the rows each check evaluated; table tests are dataset-level
// checks, one test each. The score box and the drilldown tables both add the
// two together (combineTestCounts).

/** The slice of i18next's *t* these helpers need (kept narrow so tests can
 *  pass a plain function). */
type Translate = (key: string, options: Record<string, string | number>) => string;

export type TestCounts = {
  failedTests?: number | null;
  totalTests?: number | null;
  tableFailedTests?: number | null;
  tableTotalTests?: number | null;
};

/** Checks and distinct rules in the score box's scope. */
export type CheckRuleCounts = { checkCount: number; ruleCount: number };

/**
 * Score-box subtitle: one combined line over row + table tests, e.g.
 * "16 / 21 tests passed across 2 checks and 2 rules". Without *scope* (the
 * check / rule counts are not loaded) it is just "16 / 21 tests passed";
 * "" when nothing was tested.
 */
export function formatTestCounts(
  t: Translate,
  counts: TestCounts,
  scope?: CheckRuleCounts | null,
): string {
  const { failed, total } = combineTestCounts(counts);
  if (total == null) return "";
  const passed = Math.max(0, total - (failed ?? 0)).toLocaleString();
  const totalText = total.toLocaleString();
  if (!scope) return t("resultsUi.testsPassed", { passed, total: totalText });
  return t("resultsUi.testsPassedAcross", {
    passed,
    total: totalText,
    checks: t("resultsUi.checkCount", { count: scope.checkCount }),
    rules: t("resultsUi.ruleCount", { count: scope.ruleCount }),
  });
}

/**
 * Check / rule counts from a scope's by_rule breakdown rows. The backend
 * groups by_rule on rule IDENTITY (registry rule id, else check name — see
 * dq_results_service._by_rule_rows), so the row count is the number of
 * DISTINCT rules (a rule applied to three tables is one rule, unlike a sum of
 * per-table rule_count). Every check row lands in exactly one rule group, so
 * summing check_count gives the scope's checks (the same total as summing
 * by_table check_count over the same rows). null when the rows are not loaded.
 */
export function countChecksAndRules(
  byRule: { check_count?: number | null }[] | null | undefined,
): CheckRuleCounts | null {
  if (!byRule) return null;
  return {
    checkCount: byRule.reduce((a, g) => a + (g.check_count ?? 0), 0),
    ruleCount: byRule.length,
  };
}

/**
 * Combined counts: table-level checks count as tests (one each), so failed /
 * total tests are the row-level and table-level counts added together. Both
 * are null when nothing was tested.
 */
export function combineTestCounts(counts: TestCounts): { failed: number | null; total: number | null } {
  const total = (counts.totalTests ?? 0) + (counts.tableTotalTests ?? 0);
  if (total <= 0) return { failed: null, total: null };
  return { failed: (counts.failedTests ?? 0) + (counts.tableFailedTests ?? 0), total };
}
