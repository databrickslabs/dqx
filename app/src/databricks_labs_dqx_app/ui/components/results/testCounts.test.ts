import { describe, expect, it } from "bun:test";
import { combineTestCounts, countChecksAndRules, formatTestCounts } from "./testCounts";

// Stand-in for i18next's t: resolves the English strings (including the
// _one / _other plural forms off *count*) so the assertions read like the
// rendered UI.
const EN: Record<string, string> = {
  "resultsUi.testsPassed": "{{passed}} / {{total}} tests passed",
  "resultsUi.testsPassedAcross": "{{passed}} / {{total}} tests passed across {{checks}} and {{rules}}",
  "resultsUi.checkCount_one": "{{count}} check",
  "resultsUi.checkCount_other": "{{count}} checks",
  "resultsUi.ruleCount_one": "{{count}} rule",
  "resultsUi.ruleCount_other": "{{count}} rules",
};
const t = (key: string, options: Record<string, string | number>) => {
  const count = options.count;
  const resolved =
    typeof count === "number" ? `${key}_${count === 1 ? "one" : "other"}` : key;
  return (EN[resolved] ?? key).replace(/\{\{(\w+)\}\}/g, (_, name: string) =>
    String(options[name] ?? ""),
  );
};

describe("formatTestCounts", () => {
  it("combines row and table tests into one line across checks and rules", () => {
    // "Amount not null" (1 check, 4/20 failed) + "Schema has currency"
    // (1 check, its table test failed).
    expect(
      formatTestCounts(
        t,
        { failedTests: 4, totalTests: 20, tableFailedTests: 1, tableTotalTests: 1 },
        { checkCount: 2, ruleCount: 2 },
      ),
    ).toBe("16 / 21 tests passed across 2 checks and 2 rules");
  });

  it("uses the singular for one check and one rule", () => {
    expect(
      formatTestCounts(t, { failedTests: 3, totalTests: 10 }, { checkCount: 1, ruleCount: 1 }),
    ).toBe("7 / 10 tests passed across 1 check and 1 rule");
    expect(
      formatTestCounts(t, { tableFailedTests: 0, tableTotalTests: 3 }, { checkCount: 3, ruleCount: 1 }),
    ).toBe("3 / 3 tests passed across 3 checks and 1 rule");
  });

  it("formats large totals with locale separators", () => {
    expect(
      formatTestCounts(
        t,
        { failedTests: 10_000, totalTests: 1_000_000, tableFailedTests: 1, tableTotalTests: 1 },
        { checkCount: 4, ruleCount: 2 },
      ),
    ).toBe(
      `${(990_000).toLocaleString()} / ${(1_000_001).toLocaleString()} tests passed across 4 checks and 2 rules`,
    );
  });

  it("falls back to the plain line when check / rule counts are unavailable", () => {
    const counts = { failedTests: 4, totalTests: 20, tableFailedTests: 1, tableTotalTests: 1 };
    expect(formatTestCounts(t, counts)).toBe("16 / 21 tests passed");
    expect(formatTestCounts(t, counts, null)).toBe("16 / 21 tests passed");
  });

  it("treats a null failed count as zero failures", () => {
    expect(formatTestCounts(t, { failedTests: null, totalTests: 5 })).toBe("5 / 5 tests passed");
  });

  it("returns an empty string when nothing was tested", () => {
    expect(formatTestCounts(t, {})).toBe("");
    expect(
      formatTestCounts(
        t,
        { failedTests: null, totalTests: 0, tableFailedTests: null, tableTotalTests: null },
        { checkCount: 0, ruleCount: 0 },
      ),
    ).toBe("");
  });
});

describe("countChecksAndRules", () => {
  it("counts by_rule rows as distinct rules and sums their checks", () => {
    expect(
      countChecksAndRules([{ check_count: 3 }, { check_count: 1 }, { check_count: null }]),
    ).toEqual({ checkCount: 4, ruleCount: 3 });
  });

  it("returns null when the rows are not loaded", () => {
    expect(countChecksAndRules(undefined)).toBeNull();
    expect(countChecksAndRules(null)).toBeNull();
  });

  it("returns zeros for an empty scope", () => {
    expect(countChecksAndRules([])).toEqual({ checkCount: 0, ruleCount: 0 });
  });
});

describe("combineTestCounts (drilldown rows)", () => {
  it("adds table tests to row tests", () => {
    expect(
      combineTestCounts({ failedTests: 2, totalTests: 8, tableFailedTests: 0, tableTotalTests: 1 }),
    ).toEqual({ failed: 2, total: 9 });
  });

  it("counts a table-only row as tests", () => {
    expect(
      combineTestCounts({ failedTests: null, totalTests: null, tableFailedTests: 0, tableTotalTests: 1 }),
    ).toEqual({ failed: 0, total: 1 });
  });

  it("passes row-only counts through", () => {
    expect(combineTestCounts({ failedTests: 3, totalTests: 10 })).toEqual({ failed: 3, total: 10 });
  });

  it("returns nulls when nothing was tested", () => {
    expect(combineTestCounts({ failedTests: 0, totalTests: 0 })).toEqual({ failed: null, total: null });
    expect(combineTestCounts({})).toEqual({ failed: null, total: null });
  });
});
