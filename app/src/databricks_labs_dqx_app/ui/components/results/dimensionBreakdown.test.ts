import { describe, expect, it } from "bun:test";
import { compareRows, paginateRows } from "./DimensionBreakdown";
import { sumTestCounts } from "./MultiTableResults";

describe("paginateRows (pager math)", () => {
  const rows = Array.from({ length: 19 }, (_, i) => i);

  it("is a no-op without a pageSize (dqlake behaviour)", () => {
    expect(paginateRows(rows, 3)).toEqual({ pageRows: rows, safePage: 0, totalPages: 1 });
    expect(paginateRows(rows, 3, 0)).toEqual({ pageRows: rows, safePage: 0, totalPages: 1 });
  });

  it("is a no-op when the rows fit on one page", () => {
    expect(paginateRows([1, 2, 3], 0, 8)).toEqual({
      pageRows: [1, 2, 3],
      safePage: 0,
      totalPages: 1,
    });
  });

  it("slices full and partial pages (19 rows @ 8 → 8 / 8 / 3)", () => {
    expect(paginateRows(rows, 0, 8).pageRows).toEqual(rows.slice(0, 8));
    expect(paginateRows(rows, 1, 8).pageRows).toEqual(rows.slice(8, 16));
    const last = paginateRows(rows, 2, 8);
    expect(last.pageRows).toEqual(rows.slice(16));
    expect(last.totalPages).toBe(3);
  });

  it("clamps an out-of-range page to the last page (rows shrank under the pager)", () => {
    const shrunk = paginateRows(rows.slice(0, 9), 5, 8);
    expect(shrunk.safePage).toBe(1);
    expect(shrunk.pageRows).toEqual([8]);
  });

  it("clamps a negative page to the first page", () => {
    expect(paginateRows(rows, -2, 8).safePage).toBe(0);
  });
});

describe("sumTestCounts (ScoreBox subtitle counts)", () => {
  it("sums failed and total tests from the SAME by_table rows", () => {
    expect(
      sumTestCounts([
        { label: "c.s.a", failed_tests: 3, total_tests: 100 },
        { label: "c.s.b", failed_tests: 7, total_tests: 900 },
      ]),
    ).toEqual({ failedTests: 10, totalTests: 1000, tableFailedTests: 0, tableTotalTests: 0 });
  });

  it("treats missing counts as 0 and empty/undefined input as zeros", () => {
    const zeros = { failedTests: 0, totalTests: 0, tableFailedTests: 0, tableTotalTests: 0 };
    expect(sumTestCounts([{ label: "c.s.a" }])).toEqual(zeros);
    expect(sumTestCounts(undefined)).toEqual(zeros);
  });
});

describe("compareRows (Failed / Total sort)", () => {
  // Only table checks: no row-test counts, one failed table test (shown as 1 / 1).
  const tableOnly = {
    label: "t",
    pass_rate: 0,
    failed_tests: null,
    total_tests: null,
    table_failed_tests: 1,
    table_total_tests: 1,
  };
  const rowOnly = { label: "r", pass_rate: 25, failed_tests: 3, total_tests: 4 };
  const untested = { label: "u", pass_rate: null, failed_tests: null, total_tests: null };

  it("sorts on the combined row + table counts the cells show", () => {
    expect(compareRows(tableOnly, rowOnly, "failed_tests")).toBeLessThan(0);
    expect(compareRows(tableOnly, rowOnly, "total_tests")).toBeLessThan(0);
  });

  it("still sorts rows with no tests last", () => {
    expect(compareRows(tableOnly, untested, "failed_tests")).toBeLessThan(0);
    expect(compareRows(untested, tableOnly, "total_tests")).toBeGreaterThan(0);
  });

  it("sorts descending with rows that have no tests still last", () => {
    const sorted = [untested, tableOnly, rowOnly].sort((a, b) => compareRows(a, b, "failed_tests", "desc"));
    expect(sorted.map((r) => r.label)).toEqual(["r", "t", "u"]);
  });
});
