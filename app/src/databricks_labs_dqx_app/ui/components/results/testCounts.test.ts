import { describe, expect, it } from "bun:test";
import { formatTableTestsFailed, formatTestCounts } from "./testCounts";

// Stand-in for i18next's t: resolves the English strings so the assertions
// read like the rendered UI.
const EN: Record<string, string> = {
  "resultsUi.rowTestsPassed": "{{passed}} / {{total}} row tests passed",
  "resultsUi.tableTestsPassed": "{{passed}} / {{total}} table tests passed",
  "resultsUi.tableTestsFailed": "{{failed}} / {{total}} table tests failed",
  "resultsUi.tableTestsFailedShort": "{{failed}} / {{total}} table",
};
const t = (key: string, options: Record<string, string>) =>
  (EN[key] ?? key).replace(/\{\{(\w+)\}\}/g, (_, name: string) => options[name] ?? "");

describe("formatTestCounts", () => {
  it("renders both parts, row tests first, joined by a middle dot", () => {
    expect(
      formatTestCounts(t, {
        failedTests: 10_000,
        totalTests: 1_000_000,
        tableFailedTests: 1,
        tableTotalTests: 1,
      }),
    ).toBe(
      `${(990_000).toLocaleString()} / ${(1_000_000).toLocaleString()} row tests passed · 0 / 1 table tests passed`,
    );
  });

  it("omits the table part when there are no table tests", () => {
    expect(formatTestCounts(t, { failedTests: 3, totalTests: 10, tableTotalTests: 0 })).toBe(
      "7 / 10 row tests passed",
    );
    expect(
      formatTestCounts(t, {
        failedTests: 3,
        totalTests: 10,
        tableFailedTests: null,
        tableTotalTests: null,
      }),
    ).toBe("7 / 10 row tests passed");
  });

  it("omits the row part for a scope with only dataset checks", () => {
    expect(
      formatTestCounts(t, {
        failedTests: null,
        totalTests: null,
        tableFailedTests: 1,
        tableTotalTests: 3,
      }),
    ).toBe("2 / 3 table tests passed");
    expect(
      formatTestCounts(t, { failedTests: 0, totalTests: 0, tableFailedTests: 0, tableTotalTests: 2 }),
    ).toBe("2 / 2 table tests passed");
  });

  it("treats a null failed count as zero failures", () => {
    expect(formatTestCounts(t, { failedTests: null, totalTests: 5 })).toBe("5 / 5 row tests passed");
  });

  it("returns an empty string when neither kind of test is present", () => {
    expect(formatTestCounts(t, {})).toBe("");
    expect(
      formatTestCounts(t, {
        failedTests: null,
        totalTests: 0,
        tableFailedTests: null,
        tableTotalTests: null,
      }),
    ).toBe("");
  });
});

describe("formatTableTestsFailed (breakdown-row indicator)", () => {
  it("renders failed / total table tests when the row has dataset checks", () => {
    expect(formatTableTestsFailed(t, 1, 2)).toBe("1 / 2 table tests failed");
    expect(formatTableTestsFailed(t, null, 2)).toBe("0 / 2 table tests failed");
  });

  it("has a short form that fits a narrow table cell", () => {
    expect(formatTableTestsFailed(t, 1, 2, { short: true })).toBe("1 / 2 table");
  });

  it("returns an empty string when the row has no table tests", () => {
    expect(formatTableTestsFailed(t, 0, 0)).toBe("");
    expect(formatTableTestsFailed(t, null, null)).toBe("");
    expect(formatTableTestsFailed(t, undefined, undefined)).toBe("");
    expect(formatTableTestsFailed(t, 0, 0, { short: true })).toBe("");
  });
});
