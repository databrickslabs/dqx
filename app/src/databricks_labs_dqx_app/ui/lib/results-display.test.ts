import { describe, expect, test } from "bun:test";
import type { FailingRecordOut, TableScoreOut } from "@/lib/api";
import {
  SCORE_BAND_AMBER_MIN,
  SCORE_BAND_GREEN_MIN,
  deriveRecordColumns,
  displayCellValue,
  failedCellClass,
  failureMessageForCell,
  formatScorePercent,
  isFailedCell,
  rowHasWholeRowFailure,
  scoreBandClass,
  sumMemberTestCounts,
  wholeRowFailureClass,
  wholeRowFailureMessage,
} from "./results-display";

describe("formatScorePercent", () => {
  test("renders a fraction as a one-decimal percentage", () => {
    expect(formatScorePercent(0.942)).toBe("94.2%");
  });

  test("renders a perfect score as 100.0%", () => {
    expect(formatScorePercent(1)).toBe("100.0%");
  });

  test("renders a zero score as 0.0%", () => {
    expect(formatScorePercent(0)).toBe("0.0%");
  });

  test("rounds to one decimal place", () => {
    expect(formatScorePercent(0.99999)).toBe("100.0%");
    expect(formatScorePercent(0.12345)).toBe("12.3%");
  });
});

describe("scoreBandClass", () => {
  test("green band at or above the green threshold", () => {
    expect(scoreBandClass(SCORE_BAND_GREEN_MIN)).toContain("emerald");
    expect(scoreBandClass(1)).toContain("emerald");
  });

  test("amber band between amber and green thresholds", () => {
    expect(scoreBandClass(SCORE_BAND_AMBER_MIN)).toContain("amber");
    expect(scoreBandClass(0.9)).toContain("amber");
    expect(scoreBandClass(SCORE_BAND_GREEN_MIN - 0.0001)).toContain("amber");
  });

  test("red band below the amber threshold", () => {
    expect(scoreBandClass(SCORE_BAND_AMBER_MIN - 0.0001)).toContain("red");
    expect(scoreBandClass(0)).toContain("red");
  });

  test("every band pairs a light class with a dark: variant", () => {
    for (const score of [1, 0.9, 0.1]) {
      expect(scoreBandClass(score)).toMatch(/dark:text-/);
    }
  });
});

const record = (over: Partial<FailingRecordOut> = {}): FailingRecordOut => ({
  record_key: "1",
  row_values: { id: "42", email: "not-an-email" },
  failed_columns: ["email"],
  failures: [{ rule_name: "is_valid_email", message: "bad format", columns: ["email"] }],
  ...over,
});

describe("deriveRecordColumns", () => {
  test("returns the first record's columns in order", () => {
    expect(deriveRecordColumns([record()])).toEqual(["id", "email"]);
  });

  test("unions columns across records, preserving first-seen order", () => {
    const records = [
      record(),
      record({ record_key: "2", row_values: { id: "7", name: "x" } }),
    ];
    expect(deriveRecordColumns(records)).toEqual(["id", "email", "name"]);
  });

  test("returns an empty list for no records", () => {
    expect(deriveRecordColumns([])).toEqual([]);
  });

  test("tolerates records with missing row_values", () => {
    expect(deriveRecordColumns([record({ row_values: undefined })])).toEqual([]);
  });
});

describe("isFailedCell", () => {
  test("true for a column listed in failed_columns", () => {
    expect(isFailedCell(record(), "email")).toBe(true);
  });

  test("false for a passing column", () => {
    expect(isFailedCell(record(), "id")).toBe(false);
  });

  test("false when failed_columns is missing (legacy rows)", () => {
    expect(isFailedCell(record({ failed_columns: undefined }), "email")).toBe(false);
  });
});

describe("failedCellClass", () => {
  test("returns a red tint for failed cells", () => {
    expect(failedCellClass(record(), "email")).toContain("bg-red-500/10");
  });

  test("returns undefined for passing cells", () => {
    expect(failedCellClass(record(), "id")).toBeUndefined();
  });
});

describe("failureMessageForCell", () => {
  test("returns the failure message attributed to the column", () => {
    expect(failureMessageForCell(record(), "email")).toBe("bad format");
  });

  test("returns undefined for a passing column", () => {
    expect(failureMessageForCell(record(), "id")).toBeUndefined();
  });

  test("returns undefined when no failure carries the column attribution", () => {
    const rec = record({
      failures: [{ rule_name: "row_rule", message: "row-level failure", columns: [] }],
    });
    expect(failureMessageForCell(rec, "email")).toBeUndefined();
  });

  test("returns undefined when the matching failure has a null message", () => {
    const rec = record({
      failures: [{ rule_name: "is_valid_email", message: null, columns: ["email"] }],
    });
    expect(failureMessageForCell(rec, "email")).toBeUndefined();
  });

  test("tolerates missing failures list", () => {
    expect(failureMessageForCell(record({ failures: undefined }), "email")).toBeUndefined();
  });
});

// dqlake fidelity: a failure whose `columns` is empty/undefined is a
// whole-row rule — the entire row gets tinted, not any one cell.
describe("rowHasWholeRowFailure", () => {
  test("false when every failure is attributed to specific columns", () => {
    expect(rowHasWholeRowFailure(record())).toBe(false);
  });

  test("true when a failure carries an empty columns list", () => {
    const rec = record({
      failures: [{ rule_name: "row_rule", message: "row-level failure", columns: [] }],
    });
    expect(rowHasWholeRowFailure(rec)).toBe(true);
  });

  test("true when a failure carries no columns field at all", () => {
    const rec = record({
      failures: [{ rule_name: "row_rule", message: "row-level failure" }],
    });
    expect(rowHasWholeRowFailure(rec)).toBe(true);
  });

  test("true when a whole-row failure sits alongside column-attributed ones", () => {
    const rec = record({
      failures: [
        { rule_name: "is_valid_email", message: "bad format", columns: ["email"] },
        { rule_name: "row_rule", message: "row-level failure", columns: [] },
      ],
    });
    expect(rowHasWholeRowFailure(rec)).toBe(true);
  });

  test("false when the failures list is missing", () => {
    expect(rowHasWholeRowFailure(record({ failures: undefined }))).toBe(false);
  });

  test("false when the failures list is empty", () => {
    expect(rowHasWholeRowFailure(record({ failures: [] }))).toBe(false);
  });
});

describe("wholeRowFailureClass", () => {
  test("returns a red row tint for whole-row failures", () => {
    const rec = record({
      failures: [{ rule_name: "row_rule", message: "row-level failure", columns: [] }],
    });
    expect(wholeRowFailureClass(rec)).toContain("bg-red-500/");
  });

  test("pairs the light tint with a dark: variant", () => {
    const rec = record({
      failures: [{ rule_name: "row_rule", message: "row-level failure", columns: [] }],
    });
    expect(wholeRowFailureClass(rec)).toMatch(/dark:bg-red-/);
  });

  test("returns undefined for rows with only column-attributed failures", () => {
    expect(wholeRowFailureClass(record())).toBeUndefined();
  });
});

describe("wholeRowFailureMessage", () => {
  test("returns the first column-less failure's message", () => {
    const rec = record({
      failures: [
        { rule_name: "is_valid_email", message: "bad format", columns: ["email"] },
        { rule_name: "row_rule_a", message: "first row-level failure", columns: [] },
        { rule_name: "row_rule_b", message: "second row-level failure" },
      ],
    });
    expect(wholeRowFailureMessage(rec)).toBe("first row-level failure");
  });

  test("returns undefined when the whole-row failure has a null message", () => {
    const rec = record({
      failures: [{ rule_name: "row_rule", message: null, columns: [] }],
    });
    expect(wholeRowFailureMessage(rec)).toBeUndefined();
  });

  test("returns undefined when there is no whole-row failure", () => {
    expect(wholeRowFailureMessage(record())).toBeUndefined();
  });
});

describe("displayCellValue", () => {
  test("returns the raw string value", () => {
    expect(displayCellValue(record(), "email")).toBe("not-an-email");
  });

  test("renders null values as an em dash", () => {
    expect(displayCellValue(record({ row_values: { id: null } }), "id")).toBe("—");
  });

  test("renders absent columns as an em dash", () => {
    expect(displayCellValue(record(), "missing")).toBe("—");
  });

  test("preserves empty strings (empty is a value, not absence)", () => {
    expect(displayCellValue(record({ row_values: { id: "" } }), "id")).toBe("");
  });
});

describe("sumMemberTestCounts", () => {
  const member = (over: Partial<TableScoreOut> = {}): TableScoreOut => ({
    source_table_fqn: "c.s.t",
    score: 0.9,
    total_tests: 10,
    failed_tests: 1,
    ...over,
  });

  test("sums total and failed tests across members", () => {
    const members = [
      member({ source_table_fqn: "c.s.t1", total_tests: 10, failed_tests: 1 }),
      member({ source_table_fqn: "c.s.t2", total_tests: 5, failed_tests: 2 }),
    ];
    expect(sumMemberTestCounts(members)).toEqual({ totalTests: 15, failedTests: 3 });
  });

  test("treats absent counts as zero (optional in the generated types)", () => {
    const members = [
      member({ source_table_fqn: "c.s.t1", total_tests: undefined, failed_tests: undefined, score: null }),
      member({ source_table_fqn: "c.s.t2", total_tests: 4, failed_tests: 1 }),
    ];
    expect(sumMemberTestCounts(members)).toEqual({ totalTests: 4, failedTests: 1 });
  });

  test("returns zeros for an empty member list", () => {
    expect(sumMemberTestCounts([])).toEqual({ totalTests: 0, failedTests: 0 });
  });

  test("returns zeros when the member list is absent", () => {
    expect(sumMemberTestCounts(undefined)).toEqual({ totalTests: 0, failedTests: 0 });
  });
});
