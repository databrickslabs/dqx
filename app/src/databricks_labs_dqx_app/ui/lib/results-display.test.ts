import { describe, expect, test } from "bun:test";
import type { FailingRecordOut } from "@/lib/api";
import {
  SCORE_BAND_AMBER_MIN,
  SCORE_BAND_GREEN_MIN,
  deriveRecordColumns,
  displayCellValue,
  failedCellClass,
  failureMessageForCell,
  formatScorePercent,
  isFailedCell,
  scoreBandClass,
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
