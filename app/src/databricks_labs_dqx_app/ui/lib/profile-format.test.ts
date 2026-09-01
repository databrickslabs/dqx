import { describe, expect, it } from "vitest";
import {
  LLM_PK_SUMMARY_KEY,
  columnProfilePercents,
  columnStatsFromSummary,
  formatProfileDuration,
} from "./profile-format";

describe("columnStatsFromSummary", () => {
  it("drops the LLM PK blob and keeps column objects", () => {
    const summary = {
      id: { count: 10, count_null: 0, count_distinct: 10 },
      [LLM_PK_SUMMARY_KEY]: { detected_columns: ["id"], confidence: "high" },
      noise: "skip-me",
    };
    expect(columnStatsFromSummary(summary)).toEqual({
      id: { count: 10, count_null: 0, count_distinct: 10 },
    });
  });
});

describe("columnProfilePercents", () => {
  it("formats null and distinct shares", () => {
    expect(
      columnProfilePercents(
        { count: 100, count_null: 5, count_non_null: 95, count_distinct: 40 },
        100,
      ),
    ).toEqual({ nullPct: "5.0%", completePct: "95%", distinctPct: "40%" });
  });

  it("returns dashes when stats are missing", () => {
    expect(columnProfilePercents(undefined, null)).toEqual({
      nullPct: "—",
      completePct: "—",
      distinctPct: "—",
    });
  });
});

describe("formatProfileDuration", () => {
  it("formats seconds and minutes", () => {
    expect(formatProfileDuration(null)).toBe("—");
    expect(formatProfileDuration(12)).toBe("12s");
    expect(formatProfileDuration(125)).toBe("2m 5s");
  });
});
