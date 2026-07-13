import { describe, expect, it } from "bun:test";
import { chooseChartKind, planChart, toNumber } from "./GenieResultChart";

describe("toNumber", () => {
  it("parses plain numbers and strips a trailing %", () => {
    expect(toNumber("42")).toBe(42);
    expect(toNumber(" 97.5% ")).toBe(97.5);
  });

  it("returns null for null / empty / non-numeric cells", () => {
    expect(toNumber(null)).toBeNull();
    expect(toNumber("")).toBeNull();
    expect(toNumber("abc")).toBeNull();
  });
});

describe("planChart", () => {
  it("plans a bar chart for a category + numeric measure", () => {
    const plan = planChart(
      ["rule_name", "failed_tests"],
      [
        ["Email Format Valid", "408"],
        ["Account Tier Valid", "12"],
      ],
    );
    expect(plan?.kind).toBe("bar");
    expect(plan?.valueName).toBe("failed_tests");
    expect(plan?.data).toEqual([
      { label: "Email Format Valid", value: 408 },
      { label: "Account Tier Valid", value: 12 },
    ]);
  });

  it("plans a line chart when the label column is time-like", () => {
    const plan = planChart(
      ["date", "pass_rate"],
      [
        ["2026-07-01", "99.1%"],
        ["2026-07-02", "97.4%"],
      ],
    );
    expect(plan?.kind).toBe("line");
    expect(plan?.data.map((d) => d.value)).toEqual([99.1, 97.4]);
  });

  it("snake_case run_date is NOT time-like (dqlake's regex; documents the port)", () => {
    // TIME_RE's `run\s*date` / `\bdate\b` don't cross the underscore — a
    // `run_date` label charts as a bar. Faithful to dqlake, pinned here so a
    // future "fix" is a conscious deviation.
    const plan = planChart(
      ["run_date", "pass_rate"],
      [
        ["2026-07-01", "99.1%"],
        ["2026-07-02", "97.4%"],
      ],
    );
    expect(plan?.kind).toBe("bar");
  });

  it("plans a line when there are many categories (long ordered axis)", () => {
    const rows = Array.from({ length: 10 }, (_, i) => [`c${i}`, String(i)]);
    const plan = planChart(["category", "failed_tests"], rows);
    expect(plan?.kind).toBe("line");
  });

  it("keeps a bar for a few unordered categories", () => {
    const plan = planChart(
      ["rule_name", "failed_tests"],
      [
        ["Email Format Valid", "408"],
        ["Account Tier Valid", "12"],
        ["Phone Valid", "77"],
      ],
    );
    expect(plan?.kind).toBe("bar");
  });

  it("prefers a rate/score-named measure over other numeric columns", () => {
    const plan = planChart(
      ["rule", "widgets", "pass_rate"],
      [
        ["a", "10", "90"],
        ["b", "20", "80"],
      ],
    );
    expect(plan?.valueName).toBe("pass_rate");
  });

  it("never charts identifier-ish numeric columns", () => {
    expect(
      planChart(
        ["name", "rule_id"],
        [
          ["a", "101"],
          ["b", "102"],
        ],
      ),
    ).toBeNull();
  });

  it("never uses a JSON-object column as the label axis", () => {
    expect(
      planChart(
        ["failing_record", "rules_failed"],
        [
          ['{"a":1}', "3"],
          ['{"a":2}', "2"],
        ],
      ),
    ).toBeNull();
  });

  it("returns null for single-row or single-column results", () => {
    expect(planChart(["rule", "failed"], [["a", "1"]])).toBeNull();
    expect(planChart(["failed"], [["1"], ["2"]])).toBeNull();
  });

  it("returns null when there is no numeric measure", () => {
    expect(
      planChart(
        ["rule", "severity"],
        [
          ["a", "High"],
          ["b", "Low"],
        ],
      ),
    ).toBeNull();
  });

  it("caps the plotted points at 12", () => {
    const rows = Array.from({ length: 20 }, (_, i) => [`c${i}`, String(i)]);
    const plan = planChart(["category", "failed_tests"], rows);
    expect(plan?.data).toHaveLength(12);
  });
});

describe("chooseChartKind", () => {
  it("chooses line for a time-like column name", () => {
    expect(chooseChartKind("month", ["Jan", "Feb", "Mar"])).toBe("line");
  });

  it("chooses line for all-numeric monotonic labels", () => {
    expect(chooseChartKind("bucket", ["1", "2", "3"])).toBe("line");
    expect(chooseChartKind("bucket", ["30", "20", "10"])).toBe("line");
  });

  it("chooses bar for all-numeric but unordered labels", () => {
    expect(chooseChartKind("bucket", ["3", "1", "2"])).toBe("bar");
  });

  it("chooses line once there are many categories", () => {
    const many = Array.from({ length: 8 }, (_, i) => `c${i}`);
    expect(chooseChartKind("category", many)).toBe("line");
  });

  it("chooses bar for a few unordered categories", () => {
    expect(chooseChartKind("severity", ["High", "Low", "Medium"])).toBe("bar");
  });
});
