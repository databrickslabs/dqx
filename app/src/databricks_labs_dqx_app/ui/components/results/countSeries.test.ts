import { describe, expect, it } from "bun:test";
import { toCountSeries } from "./countSeries";

describe("toCountSeries", () => {
  it("flattens per-run count rows into long-form series", () => {
    const rows = [{ run_date: "2026-06-10", rule_count: 3, check_count: 5, test_count: 40 }];
    const out = toCountSeries(rows, [
      { key: "rule_count", label: "Rules" },
      { key: "check_count", label: "Checks" },
      { key: "test_count", label: "Tests" },
    ]);
    expect(out).toEqual([
      { run_date: "2026-06-10", series: "Rules", value: 3 },
      { run_date: "2026-06-10", series: "Checks", value: 5 },
      { run_date: "2026-06-10", series: "Tests", value: 40 },
    ]);
  });

  it("emits one entry per map item per row, preserving row order", () => {
    const rows = [
      { run_date: "2026-06-10", failed_records: 9 },
      { run_date: "2026-06-11", failed_records: 4 },
    ];
    const out = toCountSeries(rows, [{ key: "failed_records", label: "Rows" }]);
    expect(out.map((r) => [r.run_date, r.value])).toEqual([
      ["2026-06-10", 9],
      ["2026-06-11", 4],
    ]);
  });

  it("turns non-numeric or missing keys into null values", () => {
    const out = toCountSeries([{ run_date: "d", rule_count: "3" }], [
      { key: "rule_count", label: "Rules" },
    ]);
    expect(out).toEqual([{ run_date: "d", series: "Rules", value: null }]);
  });

  it("stringifies a missing run_date to empty string", () => {
    const out = toCountSeries([{ rule_count: 1 }], [{ key: "rule_count", label: "Rules" }]);
    expect(out).toEqual([{ run_date: "", series: "Rules", value: 1 }]);
  });

  it("yields an empty series for undefined rows", () => {
    expect(toCountSeries(undefined, [{ key: "rule_count", label: "Rules" }])).toEqual([]);
  });
});
