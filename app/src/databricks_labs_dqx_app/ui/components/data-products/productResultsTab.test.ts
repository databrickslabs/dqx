import { describe, expect, it } from "bun:test";
import {
  DULL_TABLE_COLORS,
  buildTableColorMap,
  computeOverallPoints,
  friendlyTableName,
  productBreakdownParams,
  productRunPickerRuns,
} from "./ProductResultsTab";
import { EMPTY_FILTERS } from "@/components/monitored-tables/BindingResultsTab";
import type { TrendPointOut } from "@/lib/api";

const A = "cat.sch.orders";
const B = "cat.sch.customers";

function pt(series: string, run_date: string, pass_rate: number | null = 1): TrendPointOut {
  return { series, run_date, pass_rate };
}

describe("productRunPickerRuns", () => {
  it("offers ONLY the latest run (per-table run_ids are not product batches)", () => {
    const runs = [
      { run_id: "r3", run_ts: "2026-01-03T00:00:00" },
      { run_id: "r2", run_ts: "2026-01-02T00:00:00" },
      { run_id: "r1", run_ts: "2026-01-01T00:00:00" },
    ];
    expect(productRunPickerRuns(runs)).toEqual([runs[0]]);
  });

  it("is empty when there are no runs", () => {
    expect(productRunPickerRuns([])).toEqual([]);
  });
});

describe("productBreakdownParams", () => {
  it("NEVER carries a run_id filter (a member-table run_id cannot scope the product view)", () => {
    const params = productBreakdownParams({
      ...EMPTY_FILTERS,
      dimension: ["Completeness"],
      runId: "r2", // even a stray runId in the filters must not leak through
    });
    expect("run_id" in params).toBe(false);
    expect(params).toEqual({
      dimension: ["Completeness"],
      severity: undefined,
      rule: undefined,
      column: undefined,
      axes: "breakdown",
    });
  });

  it("omits empty facets and pins axes to breakdown for the base (unfiltered) query", () => {
    expect(productBreakdownParams(EMPTY_FILTERS)).toEqual({
      dimension: undefined,
      severity: undefined,
      rule: undefined,
      column: undefined,
      axes: "breakdown",
    });
  });
});

describe("friendlyTableName", () => {
  it("returns the last FQN segment", () => {
    expect(friendlyTableName("cat.sch.orders")).toBe("orders");
  });

  it("passes through a segment-less name and maps null/undefined to empty", () => {
    expect(friendlyTableName("orders")).toBe("orders");
    expect(friendlyTableName(null)).toBe("");
    expect(friendlyTableName(undefined)).toBe("");
  });
});

describe("buildTableColorMap", () => {
  it("assigns stable dull colours by first-seen series order", () => {
    const map = buildTableColorMap([pt(B, "2026-01-02"), pt(A, "2026-01-01"), pt(B, "2026-01-03")]);
    expect(map[B]).toBe(DULL_TABLE_COLORS[0]);
    expect(map[A]).toBe(DULL_TABLE_COLORS[1]);
  });

  it("cycles the palette past its length and skips series-less points", () => {
    const points = [
      ...DULL_TABLE_COLORS.map((_, i) => pt(`t${i}`, "2026-01-01")),
      pt("extra", "2026-01-01"),
      { run_date: "2026-01-01", pass_rate: 1 },
    ];
    const map = buildTableColorMap(points);
    expect(map.extra).toBe(DULL_TABLE_COLORS[0]);
    expect(Object.keys(map)).toHaveLength(DULL_TABLE_COLORS.length + 1);
  });

  it("is empty for undefined input", () => {
    expect(buildTableColorMap(undefined)).toEqual({});
  });
});

describe("computeOverallPoints", () => {
  // The server's trend IS the as-of carry-forward average (UC view
  // v_dq_check_results_asof); this helper only scales it 0..100. The
  // former allRanSince gate is deliberately gone: the line starts at the
  // FIRST member's first run and each member joins at its own first run.
  const trend: TrendPointOut[] = [
    { run_date: "2026-01-01T00:00:00", pass_rate: 0.5 },
    { run_date: "2026-01-02T00:00:00", pass_rate: 0.75 },
    { run_date: "2026-01-03T00:00:00", pass_rate: 1 },
  ];

  it("plots the server series as-is, scaled 0..100 (no membership gate)", () => {
    expect(computeOverallPoints(trend)).toEqual([
      { run_date: "2026-01-01T00:00:00", value: 50 },
      { run_date: "2026-01-02T00:00:00", value: 75 },
      { run_date: "2026-01-03T00:00:00", value: 100 },
    ]);
  });

  it("maps non-numeric pass rates to null values and coerces string rates", () => {
    const mixedTrend = [
      { run_date: "2026-01-01T00:00:00", pass_rate: null },
      { run_date: "2026-01-02T00:00:00", pass_rate: "0.4" as unknown as number },
    ];
    expect(computeOverallPoints(mixedTrend)).toEqual([
      { run_date: "2026-01-01T00:00:00", value: null },
      { run_date: "2026-01-02T00:00:00", value: 40 },
    ]);
  });

  it("is empty for an undefined or empty server series", () => {
    expect(computeOverallPoints(undefined)).toEqual([]);
    expect(computeOverallPoints([])).toEqual([]);
  });
});
