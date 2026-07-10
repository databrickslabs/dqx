import { describe, expect, it } from "bun:test";
import {
  DULL_TABLE_COLORS,
  buildTableColorMap,
  computeOverallPoints,
  friendlyTableName,
} from "./ProductResultsTab";
import type { TrendPointOut } from "@/lib/api";

const A = "cat.sch.orders";
const B = "cat.sch.customers";

function pt(series: string, run_date: string, pass_rate: number | null = 1): TrendPointOut {
  return { series, run_date, pass_rate };
}

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
  const trend: TrendPointOut[] = [
    { run_date: "2026-01-01T00:00:00", pass_rate: 0.5 },
    { run_date: "2026-01-02T00:00:00", pass_rate: 0.75 },
    { run_date: "2026-01-03T00:00:00", pass_rate: 1 },
  ];

  it("is empty until EVERY required member table has run", () => {
    const byTable = [pt(A, "2026-01-01T00:00:00")]; // B never ran
    expect(computeOverallPoints(byTable, trend, [A, B])).toEqual([]);
  });

  it("is empty when there are no required members (no universe, no Average)", () => {
    expect(computeOverallPoints([pt(A, "2026-01-01T00:00:00")], trend, [])).toEqual([]);
  });

  it("starts the Average at the LATEST first-run across members and scales 0..100", () => {
    const byTable = [
      pt(A, "2026-01-01T00:00:00"),
      pt(A, "2026-01-03T00:00:00"),
      pt(B, "2026-01-02T00:00:00"), // B's first run sets the cutoff
    ];
    expect(computeOverallPoints(byTable, trend, [A, B])).toEqual([
      { run_date: "2026-01-02T00:00:00", value: 75 },
      { run_date: "2026-01-03T00:00:00", value: 100 },
    ]);
  });

  it("maps non-numeric pass rates to null values and coerces string rates", () => {
    const oneTable = [pt(A, "2026-01-01T00:00:00")];
    const mixedTrend = [
      { run_date: "2026-01-01T00:00:00", pass_rate: null },
      { run_date: "2026-01-02T00:00:00", pass_rate: "0.4" as unknown as number },
    ];
    expect(computeOverallPoints(oneTable, mixedTrend, [A])).toEqual([
      { run_date: "2026-01-01T00:00:00", value: null },
      { run_date: "2026-01-02T00:00:00", value: 40 },
    ]);
  });

  it("uses each member's EARLIEST run for the cutoff even when points arrive out of order", () => {
    const byTable = [
      pt(B, "2026-01-03T00:00:00"),
      pt(B, "2026-01-01T00:00:00"), // earlier B point arrives later in the list
      pt(A, "2026-01-02T00:00:00"),
    ];
    const points = computeOverallPoints(byTable, trend, [A, B]);
    expect(points.map((p) => p.run_date)).toEqual([
      "2026-01-02T00:00:00",
      "2026-01-03T00:00:00",
    ]);
  });
});
