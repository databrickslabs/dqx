import { describe, expect, it } from "bun:test";
import {
  clampWindow,
  countSeriesColors,
  COUNT_COLORS,
  fracFromChartY,
  isFullExtent,
  niceTimeTicks,
  panWindow,
  percentFromChartY,
  pivot,
  pivotCounts,
  toggleHidden,
  zoomWindow,
} from "./ScoreTrendChart";

describe("pivot", () => {
  it("pivots long rows into one wide row per run_date with pass_rate scaled to 0-100", () => {
    const { points, series } = pivot([
      { run_date: "2026-06-10T10:00:00Z", series: "Completeness", pass_rate: 0.9 },
      { run_date: "2026-06-10T10:00:00Z", series: "Validity", pass_rate: 1 },
      { run_date: "2026-06-11T10:00:00Z", series: "Completeness", pass_rate: 0.5 },
    ]);
    expect(series).toEqual(["Completeness", "Validity"]);
    expect(points).toHaveLength(2);
    expect(points[0]).toMatchObject({
      run_date: "2026-06-10T10:00:00Z",
      Completeness: 90,
      Validity: 100,
    });
    expect(points[1]).toMatchObject({ Completeness: 50 });
  });

  it("adds a numeric epoch-ms ts and sorts points by it", () => {
    const { points } = pivot([
      { run_date: "2026-06-11T10:00:00Z", pass_rate: 0.5 },
      { run_date: "2026-06-10T10:00:00Z", pass_rate: 1 },
    ]);
    expect(points.map((p) => p.ts)).toEqual([
      Date.parse("2026-06-10T10:00:00Z"),
      Date.parse("2026-06-11T10:00:00Z"),
    ]);
  });

  it("uses the default series name for rows without a series", () => {
    const { series } = pivot([{ run_date: "2026-06-10", pass_rate: 0.75 }]);
    expect(series).toEqual(["Pass rate"]);
  });

  it("honours a caller-supplied default series name (i18n path)", () => {
    const { series, points } = pivot(
      [{ run_date: "2026-06-10", pass_rate: 0.75 }],
      "Taxa de aprovação",
    );
    expect(series).toEqual(["Taxa de aprovação"]);
    expect(points[0]["Taxa de aprovação"]).toBe(75);
  });

  it("keeps null pass rates as null (gap), not 0", () => {
    const { points } = pivot([{ run_date: "2026-06-10", pass_rate: null }]);
    expect(points[0]["Pass rate"]).toBeNull();
  });
});

describe("pivotCounts", () => {
  it("pivots count rows without scaling the values", () => {
    const { points, series } = pivotCounts([
      { run_date: "2026-06-10", series: "Rules", value: 3 },
      { run_date: "2026-06-10", series: "Tests", value: 40 },
      { run_date: "2026-06-11", series: "Rules", value: 4 },
    ]);
    expect(series).toEqual(["Rules", "Tests"]);
    expect(points[0]).toMatchObject({ Rules: 3, Tests: 40 });
    expect(points[1]).toMatchObject({ Rules: 4 });
  });

  it("preserves null values and sorts by parsed run_date", () => {
    const { points } = pivotCounts([
      { run_date: "2026-06-11", series: "Rules", value: null },
      { run_date: "2026-06-10", series: "Rules", value: 1 },
    ]);
    expect(points.map((p) => p.run_date)).toEqual(["2026-06-10", "2026-06-11"]);
    expect(points[1].Rules).toBeNull();
  });
});

describe("niceTimeTicks", () => {
  it("returns evenly spaced ticks at a nice step enclosing the range", () => {
    const min = Date.parse("2026-06-10T11:26:00Z");
    const max = Date.parse("2026-06-10T11:53:00Z");
    const ticks = niceTimeTicks(min, max, 6);
    expect(ticks.length).toBeGreaterThan(1);
    // Constant step.
    const steps = new Set<number>();
    for (let i = 1; i < ticks.length; i++) steps.add(ticks[i] - ticks[i - 1]);
    expect(steps.size).toBe(1);
    const step = [...steps][0];
    // Every tick sits on a step boundary and the domain encloses the data.
    expect(ticks.every((t) => t % step === 0)).toBe(true);
    expect(ticks[0]).toBeLessThanOrEqual(min);
    expect(ticks[ticks.length - 1]).toBeGreaterThanOrEqual(max);
  });

  it("returns a single value for a degenerate (max <= min) range", () => {
    expect(niceTimeTicks(1000, 1000)).toEqual([1000]);
    expect(niceTimeTicks(2000, 1000)).toEqual([2000]);
  });

  it("returns [] for non-finite bounds", () => {
    expect(niceTimeTicks(Number.NaN, 1000)).toEqual([]);
    expect(niceTimeTicks(0, Number.POSITIVE_INFINITY)).toEqual([]);
  });

  it("caps the step at the largest nice step (7 days) for huge ranges", () => {
    const min = 0;
    const max = 365 * 86_400_000; // one year
    const ticks = niceTimeTicks(min, max, 6);
    expect(ticks[1] - ticks[0]).toBe(7 * 86_400_000);
  });
});

describe("toggleHidden", () => {
  it("adds an absent name and removes a present one, returning a NEW set", () => {
    const cur = new Set<string>(["Rules"]);
    const added = toggleHidden(cur, "Tests");
    expect(added.has("Tests")).toBe(true);
    expect(cur.has("Tests")).toBe(false); // original untouched
    const removed = toggleHidden(added, "Rules");
    expect(removed.has("Rules")).toBe(false);
  });
});

describe("countSeriesColors", () => {
  it("assigns the muted palette in order, cycling past its length", () => {
    const names = ["a", "b", "c", "d", "e", "f"];
    const colors = countSeriesColors(names);
    expect(colors.a).toBe(COUNT_COLORS[0]);
    expect(colors.e).toBe(COUNT_COLORS[4]);
    expect(colors.f).toBe(COUNT_COLORS[0]); // cycles
  });
});

describe("percentFromChartY", () => {
  // The percent-mode 2D box-zoom inverts the cursor pixel-y to a 0–100 value
  // against the fixed plot geometry (PLOT_TOP=8 maps to 100%, PLOT_BOTTOM=170
  // to 0%).
  const TOP = 8;
  const BOTTOM = 170;

  it("maps the plot-top pixel to 100% and the plot-bottom pixel to 0%", () => {
    expect(percentFromChartY(TOP, TOP, BOTTOM)).toBe(100);
    expect(percentFromChartY(BOTTOM, TOP, BOTTOM)).toBe(0);
  });

  it("maps the mid pixel to ~50%", () => {
    const mid = (TOP + BOTTOM) / 2;
    expect(percentFromChartY(mid, TOP, BOTTOM)).toBeCloseTo(50, 5);
  });

  it("clamps pixels above the plot top / below the plot bottom to 100 / 0", () => {
    expect(percentFromChartY(TOP - 40, TOP, BOTTOM)).toBe(100);
    expect(percentFromChartY(BOTTOM + 40, TOP, BOTTOM)).toBe(0);
  });

  it("returns 0 for a degenerate (non-positive) plot span", () => {
    expect(percentFromChartY(50, 100, 100)).toBe(0);
    expect(percentFromChartY(50, 100, 20)).toBe(0);
  });
});

describe("fracFromChartY", () => {
  const TOP = 8;
  const BOTTOM = 170;
  it("maps the plot bottom→0, top→1, mid→0.5", () => {
    expect(fracFromChartY(BOTTOM, TOP, BOTTOM)).toBe(0);
    expect(fracFromChartY(TOP, TOP, BOTTOM)).toBe(1);
    expect(fracFromChartY((TOP + BOTTOM) / 2, TOP, BOTTOM)).toBeCloseTo(0.5, 5);
  });
  it("clamps outside the plot to [0,1] and returns 0 for a degenerate span", () => {
    expect(fracFromChartY(TOP - 20, TOP, BOTTOM)).toBe(1);
    expect(fracFromChartY(BOTTOM + 20, TOP, BOTTOM)).toBe(0);
    expect(fracFromChartY(50, 100, 100)).toBe(0);
  });
});

describe("zoomWindow", () => {
  it("zooms IN (factor<1) around the anchor, keeping the anchor fixed", () => {
    // Anchor at the centre, halve the width.
    expect(zoomWindow(0, 100, 50, 0.5)).toEqual([25, 75]);
    // Anchor off-centre stays put.
    const [lo, hi] = zoomWindow(0, 100, 20, 0.5);
    expect(lo).toBe(10);
    expect(hi).toBe(60);
  });
  it("zooms OUT (factor>1) around the anchor", () => {
    expect(zoomWindow(25, 75, 50, 2)).toEqual([0, 100]);
  });
});

describe("clampWindow", () => {
  it("clamps a window inside [min,max] preserving order", () => {
    expect(clampWindow(-10, 130, 0, 100)).toEqual([0, 100]);
    expect(clampWindow(20, 80, 0, 100)).toEqual([20, 80]);
  });
});

describe("panWindow", () => {
  it("shifts the window by delta within bounds, preserving width", () => {
    expect(panWindow(20, 40, 10, 0, 100)).toEqual([30, 50]);
    expect(panWindow(20, 40, -10, 0, 100)).toEqual([10, 30]);
  });
  it("stops at the low/high edge instead of scrolling past", () => {
    expect(panWindow(10, 30, -50, 0, 100)).toEqual([0, 20]);
    expect(panWindow(80, 100, 50, 0, 100)).toEqual([80, 100]);
  });
  it("pins a window wider than the extent to [min,max]", () => {
    expect(panWindow(-10, 110, 5, 0, 100)).toEqual([0, 100]);
  });
});

describe("isFullExtent", () => {
  it("is true only when the window covers the whole extent", () => {
    expect(isFullExtent(0, 100, 0, 100)).toBe(true);
    expect(isFullExtent(-5, 120, 0, 100)).toBe(true);
    expect(isFullExtent(10, 90, 0, 100)).toBe(false);
  });
});
