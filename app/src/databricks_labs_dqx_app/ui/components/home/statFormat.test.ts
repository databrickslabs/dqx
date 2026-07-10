import { describe, expect, it } from "bun:test";
import { countUpValue, formatCount, formatScorePercent } from "./statFormat";

describe("countUpValue", () => {
  it("starts at 0 and lands exactly on the target", () => {
    expect(countUpValue(120, 0)).toBe(0);
    expect(countUpValue(120, 1)).toBe(120);
  });

  it("eases out (fast start, slow finish)", () => {
    // easeOutCubic: at half time the value is well past half the target.
    expect(countUpValue(100, 0.5)).toBeCloseTo(87.5);
  });

  it("is monotonically non-decreasing over the animation", () => {
    let prev = -1;
    for (let t = 0; t <= 1.0001; t += 0.05) {
      const v = countUpValue(42, Math.min(t, 1));
      expect(v).toBeGreaterThanOrEqual(prev);
      prev = v;
    }
  });

  it("clamps progress outside [0, 1]", () => {
    expect(countUpValue(50, -0.2)).toBe(0);
    expect(countUpValue(50, 1.7)).toBe(50);
  });
});

describe("formatCount", () => {
  it("rounds the animated value to a whole number", () => {
    expect(formatCount(11.6)).toBe("12");
    expect(formatCount(0.2)).toBe("0");
  });

  it("localizes thousands", () => {
    expect(formatCount(1234)).toBe((1234).toLocaleString());
  });
});

describe("formatScorePercent", () => {
  it("renders an em dash when the cache has never produced a score", () => {
    expect(formatScorePercent(null, 0)).toBe("—");
    expect(formatScorePercent(undefined, 0)).toBe("—");
  });

  it("renders the animated percentage to one decimal", () => {
    expect(formatScorePercent(0.9134, 91.34)).toBe("91.3%");
    expect(formatScorePercent(1, 100)).toBe("100.0%");
    expect(formatScorePercent(0, 0)).toBe("0.0%");
  });
});
