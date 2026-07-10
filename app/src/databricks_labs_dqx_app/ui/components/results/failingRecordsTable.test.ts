import { describe, expect, it } from "bun:test";
import { clampTooltip } from "./FailingRecordsTable";

describe("clampTooltip", () => {
  const VW = 1024;
  const VH = 768;

  it("offsets the tooltip down-right of the cursor by default", () => {
    expect(clampTooltip(100, 100, VW, VH)).toEqual({ left: 112, top: 112 });
  });

  it("clamps horizontally at the right edge", () => {
    const { left } = clampTooltip(VW - 10, 100, VW, VH, 320);
    expect(left).toBe(VW - 320);
  });

  it("never pushes left below 0 on a narrow viewport", () => {
    const { left } = clampTooltip(100, 100, 200, VH, 320);
    expect(left).toBe(0);
  });

  it("flips above the cursor near the bottom using the measured height", () => {
    const h = 120;
    const { top } = clampTooltip(100, VH - 20, VW, VH, 320, h);
    // Bottom edge sits TT_FLIP_GAP (8px) above the cursor.
    expect(top).toBe(VH - 20 - h - 8);
  });

  it("never pushes top below 0 when flipping on a short viewport", () => {
    const { top } = clampTooltip(100, 90, 1024, 100, 320, 200);
    expect(top).toBe(0);
  });
});
