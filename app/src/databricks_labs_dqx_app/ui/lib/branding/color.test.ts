import { describe, expect, test } from "bun:test";
import {
  contrastRatio, hexToOklch, isHex, mix, oklchToHex, readableOn, tint, withLightness,
} from "./color";

describe("isHex", () => {
  test.each(["#000000", "#ff3621", "#ABCDEF"])("accepts %s", (v) => expect(isHex(v)).toBe(true));
  test.each(["#fff", "red", "#FF3621;}", "#FF362100", "", null, 7, " #000000"])("rejects %p", (v) =>
    expect(isHex(v)).toBe(false),
  );
});

describe("oklch round trip", () => {
  test.each(["#000000", "#FFFFFF", "#FF3621", "#3F0E40", "#1164A3", "#88C0D0"])("%s", (hex) => {
    expect(oklchToHex(hexToOklch(hex))).toBe(hex);
  });
  test("white has lightness 1 and no chroma", () => {
    const w = hexToOklch("#FFFFFF");
    expect(w.l).toBeCloseTo(1, 3);
    expect(w.c).toBeCloseTo(0, 3);
  });
});

describe("mix / withLightness / tint", () => {
  test("mix endpoints", () => {
    expect(mix("#000000", "#FFFFFF", 0)).toBe("#000000");
    expect(mix("#000000", "#FFFFFF", 1)).toBe("#FFFFFF");
  });
  test("withLightness changes only lightness", () => {
    const out = hexToOklch(withLightness("#1164A3", 0.3));
    expect(out.l).toBeCloseTo(0.3, 1);
  });
  test("tint with a grey brand leaves the colour unchanged", () => {
    expect(tint("#F5F5F5", "#333333", 0.1)).toBe("#F5F5F5");
  });
  test("tint with a blue brand adds blue chroma", () => {
    expect(hexToOklch(tint("#F5F5F5", "#1164A3", 0.1)).c).toBeGreaterThan(0.005);
  });
});

describe("contrast", () => {
  test("black on white is 21:1", () => expect(contrastRatio("#000000", "#FFFFFF")).toBeCloseTo(21, 1));
  test("same colour is 1:1", () => expect(contrastRatio("#777777", "#777777")).toBeCloseTo(1, 5));
  test.each(["#000000", "#FFFFFF", "#FF3621", "#3F0E40", "#808080", "#FDF6E3", "#1164A3"])(
    "readableOn(%s) gives at least 4.5:1",
    (bg) => expect(contrastRatio(bg, readableOn(bg))).toBeGreaterThanOrEqual(4.5),
  );
});
