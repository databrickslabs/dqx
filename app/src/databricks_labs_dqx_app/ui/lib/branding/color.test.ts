import { describe, expect, test } from "bun:test";
import {
  contrastRatio, hexToOklch, isHex, mix, oklchToHex, parseHexInput, readableOn, tint, withLightness,
} from "./color";

describe("isHex", () => {
  test.each(["#000000", "#ff3621", "#ABCDEF"])("accepts %s", (v) => expect(isHex(v)).toBe(true));
  test.each(["#fff", "red", "#FF3621;}", "#FF362100", "", null, 7, " #000000"])("rejects %p", (v) =>
    expect(isHex(v)).toBe(false),
  );
});

describe("parseHexInput", () => {
  test.each([
    ["#1a2b3c", "#1A2B3C"],
    ["1A2B3C", "#1A2B3C"],
    ["  #1a2b3c  ", "#1A2B3C"],
    ["#FFF", "#FFFFFF"],
    ["abc", "#AABBCC"],
    [" #0f0 ", "#00FF00"],
  ])("accepts %p", (input, expected) => expect(parseHexInput(input)).toBe(expected));
  test.each(["", "#", "red", "#12345", "#1234567", "#GGGGGG", "#FF3621;}", "# FFF"])("rejects %p", (input) =>
    expect(parseHexInput(input)).toBeNull(),
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
  test("withLightness preserves hue and chroma (in-gamut)", () => {
    const before = hexToOklch("#6B7A8F");
    const out = hexToOklch(withLightness("#6B7A8F", 0.45));
    expect(out.l).toBeCloseTo(0.45, 2);
    expect(out.c).toBeCloseTo(before.c, 2);
    expect(Math.abs(out.h - before.h)).toBeLessThan(2);
  });
  test("withLightness clamps out-of-gamut targets", () => {
    const result = withLightness("#1164A3", 0.3);
    expect(isHex(result)).toBe(true);
    const out = hexToOklch(result);
    expect(Math.abs(out.l - 0.3)).toBeLessThan(0.06);
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
  test.each(["#000000", "#FFFFFF", "#FF3621", "#3F0E40", "#808080", "#FDF6E3", "#1164A3", "#757575", "#777777", "#7A7A7A", "#E0457B"])(
    "readableOn(%s) gives at least 4.5:1",
    (bg) => expect(contrastRatio(bg, readableOn(bg))).toBeGreaterThanOrEqual(4.5),
  );
});
