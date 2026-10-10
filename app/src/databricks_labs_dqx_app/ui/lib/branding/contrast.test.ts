import { describe, expect, test } from "bun:test";
import { checkContrast, CONTRAST_PAIR_FIELDS, CONTRAST_PAIRS } from "./contrast";
import { deriveAllTokens } from "./derive";

describe("checkContrast", () => {
  test("DQX Default has no warnings", () => {
    expect(checkContrast("light", deriveAllTokens("light", {}))).toEqual([]);
    expect(checkContrast("dark", deriveAllTokens("dark", {}))).toEqual([]);
  });
  test("grey text on grey page warns", () => {
    const warnings = checkContrast("light", deriveAllTokens("light", { page_background: "#888888", text: "#999999" }));
    expect(warnings.some((w) => w.pair === "text")).toBe(true);
    expect(warnings.every((w) => w.ratio < w.min)).toBe(true);
  });
  test("every warning names the colour groups to adjust", () => {
    const warnings = checkContrast("light", deriveAllTokens("light", { page_background: "#888888", text: "#999999" }));
    const text = warnings.find((w) => w.pair === "text");
    expect(text?.fields).toEqual(["text", "page_background"]);
  });
  test("every pair maps to at least one colour group", () => {
    for (const [pair] of CONTRAST_PAIRS) expect(CONTRAST_PAIR_FIELDS[pair]?.length).toBeGreaterThan(0);
  });
});
