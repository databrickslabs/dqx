import { describe, expect, test } from "bun:test";
import { checkContrast } from "./contrast";
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
});
