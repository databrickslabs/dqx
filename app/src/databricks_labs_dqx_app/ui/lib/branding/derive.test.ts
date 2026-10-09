import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { hexToOklch } from "./color";
import { DEFAULT_GROUPS } from "./groups";
import { deriveAllTokens, deriveOverrides, effectiveDark, generateDark, THEMABLE_TOKENS } from "./derive";

const css = readFileSync(join(import.meta.dir, "../../styles/globals.css"), "utf8");
function block(selector: string): string {
  const start = css.indexOf(`${selector} {`);
  return css.slice(start, css.indexOf("}", start));
}
function oklchL(blockText: string, token: string): number {
  const m = blockText.match(new RegExp(`${token}:\\s*oklch\\(([0-9.]+)`));
  if (!m) throw new Error(`missing ${token}`);
  return Number(m[1]);
}

describe("DEFAULT_GROUPS match globals.css", () => {
  const cases: [string, "light" | "dark", string, string][] = [
    [":root", "light", "page_background", "--background"],
    [":root", "light", "text", "--foreground"],
    [":root", "light", "brand", "--primary"],
    [":root", "light", "sidebar", "--sidebar"],
    [".dark", "dark", "page_background", "--background"],
    [".dark", "dark", "text", "--foreground"],
    [".dark", "dark", "brand", "--primary"],
    [".dark", "dark", "sidebar", "--sidebar"],
  ];
  test.each(cases)("%s %s %s", (sel, mode, group, token) => {
    const expected = oklchL(block(sel), token);
    expect(hexToOklch(DEFAULT_GROUPS[mode][group as keyof (typeof DEFAULT_GROUPS)["light"]]).l).toBeCloseTo(expected, 2);
  });
  test("header defaults to the page background", () => {
    expect(DEFAULT_GROUPS.light.header).toBe(DEFAULT_GROUPS.light.page_background);
    expect(DEFAULT_GROUPS.dark.header).toBe(DEFAULT_GROUPS.dark.page_background);
  });
});

describe("deriveOverrides", () => {
  test("DQX Default emits nothing", () => {
    expect(deriveOverrides("light", {})).toEqual({});
    expect(deriveOverrides("dark", {})).toEqual({});
  });
  test("header only touches header tokens", () => {
    const out = deriveOverrides("light", { header: "#3F0E40" });
    expect(Object.keys(out).sort()).toEqual(["--header", "--header-foreground"]);
    expect(out["--header"]).toBe("#3F0E40");
    expect(out["--header-foreground"]).toBe("#FFFFFF");
  });
  test("page background drives surfaces, hover and borders", () => {
    const keys = Object.keys(deriveOverrides("light", { page_background: "#FDF6E3" }));
    for (const k of ["--background", "--card", "--popover", "--accent", "--secondary", "--muted", "--border", "--input", "--muted-foreground"]) {
      expect(keys).toContain(k);
    }
    expect(keys).not.toContain("--header");
    expect(keys).not.toContain("--sidebar");
  });
  test("brand sets primary with a readable foreground", () => {
    const out = deriveOverrides("light", { brand: "#FF3621" });
    expect(out["--primary"]).toBe("#FF3621");
    expect(["#000000", "#FFFFFF"]).toContain(out["--primary-foreground"]);
  });
  test("never emits chart, destructive or radius tokens", () => {
    const out = deriveOverrides("dark", { header: "#111111", page_background: "#222222", text: "#EEEEEE", brand: "#88C0D0", sidebar: "#333333" });
    for (const k of Object.keys(out)) {
      expect(k.startsWith("--chart") || k.startsWith("--destructive") || k === "--radius").toBe(false);
      expect(THEMABLE_TOKENS).toContain(k);
    }
  });
});

describe("dark mode", () => {
  test("generateDark only generates groups that were set", () => {
    expect(Object.keys(generateDark({ header: "#3F0E40" }))).toEqual(["header"]);
  });
  test("generated dark page background is dark and text is light", () => {
    const d = generateDark({ page_background: "#FFFFFF", text: "#111111" });
    expect(hexToOklch(d.page_background!).l).toBeLessThan(0.3);
    expect(hexToOklch(d.text!).l).toBeGreaterThan(0.8);
  });
  test("effectiveDark uses custom colours only when customised", () => {
    expect(effectiveDark({ brand: "#FF3621" }, true, { brand: "#00FF00" })).toEqual({ brand: "#00FF00" });
    expect(effectiveDark({}, false, { brand: "#00FF00" })).toEqual({});
  });
  test("deriveAllTokens is complete for every mode", () => {
    for (const mode of ["light", "dark"] as const) {
      expect(Object.keys(deriveAllTokens(mode, {})).sort()).toEqual([...THEMABLE_TOKENS].sort());
    }
  });
});
