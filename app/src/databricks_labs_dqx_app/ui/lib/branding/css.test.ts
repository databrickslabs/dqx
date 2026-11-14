import { describe, expect, test } from "bun:test";
import { themeOverrides, toStyleSheet } from "./css";
import { THEMABLE_TOKENS } from "./derive";

describe("toStyleSheet", () => {
  test("empty for DQX Default", () => {
    expect(toStyleSheet(themeOverrides({ light: {}, darkCustomised: false, dark: {} }))).toBe("");
  });
  test("emits scoped light and dark rules", () => {
    const css = toStyleSheet(themeOverrides({ light: { header: "#3F0E40" }, darkCustomised: false, dark: {} }));
    expect(css).toContain("html:root:not(.dark){");
    expect(css).toContain("--header:#3F0E40;");
    expect(css).toContain("html.dark{");
  });
  test("drops injection attempts and unknown tokens", () => {
    const css = toStyleSheet({
      light: { "--background": "red;}body{display:none", "--evil": "#000000", "--header": "#123456" },
      dark: { "--background": "#000000;}" },
    });
    expect(css).toBe("html:root:not(.dark){--header:#123456;}");
  });
  test("light rule never matches in dark mode", () => {
    const css = toStyleSheet(themeOverrides({ light: { page_background: "#FFF8E7" }, darkCustomised: true, dark: {} }));
    const lightRules = css.split("}").filter((r) => r.includes("--background:#FFF8E7"));
    expect(lightRules.length).toBe(1);
    expect(lightRules[0].startsWith("html:root:not(.dark){")).toBe(true);
  });
});

describe("themeOverrides", () => {
  test("customised dark with fewer groups still declares every light token", () => {
    const o = themeOverrides({
      light: { page_background: "#FFF8E7", header: "#3F0E40", brand: "#5E81AC" },
      darkCustomised: true,
      dark: { header: "#1D2026" },
    });
    for (const token of Object.keys(o.light)) expect(o.dark[token]).toBeDefined();
    expect(o.dark["--header"]).toBe("#1D2026");
  });
  test("customised empty dark falls back to DQX Default dark values", () => {
    const o = themeOverrides({ light: { page_background: "#FFF8E7" }, darkCustomised: true, dark: {} });
    expect(o.dark["--background"]).not.toBe("#FFF8E7");
    expect(Object.keys(o.dark).every((k) => THEMABLE_TOKENS.includes(k))).toBe(true);
  });
  test("dark-only customisation emits no light rule", () => {
    const o = themeOverrides({ light: {}, darkCustomised: true, dark: { header: "#1D2026" } });
    expect(o.light).toEqual({});
    expect(o.dark["--header"]).toBe("#1D2026");
  });
});
