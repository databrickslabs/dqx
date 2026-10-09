import { describe, expect, test } from "bun:test";
import { themeOverrides, toStyleSheet } from "./css";

describe("toStyleSheet", () => {
  test("empty for DQX Default", () => {
    expect(toStyleSheet(themeOverrides({ light: {}, darkCustomised: false, dark: {} }))).toBe("");
  });
  test("emits scoped light and dark rules", () => {
    const css = toStyleSheet(themeOverrides({ light: { header: "#3F0E40" }, darkCustomised: false, dark: {} }));
    expect(css).toContain("html:root{");
    expect(css).toContain("--header:#3F0E40;");
    expect(css).toContain("html.dark{");
  });
  test("drops injection attempts and unknown tokens", () => {
    const css = toStyleSheet({
      light: { "--background": "red;}body{display:none", "--evil": "#000000", "--header": "#123456" },
      dark: { "--background": "#000000;}" },
    });
    expect(css).toBe("html:root{--header:#123456;}");
  });
});
