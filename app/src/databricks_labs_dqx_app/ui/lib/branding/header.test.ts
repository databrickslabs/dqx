import { describe, expect, test } from "bun:test";
import { deriveAllTokens } from "./derive";
import { headerSwatches, headerTitle, logoUrl, pickHeaderLogo } from "./header";

describe("headerTitle", () => {
  test("without company", () => expect(headerTitle(null)).toEqual({ product: "DQX Studio", company: null }));
  test("with company", () => expect(headerTitle("Acme")).toEqual({ product: "DQX Studio", company: "Acme" }));
  test("blank company is ignored", () => expect(headerTitle("  ")).toEqual({ product: "DQX Studio", company: null }));
});

describe("pickHeaderLogo", () => {
  const logos = { light: "aaaaaaaaaaaaaaaa", dark: "bbbbbbbbbbbbbbbb" };
  test("shared uses light in both modes", () => {
    expect(pickHeaderLogo({ logoMode: "shared", logos }, true)).toEqual({ slot: "light", hash: logos.light });
  });
  test("separate uses the mode's logo", () => {
    expect(pickHeaderLogo({ logoMode: "separate", logos }, true)).toEqual({ slot: "dark", hash: logos.dark });
    expect(pickHeaderLogo({ logoMode: "separate", logos }, false)).toEqual({ slot: "light", hash: logos.light });
  });
  test("separate with missing dark logo falls back to the light logo", () => {
    expect(pickHeaderLogo({ logoMode: "separate", logos: { light: logos.light, dark: null } }, true)).toEqual({
      slot: "light",
      hash: logos.light,
    });
  });
  test("separate with no logos at all shows the DQX icon", () => {
    expect(pickHeaderLogo({ logoMode: "separate", logos: { light: null, dark: null } }, true)).toBeNull();
  });
  test("no logos", () => expect(pickHeaderLogo({ logoMode: "shared", logos: { light: null, dark: null } }, false)).toBeNull());
  test("logoUrl", () => expect(logoUrl("dark", "abc")).toBe("/api/v1/config/branding/logo/dark?v=abc"));
});

describe("headerSwatches", () => {
  test("DQX Default uses the default header colours", () => {
    const sw = headerSwatches({ light: {}, darkCustomised: false, dark: {} });
    expect(sw.light.background).toBe(deriveAllTokens("light", {})["--header"]);
    expect(sw.dark.background).toBe(deriveAllTokens("dark", {})["--header"]);
  });
  test("follows the saved header colour per mode", () => {
    const sw = headerSwatches({ light: { header: "#3F0E40" }, darkCustomised: true, dark: { header: "#1D2026" } });
    expect(sw.light).toEqual({ background: "#3F0E40", foreground: deriveAllTokens("light", { header: "#3F0E40" })["--header-foreground"] });
    expect(sw.dark.background).toBe("#1D2026");
  });
});
