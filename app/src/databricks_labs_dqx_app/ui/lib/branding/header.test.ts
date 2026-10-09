import { describe, expect, test } from "bun:test";
import { headerTitle, logoUrl, pickHeaderLogo } from "./header";

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
  test("separate with missing dark logo falls back to the DQX icon", () => {
    expect(pickHeaderLogo({ logoMode: "separate", logos: { light: logos.light, dark: null } }, true)).toBeNull();
  });
  test("no logos", () => expect(pickHeaderLogo({ logoMode: "shared", logos: { light: null, dark: null } }, false)).toBeNull());
  test("logoUrl", () => expect(logoUrl("dark", "abc")).toBe("/api/v1/config/branding/logo/dark?v=abc"));
});
