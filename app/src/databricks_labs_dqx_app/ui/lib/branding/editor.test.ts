import { describe, expect, test } from "bun:test";
import { generateDark, generateLight } from "./derive";
import { applyCustomPreset, applyPreset, customPresetNumber, draftDark, draftFromApi, draftToTheme, draftWarnings, fileToLogoPayload, isDirty, MAX_LOGO_BYTES, setGroup } from "./editor";

describe("theme editor helpers", () => {
  test("applyPreset loads both modes and records the preset", () => {
    const d = applyPreset("aubergine");
    expect(d.preset).toBe("aubergine");
    expect(d.light.header).toBe("#3F0E40");
    expect(d.dark.header).toBe("#2C092D");
  });
  test("dqx-default clears colours", () => {
    expect(applyPreset("dqx-default")).toEqual({ preset: "dqx-default", light: {}, dark: {}, manual: { light: [], dark: [] } });
  });
  test("editing a colour detaches the preset", () => {
    const d = setGroup(applyPreset("nord"), "light", "brand", "#000000");
    expect(d.preset).toBeNull();
    expect(d.light.brand).toBe("#000000");
  });
  test("setting a colour to the preset's own value keeps the preset", () => {
    const d = setGroup(applyPreset("nord"), "light", "brand", "#5e81ac");
    expect(d.preset).toBe("nord");
    expect(d.light.brand).toBe("#5E81AC");
  });
  test("warnings for unreadable text, none for default", () => {
    expect(draftWarnings(applyPreset("dqx-default"))).toEqual([]);
    const bad = setGroup(setGroup(applyPreset("dqx-default"), "light", "page_background", "#888888"), "light", "text", "#999999");
    expect(draftWarnings(bad).length).toBeGreaterThan(0);
  });
  test("isDirty", () => {
    expect(isDirty(applyPreset("nord"), applyPreset("nord"))).toBe(false);
    expect(isDirty(applyPreset("nord"), applyPreset("dracula"))).toBe(true);
  });
  test("draftFromApi copies server colours", () => {
    const d = draftFromApi({
      preset: null,
      company_name: null,
      logo_mode: "shared",
      light: { colors: { brand: "#112233" } },
      dark: { customised: false, colors: {} },
      logos: {},
    });
    expect(d).toEqual({ preset: null, light: { brand: "#112233" }, dark: {}, manual: { light: ["brand"], dark: [] } });
  });
});

describe("custom presets", () => {
  const custom = {
    id: "custom-2",
    light: { colors: { brand: "#112233" } },
    dark: { customised: true, colors: { brand: "#445566" } },
  };
  test("applyCustomPreset loads its colours and records it", () => {
    expect(applyCustomPreset(custom)).toEqual({
      preset: "custom-2",
      light: { brand: "#112233" },
      dark: { brand: "#445566" },
      manual: { light: [], dark: [] },
    });
  });
  test("editing a custom preset detaches it", () => {
    expect(setGroup(applyCustomPreset(custom), "light", "text", "#000000").preset).toBeNull();
  });
  test("customPresetNumber", () => {
    expect(customPresetNumber("custom-12")).toBe(12);
    expect(customPresetNumber("nord")).toBeNull();
    expect(customPresetNumber("custom-0")).toBeNull();
  });
});

describe("light and dark follow each other", () => {
  test("a light edit regenerates the dark colour", () => {
    const d = setGroup(applyPreset("nord"), "light", "header", "#3F0E40");
    expect(d.dark.header).toBeUndefined();
    expect(draftDark(d).header).toBe(generateDark({ header: "#3F0E40" }).header!);
  });
  test("a dark edit generates the light colour", () => {
    const d = setGroup(applyPreset("dqx-default"), "dark", "page_background", "#1A1D21");
    expect(d.dark.page_background).toBe("#1A1D21");
    expect(d.light.page_background).toBe(generateLight({ page_background: "#1A1D21" }).page_background!);
    expect(draftDark(d).page_background).toBe("#1A1D21");
  });
  test("colours set by hand are never replaced from the other mode", () => {
    let d = setGroup(applyPreset("dqx-default"), "dark", "brand", "#00AA00");
    d = setGroup(d, "light", "brand", "#112233");
    d = setGroup(d, "dark", "brand", "#00BB00");
    expect(d.light.brand).toBe("#112233");
    expect(draftDark(d).brand).toBe("#00BB00");
  });
  test("generated colours stay generated after a save and reload", () => {
    const d = setGroup(applyPreset("dqx-default"), "dark", "text", "#EEEEEE");
    const t = draftToTheme(d);
    const loaded = draftFromApi({ preset: null, company_name: null, logo_mode: "shared", light: t.light, dark: t.dark, logos: {} });
    expect(loaded.manual).toEqual({ light: [], dark: ["text"] });
  });
  test("dark overrides are only saved when there are any", () => {
    expect(draftToTheme(setGroup(applyPreset("dqx-default"), "light", "brand", "#2F6FAE")).dark).toEqual({ customised: false, colors: {} });
    expect(draftToTheme(applyPreset("nord")).dark.customised).toBe(true);
  });
});

describe("fileToLogoPayload", () => {
  test("encodes a PNG as base64", async () => {
    const file = new File([new Uint8Array([1, 2, 3])], "logo.png", { type: "image/png" });
    expect(await fileToLogoPayload(file)).toEqual({ content_type: "image/png", data_base64: "AQID" });
  });
  test("rejects unsupported types", async () => {
    const file = new File(["<svg/>"], "logo.svg", { type: "image/svg+xml" });
    await expect(fileToLogoPayload(file)).rejects.toThrow("type");
  });
  test("rejects files over the size limit", async () => {
    const file = new File([new Uint8Array(MAX_LOGO_BYTES + 1)], "big.png", { type: "image/png" });
    await expect(fileToLogoPayload(file)).rejects.toThrow("size");
  });
});
