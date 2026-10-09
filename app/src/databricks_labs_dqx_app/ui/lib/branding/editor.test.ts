import { describe, expect, test } from "bun:test";
import { applyPreset, draftFromApi, draftWarnings, fileToLogoPayload, isDirty, MAX_LOGO_BYTES, setGroup } from "./editor";

describe("theme editor helpers", () => {
  test("applyPreset loads both modes and records the preset", () => {
    const d = applyPreset("aubergine");
    expect(d.preset).toBe("aubergine");
    expect(d.light.header).toBe("#3F0E40");
    expect(d.darkCustomised).toBe(true);
  });
  test("dqx-default clears colours", () => {
    expect(applyPreset("dqx-default")).toEqual({ preset: "dqx-default", light: {}, darkCustomised: false, dark: {} });
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
    expect(d).toEqual({ preset: null, light: { brand: "#112233" }, darkCustomised: false, dark: {} });
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
