import type { BrandingCustomPresetOut, BrandingOut } from "@/lib/api";
import { checkContrast, type ContrastWarning } from "./contrast";
import { deriveAllTokens, effectiveDark, generateDark, generateLight } from "./derive";
import { COLOR_GROUPS, type ColorGroup, type GroupColors, type Mode } from "./groups";
import { presetById } from "./presets";

/**
 * Unsaved theme being edited in Settings -> Customisation. Editing a colour in one mode generates the
 * same colour in the other mode, unless that one was set by hand (*manual*). *dark* holds only dark
 * colours that differ from the ones generated from light. Preset colours are never manual.
 */
export type ThemeDraft = {
  preset: string | null;
  /** Built-in preset the draft started from, which "Reset theme" restores; null for custom presets. */
  base: string | null;
  /** Started from "Add new": colours not set in either mode show as not set rather than as defaults. */
  blank: boolean;
  light: GroupColors;
  dark: GroupColors;
  manual: Record<Mode, ColorGroup[]>;
};

const NO_MANUAL: Record<Mode, ColorGroup[]> = { light: [], dark: [] };

export function draftFromApi(b: BrandingOut): ThemeDraft {
  const light = { ...(b.light.colors as GroupColors) };
  const dark = b.dark.customised ? { ...(b.dark.colors as GroupColors) } : {};
  const preset = b.preset ?? null;
  const builtIn = (id: string | null) => (id && presetById(id) ? id : null);
  const isDefault = Object.keys(light).length === 0 && Object.keys(dark).length === 0;
  const base = builtIn(preset) ?? (preset === null && isDefault ? "dqx-default" : null);
  if (preset) return { preset, base, blank: false, light, dark, manual: NO_MANUAL };
  // A colour equal to what the other mode would generate is treated as generated.
  const fromLight = generateDark(light);
  const manual = {
    light: COLOR_GROUPS.filter((g) => {
      const v = light[g];
      if (v === undefined) return false;
      const d = dark[g];
      return d === undefined || generateLight({ [g]: d })[g] !== v;
    }),
    dark: COLOR_GROUPS.filter((g) => dark[g] !== undefined && dark[g] !== fromLight[g]),
  };
  return { preset, base, blank: false, light, dark, manual };
}

export function applyPreset(id: string): ThemeDraft {
  const p = presetById(id);
  if (!p || id === "dqx-default") return { preset: "dqx-default", base: "dqx-default", blank: false, light: {}, dark: {}, manual: NO_MANUAL };
  return { preset: id, base: id, blank: false, light: { ...p.light }, dark: { ...p.dark }, manual: NO_MANUAL };
}

/** Loads a saved custom preset ("Custom N"). */
export function applyCustomPreset(p: BrandingCustomPresetOut): ThemeDraft {
  return {
    preset: p.id,
    base: null,
    blank: false,
    light: { ...(p.light.colors as GroupColors) },
    dark: p.dark.customised ? { ...(p.dark.colors as GroupColors) } : {},
    manual: NO_MANUAL,
  };
}

/** A new theme with no colours set; saving it adds a custom preset. */
export function blankDraft(): ThemeDraft {
  return { preset: null, base: null, blank: true, light: {}, dark: {}, manual: NO_MANUAL };
}

/** "custom-3" -> 3; null for built-in presets. */
export function customPresetNumber(id: string): number | null {
  const m = /^custom-([1-9][0-9]{0,3})$/.exec(id);
  return m ? Number(m[1]) : null;
}

/**
 * Sets one colour by hand. A built-in preset stops being selected as soon as the colours differ
 * from it (saving then adds a custom preset); a custom preset stays selected, so saving updates
 * it. The same colour in the other mode is regenerated from this one unless it was set by hand.
 */
export function setGroup(d: ThemeDraft, mode: Mode, group: ColorGroup, hex: string): ThemeDraft {
  const value = hex.toUpperCase();
  if (d[mode][group]?.toUpperCase() === value) return d;
  const other: Mode = mode === "light" ? "dark" : "light";
  const manual = { ...d.manual, [mode]: d.manual[mode].includes(group) ? d.manual[mode] : [...d.manual[mode], group] };
  const light = { ...d.light };
  const dark = { ...d.dark };
  if (mode === "light") light[group] = value;
  else dark[group] = value;
  if (!d.manual[other].includes(group)) {
    // Dark follows light through generation, so drop the stored dark colour; light needs a value.
    if (mode === "light") delete dark[group];
    else light[group] = generateLight({ [group]: value })[group];
  }
  const preset = d.preset && customPresetNumber(d.preset) !== null ? d.preset : null;
  return { ...d, preset, light, dark, manual };
}

/** The dark colours the draft shows: generated from light, with the dark overrides on top. */
export function draftDark(d: ThemeDraft): GroupColors {
  return effectiveDark(d.light, Object.keys(d.dark).length > 0, d.dark);
}

/** The theme payload saved to the backend. */
export function draftToTheme(d: ThemeDraft): {
  preset: string | null;
  light: { colors: GroupColors };
  dark: { customised: boolean; colors: GroupColors };
} {
  const customised = Object.keys(d.dark).length > 0;
  return { preset: d.preset, light: { colors: d.light }, dark: { customised, colors: customised ? d.dark : {} } };
}

export function draftWarnings(d: ThemeDraft): ContrastWarning[] {
  return [
    ...checkContrast("light", deriveAllTokens("light", d.light)),
    ...checkContrast("dark", deriveAllTokens("dark", draftDark(d))),
  ];
}

export function isDirty(a: ThemeDraft, b: ThemeDraft): boolean {
  return JSON.stringify(a) !== JSON.stringify(b);
}

const LOGO_TYPES = ["image/png", "image/jpeg", "image/webp"];
export const LOGO_ACCEPT = LOGO_TYPES.join(",");
export const MAX_LOGO_BYTES = 262144;

/** Reads a logo file as base64; rejects with Error("type") or Error("size"). */
export async function fileToLogoPayload(file: File): Promise<{ content_type: string; data_base64: string }> {
  if (!LOGO_TYPES.includes(file.type)) throw new Error("type");
  if (file.size > MAX_LOGO_BYTES) throw new Error("size");
  const bytes = new Uint8Array(await file.arrayBuffer());
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return { content_type: file.type, data_base64: btoa(binary) };
}
