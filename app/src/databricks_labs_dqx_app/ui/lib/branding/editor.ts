import type { BrandingOut } from "@/lib/api";
import { checkContrast, type ContrastWarning } from "./contrast";
import { deriveAllTokens, effectiveDark } from "./derive";
import type { ColorGroup, GroupColors, Mode } from "./groups";
import { presetById } from "./presets";

/** Unsaved theme being edited in Settings -> Styling. */
export type ThemeDraft = { preset: string | null; light: GroupColors; darkCustomised: boolean; dark: GroupColors };

export function draftFromApi(b: BrandingOut): ThemeDraft {
  return {
    preset: b.preset ?? null,
    light: { ...(b.light.colors as GroupColors) },
    darkCustomised: !!b.dark.customised,
    dark: { ...(b.dark.colors as GroupColors) },
  };
}

export function applyPreset(id: string): ThemeDraft {
  const p = presetById(id);
  if (!p || id === "dqx-default") return { preset: "dqx-default", light: {}, darkCustomised: false, dark: {} };
  return { preset: id, light: { ...p.light }, darkCustomised: true, dark: { ...p.dark } };
}

/** Sets one colour; the draft stops being a preset as soon as it differs from it. */
export function setGroup(d: ThemeDraft, mode: Mode, group: ColorGroup, hex: string): ThemeDraft {
  const value = hex.toUpperCase();
  const next: ThemeDraft = { ...d, [mode]: { ...d[mode], [group]: value } };
  const p = d.preset ? presetById(d.preset) : undefined;
  const presetValue = p ? (p[mode] as GroupColors)[group] : undefined;
  if (presetValue?.toUpperCase() !== value) next.preset = null;
  return next;
}

/**
 * Switches dark mode between auto (generated from light) and custom (seeded with the generated
 * colours). The preset is kept only if the result still equals it exactly.
 */
export function setDarkAuto(d: ThemeDraft, auto: boolean): ThemeDraft {
  const next: ThemeDraft = auto
    ? { ...d, darkCustomised: false, dark: {} }
    : { ...d, darkCustomised: true, dark: effectiveDark(d.light, false, {}) };
  if (!d.preset || isDirty(next, applyPreset(d.preset))) next.preset = null;
  return next;
}

export function draftWarnings(d: ThemeDraft): ContrastWarning[] {
  return [
    ...checkContrast("light", deriveAllTokens("light", d.light)),
    ...checkContrast("dark", deriveAllTokens("dark", effectiveDark(d.light, d.darkCustomised, d.dark))),
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
