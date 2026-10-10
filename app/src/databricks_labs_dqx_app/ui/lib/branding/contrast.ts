import { contrastRatio, isHex } from "./color";
import type { ColorGroup, Mode } from "./groups";

/** *fields* are the colour groups an admin can change to fix the warning. */
export type ContrastWarning = { mode: Mode; pair: string; ratio: number; min: number; fields: ColorGroup[] };

/** [pair id, background token, foreground token, minimum ratio] */
export const CONTRAST_PAIRS: readonly [string, string, string, number][] = [
  ["text", "--background", "--foreground", 4.5],
  ["mutedText", "--background", "--muted-foreground", 4.5],
  ["header", "--header", "--header-foreground", 4.5],
  ["button", "--primary", "--primary-foreground", 4.5],
  ["hover", "--accent", "--accent-foreground", 4.5],
  ["sidebar", "--sidebar", "--sidebar-foreground", 4.5],
  ["sidebarActive", "--sidebar-accent", "--sidebar-accent-foreground", 4.5],
];

/** Colour group(s) whose picker controls each pair, in the order they're suggested. */
export const CONTRAST_PAIR_FIELDS: Readonly<Record<string, ColorGroup[]>> = {
  text: ["text", "page_background"],
  mutedText: ["text", "page_background"],
  header: ["header"],
  button: ["brand"],
  hover: ["page_background", "brand"],
  sidebar: ["sidebar"],
  sidebarActive: ["sidebar"],
};

export function checkContrast(mode: Mode, tokens: Record<string, string>): ContrastWarning[] {
  const warnings: ContrastWarning[] = [];
  for (const [pair, bg, fg, min] of CONTRAST_PAIRS) {
    const a = tokens[bg];
    const b = tokens[fg];
    if (!isHex(a) || !isHex(b)) continue;
    const ratio = contrastRatio(a, b);
    if (ratio < min) {
      warnings.push({ mode, pair, ratio: Math.round(ratio * 10) / 10, min, fields: [...(CONTRAST_PAIR_FIELDS[pair] ?? [])] });
    }
  }
  return warnings;
}
