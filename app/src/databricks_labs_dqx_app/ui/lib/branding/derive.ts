import { hexToOklch, mix, oklchToHex, readableOn, tint, withLightness } from "./color";
import { COLOR_GROUPS, DEFAULT_GROUPS, type ColorGroup, type GroupColors, type Mode } from "./groups";

/** Mode-specific step sizes, measured from today's globals.css. */
const STEP = {
  light: { hover: 0.035, border: 0.09, muted: 0.45, sidebarActive: 0.06 },
  dark: { hover: 0.15, border: 0.18, muted: 0.33, sidebarActive: 0.12 },
} as const;

/** token -> groups it depends on. Only tokens listed here are ever overridden. */
const INPUTS: Record<string, ColorGroup[]> = {
  "--header": ["header"],
  "--header-foreground": ["header"],
  "--background": ["page_background"],
  "--card": ["page_background"],
  "--popover": ["page_background"],
  "--foreground": ["text"],
  "--card-foreground": ["text"],
  "--popover-foreground": ["text"],
  "--muted-foreground": ["text", "page_background"],
  "--primary": ["brand"],
  "--primary-foreground": ["brand"],
  "--ring": ["brand", "page_background"],
  "--accent": ["page_background", "text", "brand"],
  "--accent-foreground": ["page_background", "text", "brand"],
  "--secondary": ["page_background", "text", "brand"],
  "--secondary-foreground": ["page_background", "text", "brand"],
  "--muted": ["page_background", "text", "brand"],
  "--border": ["page_background", "text"],
  "--input": ["page_background", "text"],
  "--sidebar": ["sidebar"],
  "--sidebar-foreground": ["sidebar"],
  "--sidebar-accent": ["sidebar"],
  "--sidebar-accent-foreground": ["sidebar"],
  "--sidebar-border": ["sidebar"],
  "--sidebar-primary": ["brand"],
  "--sidebar-primary-foreground": ["brand"],
  "--sidebar-ring": ["brand", "page_background"],
};

export const THEMABLE_TOKENS: readonly string[] = Object.keys(INPUTS);

export function deriveAllTokens(mode: Mode, set: GroupColors): Record<string, string> {
  const g = { ...DEFAULT_GROUPS[mode], ...set };
  const s = STEP[mode];
  const hover = tint(mix(g.page_background, g.text, s.hover), g.brand, 0.1);
  const sidebarFg = readableOn(g.sidebar);
  const sidebarActive = mix(g.sidebar, sidebarFg, s.sidebarActive);
  const ring = mix(g.brand, g.page_background, 0.35);
  const border = mix(g.page_background, g.text, s.border);
  const header = set.header ?? g.page_background;
  return {
    "--header": header,
    "--header-foreground": readableOn(header),
    "--background": g.page_background,
    "--card": g.page_background,
    "--popover": g.page_background,
    "--foreground": g.text,
    "--card-foreground": g.text,
    "--popover-foreground": g.text,
    "--muted-foreground": mix(g.text, g.page_background, s.muted),
    "--primary": g.brand,
    "--primary-foreground": readableOn(g.brand),
    "--ring": ring,
    "--accent": hover,
    "--accent-foreground": readableOn(hover),
    "--secondary": hover,
    "--secondary-foreground": readableOn(hover),
    "--muted": hover,
    "--border": border,
    "--input": border,
    "--sidebar": g.sidebar,
    "--sidebar-foreground": sidebarFg,
    "--sidebar-accent": sidebarActive,
    "--sidebar-accent-foreground": readableOn(sidebarActive),
    "--sidebar-border": mix(g.sidebar, sidebarFg, s.border),
    "--sidebar-primary": g.brand,
    "--sidebar-primary-foreground": readableOn(g.brand),
    "--sidebar-ring": ring,
  };
}

export function deriveOverrides(mode: Mode, set: GroupColors): Record<string, string> {
  const setGroups = new Set(COLOR_GROUPS.filter((k) => set[k] !== undefined));
  if (setGroups.size === 0) return {};
  const all = deriveAllTokens(mode, set);
  const out: Record<string, string> = {};
  for (const [token, inputs] of Object.entries(INPUTS)) {
    if (inputs.some((i) => setGroups.has(i))) out[token] = all[token];
  }
  return out;
}

/** Auto dark mode: invert surfaces, keep hue/chroma of accents, only for groups that were set. */
export function generateDark(light: GroupColors): GroupColors {
  const out: GroupColors = {};
  for (const group of COLOR_GROUPS) {
    const hex = light[group];
    if (!hex) continue;
    const c = hexToOklch(hex);
    if (group === "page_background") out[group] = oklchToHex({ l: Math.min(0.25, Math.max(0.14, 1 - c.l)), c: c.c * 0.5, h: c.h });
    else if (group === "text") out[group] = oklchToHex({ l: Math.min(0.98, Math.max(0.85, 1 - c.l)), c: c.c * 0.5, h: c.h });
    else if (group === "brand") out[group] = c.l < 0.6 ? withLightness(hex, 0.68) : hex;
    else out[group] = c.l > 0.6 ? withLightness(hex, 0.22) : hex; // header, sidebar
  }
  return out;
}

export function effectiveDark(light: GroupColors, darkCustomised: boolean, dark: GroupColors): GroupColors {
  return darkCustomised ? dark : generateDark(light);
}
