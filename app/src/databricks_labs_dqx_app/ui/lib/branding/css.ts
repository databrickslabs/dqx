import { isHex } from "./color";
import { deriveOverrides, effectiveDark, THEMABLE_TOKENS } from "./derive";
import { COLOR_GROUPS, DEFAULT_GROUPS, type GroupColors } from "./groups";

export type BrandingTheme = { light: GroupColors; darkCustomised: boolean; dark: GroupColors };
export type ThemeOverrides = { light: Record<string, string>; dark: Record<string, string> };

export const BRANDING_STYLE_ID = "dqx-branding";
const TOKEN_RE = /^--[a-z][a-z0-9-]*$/;
const ALLOWED = new Set(THEMABLE_TOKENS);

/**
 * Dark overrides cover every group set in either mode: a group customised only in light
 * takes DQX Default's dark value, so each light-overridden token is also declared for dark
 * and nothing from the light theme can show through in dark mode.
 */
export function themeOverrides(theme: BrandingTheme): ThemeOverrides {
  const dark: GroupColors = { ...effectiveDark(theme.light, theme.darkCustomised, theme.dark) };
  for (const group of COLOR_GROUPS) {
    if (theme.light[group] !== undefined && dark[group] === undefined) dark[group] = DEFAULT_GROUPS.dark[group];
  }
  return { light: deriveOverrides("light", theme.light), dark: deriveOverrides("dark", dark) };
}

function rule(selector: string, tokens: Record<string, string>): string {
  const body = Object.entries(tokens)
    .filter(([k, v]) => TOKEN_RE.test(k) && ALLOWED.has(k) && isHex(v))
    .map(([k, v]) => `${k}:${v};`)
    .join("");
  return body ? `${selector}{${body}}` : "";
}

/** Light rule selector: never matches in dark mode, and out-ranks globals.css :root. */
export const LIGHT_SELECTOR = "html:root:not(.dark)";
/** Dark rule selector: out-ranks globals.css .dark regardless of load order. */
export const DARK_SELECTOR = "html.dark";

export function toStyleSheet(o: ThemeOverrides): string {
  return rule(LIGHT_SELECTOR, o.light) + rule(DARK_SELECTOR, o.dark);
}

export function applyStyleSheet(css: string, doc: Document = document): void {
  let el = doc.getElementById(BRANDING_STYLE_ID);
  if (!css) {
    el?.remove();
    return;
  }
  if (!el) {
    el = doc.createElement("style");
    el.id = BRANDING_STYLE_ID;
    doc.head.appendChild(el);
  }
  if (el.textContent !== css) el.textContent = css;
}
