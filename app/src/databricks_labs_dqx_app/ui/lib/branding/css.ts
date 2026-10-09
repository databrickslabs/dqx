import { isHex } from "./color";
import { deriveOverrides, effectiveDark, THEMABLE_TOKENS } from "./derive";
import type { GroupColors } from "./groups";

export type BrandingTheme = { light: GroupColors; darkCustomised: boolean; dark: GroupColors };
export type ThemeOverrides = { light: Record<string, string>; dark: Record<string, string> };

export const BRANDING_STYLE_ID = "dqx-branding";
const TOKEN_RE = /^--[a-z][a-z0-9-]*$/;
const ALLOWED = new Set(THEMABLE_TOKENS);

export function themeOverrides(theme: BrandingTheme): ThemeOverrides {
  return {
    light: deriveOverrides("light", theme.light),
    dark: deriveOverrides("dark", effectiveDark(theme.light, theme.darkCustomised, theme.dark)),
  };
}

function rule(selector: string, tokens: Record<string, string>): string {
  const body = Object.entries(tokens)
    .filter(([k, v]) => TOKEN_RE.test(k) && ALLOWED.has(k) && isHex(v))
    .map(([k, v]) => `${k}:${v};`)
    .join("");
  return body ? `${selector}{${body}}` : "";
}

/** html:root / html.dark out-rank globals.css :root / .dark regardless of load order. */
export function toStyleSheet(o: ThemeOverrides): string {
  return rule("html:root", o.light) + rule("html.dark", o.dark);
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
