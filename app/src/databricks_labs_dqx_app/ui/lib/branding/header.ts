import { themeOverrides, type BrandingTheme } from "./css";
import { deriveAllTokens } from "./derive";

export type HeaderLogoSlot = "light" | "dark";

export function headerTitle(companyName: string | null | undefined): { product: string; company: string | null } {
  const company = companyName?.trim() ? companyName.trim() : null;
  return { product: "DQX Studio", company };
}

export function pickHeaderLogo(
  s: { logoMode: string; logos: { light: string | null; dark: string | null } },
  isDark: boolean,
): { slot: HeaderLogoSlot; hash: string } | null {
  // Separate mode without a dark logo shows the light logo rather than the DQX icon.
  if (s.logoMode === "separate" && isDark && s.logos.dark) return { slot: "dark", hash: s.logos.dark };
  return s.logos.light ? { slot: "light", hash: s.logos.light } : null;
}

export function logoUrl(slot: HeaderLogoSlot, hash: string): string {
  return `/api/v1/config/branding/logo/${slot}?v=${hash}`;
}

export type HeaderSwatch = { background: string; foreground: string };

/** The header background and text colour each logo slot is shown on, as the saved theme applies them. */
export function headerSwatches(theme: BrandingTheme): Record<HeaderLogoSlot, HeaderSwatch> {
  const overrides = themeOverrides(theme);
  const swatch = (slot: HeaderLogoSlot): HeaderSwatch => {
    const fallback = deriveAllTokens(slot, {});
    const tokens = overrides[slot];
    return {
      background: tokens["--header"] ?? fallback["--header"],
      foreground: tokens["--header-foreground"] ?? fallback["--header-foreground"],
    };
  };
  return { light: swatch("light"), dark: swatch("dark") };
}
