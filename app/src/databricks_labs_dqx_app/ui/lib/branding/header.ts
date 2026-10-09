export type HeaderLogoSlot = "light" | "dark";

export function headerTitle(companyName: string | null | undefined): { product: string; company: string | null } {
  const company = companyName?.trim() ? companyName.trim() : null;
  return { product: "DQX Studio", company };
}

export function pickHeaderLogo(
  s: { logoMode: string; logos: { light: string | null; dark: string | null } },
  isDark: boolean,
): { slot: HeaderLogoSlot; hash: string } | null {
  const slot: HeaderLogoSlot = s.logoMode === "separate" && isDark ? "dark" : "light";
  const hash = s.logos[slot];
  return hash ? { slot, hash } : null;
}

export function logoUrl(slot: HeaderLogoSlot, hash: string): string {
  return `/api/v1/config/branding/logo/${slot}?v=${hash}`;
}
