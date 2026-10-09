import type { BrandingOut } from "@/lib/api";
import { isHex } from "./color";
import { themeOverrides, type ThemeOverrides } from "./css";
import type { GroupColors } from "./groups";

export const CACHE_KEY = "dqx.branding.v1";
const TOKEN_RE = /^--[a-z][a-z0-9-]*$/;
const HASH_RE = /^[0-9a-f]{16}$/;

export type BrandingSnapshot = {
  overrides: ThemeOverrides;
  companyName: string | null;
  logoMode: "shared" | "separate";
  logos: { light: string | null; dark: string | null };
};

function validTokens(v: unknown): v is Record<string, string> {
  return (
    !!v &&
    typeof v === "object" &&
    !Array.isArray(v) &&
    Object.entries(v as Record<string, unknown>).every(([k, x]) => TOKEN_RE.test(k) && isHex(x))
  );
}
const validHash = (v: unknown): boolean => v === null || (typeof v === "string" && HASH_RE.test(v));

export function readBrandingCache(storage: Storage = localStorage): BrandingSnapshot | null {
  try {
    const raw = storage.getItem(CACHE_KEY);
    if (!raw) return null;
    const s = JSON.parse(raw) as Partial<BrandingSnapshot> | null;
    if (!s || typeof s !== "object" || !s.overrides) return null;
    if (!validTokens(s.overrides.light) || !validTokens(s.overrides.dark)) return null;
    if (!(s.companyName === null || (typeof s.companyName === "string" && s.companyName.length <= 60))) return null;
    if (s.logoMode !== "shared" && s.logoMode !== "separate") return null;
    if (!s.logos || !validHash(s.logos.light) || !validHash(s.logos.dark)) return null;
    return {
      overrides: { light: s.overrides.light, dark: s.overrides.dark },
      companyName: s.companyName,
      logoMode: s.logoMode,
      logos: { light: s.logos.light ?? null, dark: s.logos.dark ?? null },
    };
  } catch {
    return null;
  }
}

export function writeBrandingCache(s: BrandingSnapshot, storage: Storage = localStorage): void {
  try {
    storage.setItem(CACHE_KEY, JSON.stringify(s));
  } catch {
    /* storage full or disabled: theming still works without the cache */
  }
}

export function clearBrandingCache(storage: Storage = localStorage): void {
  try {
    storage.removeItem(CACHE_KEY);
  } catch {
    /* storage unavailable: nothing to clear */
  }
}

export function snapshotFromApi(b: BrandingOut): BrandingSnapshot {
  return {
    overrides: themeOverrides({
      light: b.light.colors as GroupColors,
      darkCustomised: b.dark.customised,
      dark: b.dark.colors as GroupColors,
    }),
    companyName: b.company_name ?? null,
    logoMode: b.logo_mode === "separate" ? "separate" : "shared",
    logos: { light: b.logos.light ?? null, dark: b.logos.dark ?? null },
  };
}
