import type { i18n as I18n } from "i18next";

/*
 * Language metadata and lazy locale loading, free of the i18next initialisation in
 * index.ts so components (and their tests) can use them without booting i18n.
 */

export const SUPPORTED_LANGUAGES = [
  { code: "en", label: "English", nativeLabel: "English" },
  { code: "fr", label: "French", nativeLabel: "Français" },
  { code: "pt-BR", label: "Portuguese (Brazil)", nativeLabel: "Português (Brasil)" },
  { code: "it", label: "Italian", nativeLabel: "Italiano" },
  { code: "es", label: "Spanish", nativeLabel: "Español" },
] as const;

export type LanguageCode = (typeof SUPPORTED_LANGUAGES)[number]["code"];

export const LANGUAGE_STORAGE_KEY = "dqx.language";

const localeLoaders: Record<string, () => Promise<{ default: Record<string, unknown> }>> = {
  fr: () => import("./locales/fr.json"),
  "pt-BR": () => import("./locales/pt-BR.json"),
  it: () => import("./locales/it.json"),
  es: () => import("./locales/es.json"),
};

const SUPPORTED_CODES = SUPPORTED_LANGUAGES.map((l) => l.code);

/**
 * Normalize a (possibly region-tagged) language code to the closest supported
 * code. Region variants resolve to their base locale — e.g. "es-MX" -> "es",
 * "en-GB" -> "en", "pt-PT" -> "pt-BR" — mirroring i18next's
 * `nonExplicitSupportedLngs` behaviour so the lazy loader can find a bundle.
 */
export function toSupportedCode(lng: string): string {
  if (SUPPORTED_CODES.includes(lng as LanguageCode)) return lng;
  const base = lng.split("-")[0];
  return SUPPORTED_CODES.find((code) => code.split("-")[0] === base) ?? "en";
}

/** Lazy-load *lng*'s bundle into *instance*; English always ships in the initial bundle. */
export async function loadLocale(instance: I18n, lng: string): Promise<void> {
  const code = toSupportedCode(lng);
  if (code === "en" || instance.hasResourceBundle(code, "translation")) return;
  const loader = localeLoaders[code];
  if (!loader) return;
  const mod = await loader();
  instance.addResourceBundle(code, "translation", mod.default);
}
