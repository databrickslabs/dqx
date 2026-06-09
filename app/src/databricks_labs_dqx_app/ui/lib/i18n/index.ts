import i18n from "i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import { initReactI18next } from "react-i18next";

import en from "./locales/en.json";

export const SUPPORTED_LANGUAGES = [
  { code: "en", label: "English", nativeLabel: "English" },
  { code: "pt-BR", label: "Portuguese (Brazil)", nativeLabel: "Português (Brasil)" },
  { code: "it", label: "Italian", nativeLabel: "Italiano" },
  { code: "es", label: "Spanish", nativeLabel: "Español" },
] as const;

export type LanguageCode = (typeof SUPPORTED_LANGUAGES)[number]["code"];

export const LANGUAGE_STORAGE_KEY = "dqx.language";

const localeLoaders: Record<string, () => Promise<{ default: Record<string, unknown> }>> = {
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

export async function ensureLocaleLoaded(lng: string): Promise<void> {
  const code = toSupportedCode(lng);
  if (code === "en" || i18n.hasResourceBundle(code, "translation")) return;
  const loader = localeLoaders[code];
  if (!loader) return;
  const mod = await loader();
  i18n.addResourceBundle(code, "translation", mod.default);
}

export const i18nReady = i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources: {
      en: { translation: en },
    },
    fallbackLng: "en",
    supportedLngs: SUPPORTED_LANGUAGES.map((l) => l.code),
    load: "currentOnly",
    interpolation: {
      escapeValue: false,
    },
    detection: {
      order: ["localStorage", "navigator"],
      lookupLocalStorage: LANGUAGE_STORAGE_KEY,
      caches: ["localStorage"],
    },
  })
  .then(async () => {
    // Resolve the detected language (which may carry a region tag, e.g.
    // "es-MX") to a supported code, then lazy-load and select its bundle. Only
    // "en" ships in the initial bundle, so non-English locales need this step.
    const requested = toSupportedCode(i18n.language);
    if (requested !== "en") {
      await ensureLocaleLoaded(requested);
      if (i18n.resolvedLanguage !== requested) {
        await i18n.changeLanguage(requested);
      }
    }
    document.documentElement.lang = i18n.resolvedLanguage ?? requested;
  });

i18n.on("languageChanged", (lng) => {
  document.documentElement.lang = lng;
  void ensureLocaleLoaded(lng);
});

export default i18n;
