import i18n from "i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import { initReactI18next } from "react-i18next";

import en from "./locales/en.json";
import {
  LANGUAGE_STORAGE_KEY,
  SUPPORTED_LANGUAGES,
  loadLocale,
  toSupportedCode,
} from "./languages";

export {
  LANGUAGE_STORAGE_KEY,
  SUPPORTED_LANGUAGES,
  toSupportedCode,
  type LanguageCode,
} from "./languages";

export async function ensureLocaleLoaded(lng: string): Promise<void> {
  await loadLocale(i18n, lng);
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
