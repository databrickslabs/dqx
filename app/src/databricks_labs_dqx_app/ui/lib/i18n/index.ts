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

export async function ensureLocaleLoaded(lng: string): Promise<void> {
  if (lng === "en" || i18n.hasResourceBundle(lng, "translation")) return;
  const loader = localeLoaders[lng];
  if (!loader) return;
  const mod = await loader();
  i18n.addResourceBundle(lng, "translation", mod.default);
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
    const requested = i18n.language;
    if (requested && requested !== "en") {
      await ensureLocaleLoaded(requested);
      if (i18n.resolvedLanguage !== requested) {
        await i18n.changeLanguage(requested);
      }
    }
    document.documentElement.lang = i18n.resolvedLanguage ?? requested ?? "en";
  });

i18n.on("languageChanged", (lng) => {
  document.documentElement.lang = lng;
  void ensureLocaleLoaded(lng);
});

export default i18n;
