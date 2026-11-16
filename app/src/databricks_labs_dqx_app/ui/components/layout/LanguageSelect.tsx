import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { ChevronDown, Languages } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SUPPORTED_LANGUAGES,
  loadLocale,
  toSupportedCode,
} from "@/lib/i18n/languages";
import { cn } from "@/lib/utils";

/** Every registered locale, sorted by the native name shown to the user. */
export function sortedLanguages() {
  return [...SUPPORTED_LANGUAGES].sort((a, b) =>
    a.nativeLabel.localeCompare(b.nativeLabel),
  );
}

/** The current language and a switcher that lazy-loads the locale first. */
export function useLanguageSwitcher() {
  const { t, i18n } = useTranslation();
  const languages = useMemo(sortedLanguages, []);
  const currentLanguage = toSupportedCode(i18n.resolvedLanguage ?? "en");

  const changeLanguage = (value: string) => {
    loadLocale(i18n, value)
      .then(() => i18n.changeLanguage(value))
      .catch((err) => {
        console.error("Failed to switch language", err);
        toast.error(t("language.switchFailed"));
      });
  };

  return { languages, currentLanguage, changeLanguage };
}

/** Compact language dropdown for page headers. */
export function LanguageSelect({ className }: { className?: string } = {}) {
  const { t } = useTranslation();
  const { languages, currentLanguage, changeLanguage } = useLanguageSwitcher();
  const current = languages.find((lang) => lang.code === currentLanguage);

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className={cn("h-8 gap-1.5 px-2 text-sm", className)}
          aria-label={t("language.label")}
        >
          <Languages className="size-4" aria-hidden="true" />
          <span className="hidden sm:inline">{current?.nativeLabel}</span>
          <ChevronDown className="size-3.5 opacity-70" aria-hidden="true" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuRadioGroup
          value={currentLanguage}
          onValueChange={changeLanguage}
        >
          {languages.map((lang) => (
            <DropdownMenuRadioItem key={lang.code} value={lang.code}>
              {lang.nativeLabel}
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
