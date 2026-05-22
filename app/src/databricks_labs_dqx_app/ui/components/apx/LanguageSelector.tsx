import { useTranslation } from "react-i18next";
import { Languages } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { SUPPORTED_LANGUAGES } from "@/lib/i18n";

interface LanguageSelectorProps {
  className?: string;
  hideLabel?: boolean;
}

export default function LanguageSelector({
  className,
  hideLabel = false,
}: LanguageSelectorProps) {
  const { t, i18n } = useTranslation();
  const primaryTag = i18n.resolvedLanguage?.split("-")[0];
  const current =
    SUPPORTED_LANGUAGES.find((l) => l.code === i18n.resolvedLanguage)?.code ??
    SUPPORTED_LANGUAGES.find((l) => l.code.split("-")[0] === primaryTag)?.code ??
    "en";

  const handleChange = (value: string) => {
    void i18n.changeLanguage(value);
  };

  return (
    <div className={className}>
      {!hideLabel && (
        <Label htmlFor="language-selector" className="flex items-center gap-2 mb-2">
          <Languages className="h-4 w-4" />
          {t("language.label")}
        </Label>
      )}
      <Select value={current} onValueChange={handleChange}>
        <SelectTrigger id="language-selector" className="w-full sm:w-64">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {SUPPORTED_LANGUAGES.map((lang) => (
            <SelectItem key={lang.code} value={lang.code}>
              <span className="font-medium">{lang.nativeLabel}</span>
              {lang.nativeLabel !== lang.label && (
                <span className="text-muted-foreground ml-2 text-xs">
                  ({lang.label})
                </span>
              )}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {!hideLabel && (
        <p className="text-sm text-muted-foreground mt-2">
          {t("language.description")}
        </p>
      )}
    </div>
  );
}
